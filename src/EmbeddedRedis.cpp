#include "redismm/EmbeddedRedis.hpp"
#include "redismm/Encoder.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <format>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>

namespace redismm {

namespace {

template <typename T>
bool parse_number_strict(std::string_view raw, T& value) {
  auto const [ptr, ec] = std::from_chars(raw.data(), raw.data() + raw.size(), value);
  return ec == std::errc{} && ptr == raw.data() + raw.size();
}

/**
 * @brief スレッドローカルな乱数生成器
 * @details rand() と異なりスレッドセーフで、分布の偏りもない
 */
std::mt19937_64& rng() {
  static thread_local std::mt19937_64 gen{std::random_device{}()};
  return gen;
}

/** @brief [0, n) の一様乱数 @param n 上限（排他） @return 乱数 */
uint64_t random_below(uint64_t n) {
  return std::uniform_int_distribution<uint64_t>(0, n - 1)(rng());
}

/**
 * @brief 競合後の再試行前に待機する
 * @details バックオフ無しで再突入すると、競合したスレッド同士が同期したまま
 *          衝突し続けてライブロックする。指数的に広がる窓からランダムに選ぶ
 *          ことで再突入のタイミングをばらけさせる。
 * @param attempt 0 起算の試行回数
 */
void backoff(int attempt) {
  auto const slots = 1ULL << std::min(attempt, 10); // 上限は約 1ms
  std::this_thread::sleep_for(std::chrono::microseconds(random_below(slots) + 1));
}

} // namespace

/**
 * @brief Pimpl 実装：RocksDB のラッパーと内部操作
 * @details すべての操作は OptimisticTransactionDB のトランザクション内で実行される。
 *   メタキーを GetForUpdate で読むことで、同一キーに対する並行操作は
 *   コミット時に競合として検出され、run_txn が自動的に再実行する。
 */
struct EmbeddedRedis::Impl {
  rocksdb::OptimisticTransactionDB* txn_db     = nullptr; ///< RocksDB インスタンス（所有）
  rocksdb::WriteOptions             write_opts = {};      ///< 書き込みオプション
  rocksdb::ReadOptions              read_opts  = {};      ///< 読み取りオプション

  /**
   * @brief 競合時に同一操作を再実行する上限回数
   * @details 楽観ロックでは 1 ラウンドにつき勝者は 1 スレッドだけなので、
   *   ホットキーでは連敗しうる。backoff() の待ちが上限に達した状態でも
   *   合計待ち時間は 0.3 秒程度に収まる。これを超えても解消しない場合のみ
   *   ErrorCode::Busy を返す。
   */
  static constexpr int kMaxRetries = 256;

  ~Impl() {
    delete txn_db;
  }

  /**
   * @brief トランザクション内で操作を実行し、競合時は再実行する
   *
   * @param fn rocksdb::Transaction& を受け取り Result<T> を返す呼び出し可能オブジェクト
   * @return fn の戻り値。競合が解消しなければ ErrorCode::Busy
   * @note fn は複数回呼ばれうるため、副作用はトランザクションの中だけに閉じている必要がある
   */
  template <typename Fn>
  auto run_txn(Fn&& fn) -> std::invoke_result_t<Fn, rocksdb::Transaction&> {
    using R = std::invoke_result_t<Fn, rocksdb::Transaction&>;
    if (txn_db == nullptr) {
      return R(std::unexpected(ErrorCode::RocksDBError));
    }

    rocksdb::OptimisticTransactionOptions txn_opts;
    txn_opts.set_snapshot = true; // 操作中は一貫したスナップショットを見る

    std::unique_ptr<rocksdb::Transaction> txn;
    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
      // 2 回目以降は Transaction オブジェクトを再利用して割り当てを避ける
      txn.reset(txn_db->BeginTransaction(write_opts, txn_opts, txn.release()));

      R result = fn(*txn);
      if (!result) {
        std::ignore = txn->Rollback();
        return result;
      }

      auto const s = txn->Commit();
      if (s.ok()) {
        return result;
      }
      if (s.IsBusy() || s.IsTryAgain()) {
        backoff(attempt);
        continue; // 他スレッドと競合した。操作全体をやり直す
      }
      return R(std::unexpected(ErrorCode::RocksDBError));
    }
    return R(std::unexpected(ErrorCode::Busy));
  }

  /** @brief トランザクションのスナップショットを使う点読み取りオプション */
  static rocksdb::ReadOptions snap_opts(rocksdb::Transaction& txn) {
    rocksdb::ReadOptions ro;
    ro.snapshot = txn.GetSnapshot();
    return ro;
  }

  /** @brief トランザクションのスナップショットを使う走査用オプション（キャッシュ汚染を避ける） */
  static rocksdb::ReadOptions snap_iter_opts(rocksdb::Transaction& txn) {
    auto ro       = snap_opts(txn);
    ro.fill_cache = false;
    return ro;
  }

  /**
   * @brief 書き込みパス用のメタデータ取得
   * @details GetForUpdate でメタキーを競合検出の対象に登録する。これにより同一キーへの
   *   並行操作はコミット時に必ず衝突し、run_txn が再実行する。
   * @param txn トランザクション
   * @param key ユーザーキー
   * @return メタデータ。存在しないか期限切れなら nullopt（期限切れデータは同一トランザクションで削除）
   */
  std::optional<MetaValue> get_meta(rocksdb::Transaction& txn, std::string_view key) {
    std::string raw;
    auto const  s = txn.GetForUpdate(snap_opts(txn), encode_meta_key(key), &raw);
    return finish_get_meta(txn, key, s, raw);
  }

  /**
   * @brief 読み取りパス用のメタデータ取得
   * @details 競合検出の対象にしないため、読み取り操作が Busy で失敗することはない
   * @param txn トランザクション
   * @param key ユーザーキー
   * @return メタデータ。存在しないか期限切れなら nullopt
   */
  std::optional<MetaValue> get_meta_ro(rocksdb::Transaction& txn, std::string_view key) {
    std::string raw;
    auto const  s = txn.Get(snap_opts(txn), encode_meta_key(key), &raw);
    return finish_get_meta(txn, key, s, raw);
  }

  /** @brief get_meta / get_meta_ro 共通の後処理（デコードと期限切れ判定） */
  std::optional<MetaValue> finish_get_meta(rocksdb::Transaction& txn, std::string_view key, rocksdb::Status const& s,
                                           std::string const& raw) {
    if (!s.ok()) {
      return std::nullopt;
    }
    auto meta = decode_meta_value(raw);
    if (!meta) {
      return std::nullopt;
    }
    if (is_expired(*meta)) {
      erase_key_data(txn, key, *meta); // 期限切れは見つけた時点で回収する
      return std::nullopt;
    }
    return meta;
  }

  /**
   * @brief メタデータをトランザクションに書き込む
   *
   * @param txn トランザクション
   * @param key ユーザーキー
   * @param meta メタデータ
   */
  void put_meta(rocksdb::Transaction& txn, std::string_view key, MetaValue const& meta) {
    std::ignore = txn.Put(encode_meta_key(key), encode_meta_value(meta));
  }

  /**
   * @brief プレフィックスに合致する全キーを削除する
   *
   * @param txn トランザクション
   * @param pfx プレフィックス
   * @note OptimisticTransactionDB は DeleteRange を受け付けないため、走査して個別に削除する
   */
  void delete_by_prefix(rocksdb::Transaction& txn, std::string const& pfx) {
    rocksdb::Slice const               pfx_slice(pfx);
    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));
    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      if (!it->key().starts_with(pfx_slice)) {
        break;
      }
      std::ignore = txn.Delete(it->key());
    }
  }

  // ---- 引数を書き換える操作の実体 ----
  // run_txn は競合時に同じラムダを再呼び出しするため、ラムダ内で値渡し引数を書き換えると
  // 変更が次の試行に持ち越されてしまう。関数の引数として毎回コピーを渡すことで防ぐ。
  auto getrange(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t end) -> Result<std::string>;
  auto lrange(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>>;
  auto lindex(rocksdb::Transaction& txn, std::string_view key, int64_t index) -> Result<std::string>;
  auto lset(rocksdb::Transaction& txn, std::string_view key, int64_t index, std::string_view value) -> Result<void>;
  auto zrange(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>>;
  auto zrevrange(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>>;
  auto zremrangebyrank(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<uint64_t>;

  // ---- Pipeline と共有する操作の実体（同じロジックを二度書かないための集約） ----
  auto set(rocksdb::Transaction& txn, std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms) -> Result<void>;
  auto append(rocksdb::Transaction& txn, std::string_view key, std::string_view value) -> Result<uint64_t>;
  auto hset(rocksdb::Transaction& txn, std::string_view key, std::string_view field, std::string_view value) -> Result<bool>;
  auto hdel(rocksdb::Transaction& txn, std::string_view key, std::string_view field) -> Result<bool>;
  auto lpush(rocksdb::Transaction& txn, std::string_view key, std::string_view value) -> Result<uint64_t>;
  auto rpush(rocksdb::Transaction& txn, std::string_view key, std::string_view value) -> Result<uint64_t>;
  auto lrem(rocksdb::Transaction& txn, std::string_view key, int64_t count, std::string_view value) -> Result<uint64_t>;
  auto ltrim(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<void>;
  auto sadd(rocksdb::Transaction& txn, std::string_view key, std::string_view member) -> Result<bool>;
  auto srem(rocksdb::Transaction& txn, std::string_view key, std::string_view member) -> Result<bool>;
  auto zadd(rocksdb::Transaction& txn, std::string_view key, double score, std::string_view member) -> Result<bool>;
  auto zrem(rocksdb::Transaction& txn, std::string_view key, std::string_view member) -> Result<bool>;
  auto xadd(rocksdb::Transaction& txn, std::string_view key, std::string_view id, std::vector<std::pair<std::string, std::string>> const& fields) -> Result<std::string>;
  auto del(rocksdb::Transaction& txn, std::string_view key) -> Result<bool>;
  auto pexpire(rocksdb::Transaction& txn, std::string_view key, uint64_t milliseconds) -> Result<bool>;
  auto persist(rocksdb::Transaction& txn, std::string_view key) -> Result<bool>;

  /**
   * @brief キーに関連する全データ（メタ＋データ）を削除する
   *
   * @param txn トランザクション
   * @param key ユーザーキー
   * @param meta メタデータ
   */
  void erase_key_data(rocksdb::Transaction& txn, std::string_view key, MetaValue const& meta) {
    std::ignore = txn.Delete(encode_meta_key(key));
    switch (meta.type) {
    case DataType::String:
      std::ignore = txn.Delete(encode_string_key(key));
      break;
    case DataType::Hash:
      delete_by_prefix(txn, encode_hash_prefix(key, meta.version));
      break;
    case DataType::List:
      delete_by_prefix(txn, encode_list_prefix(key, meta.version));
      break;
    case DataType::Set:
      delete_by_prefix(txn, encode_set_prefix(key, meta.version));
      break;
    case DataType::ZSet:
      delete_by_prefix(txn, encode_zset_member_prefix(key, meta.version));
      delete_by_prefix(txn, encode_zset_score_range_prefix(key, meta.version));
      break;
    case DataType::Stream:
      delete_by_prefix(txn, encode_stream_prefix(key, meta.version));
      break;
    }
  }
};

// ============================================================
// コンストラクタ / デストラクタ
// ============================================================

/**
 * @brief データベースを開く
 *
 * @param db_path RocksDB のパス。ディレクトリがなければ作成される
 */
EmbeddedRedis::EmbeddedRedis(std::string_view db_path) : impl_(std::make_unique<Impl>()) {
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.compression       = rocksdb::kNoCompression;

  rocksdb::OptimisticTransactionDB* raw_db = nullptr;
  auto const status = rocksdb::OptimisticTransactionDB::Open(opts, std::string(db_path), &raw_db);
  if (!status.ok()) {
    std::cerr << std::format("[EmbeddedRedis] DB::Open failed: {}\n", status.ToString());
    return;
  }
  impl_->txn_db = raw_db;
}

EmbeddedRedis::~EmbeddedRedis() = default;
EmbeddedRedis::EmbeddedRedis(EmbeddedRedis&&) noexcept            = default;
EmbeddedRedis& EmbeddedRedis::operator=(EmbeddedRedis&&) noexcept = default;

/**
 * @brief データベースが開かれているか確認する
 *
 * @return DB が利用可能なら true
 */
bool EmbeddedRedis::is_open() const noexcept {
  return impl_ && impl_->txn_db != nullptr;
}

// ============================================================
// Strings
// ============================================================

/**
 * @brief 文字列値を格納する
 *
 * @copydoc EmbeddedRedis::set
 */
/** @brief set の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::set(rocksdb::Transaction& txn, std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms) -> Result<void> {

  auto existing = get_meta(txn, key);

  // 既存キーが別の型なら先に全削除する
  if (existing && existing->type != DataType::String) {
    erase_key_data(txn, key, *existing);
    existing = std::nullopt;
  }

  MetaValue meta;
  meta.type          = DataType::String;
  meta.version       = 1;
  meta.size          = value.size();
  meta.expiration_ms = ttl_ms ? now_ms() + *ttl_ms : 0;

  put_meta(txn, key, meta);
  std::ignore = txn.Put(encode_string_key(key), rocksdb::Slice(value.data(), value.size()));

  return {};
}

auto EmbeddedRedis::set(std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms) -> Result<void> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<void> {
    return impl_->set(txn, key, value, ttl_ms);
  });
}

/**
 * @brief 文字列値を取得する
 *
 * @copydoc EmbeddedRedis::get
 */
auto EmbeddedRedis::get(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::String) {
      return std::unexpected(ErrorCode::WrongType);
    }

    std::string value;
    if (!txn.Get(Impl::snap_opts(txn), encode_string_key(key), &value).ok()) {
      return std::unexpected(ErrorCode::NotFound);
    }
    return value;
  });
}

/**
 * @brief 文字列値を末尾に追加する
 *
 * @copydoc EmbeddedRedis::append
 */
/** @brief append の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::append(rocksdb::Transaction& txn, std::string_view key, std::string_view value) -> Result<uint64_t> {
  auto existing = get_meta(txn, key);
  if (existing && existing->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  std::string current;
  if (existing) {
    std::ignore = txn.Get(snap_opts(txn), encode_string_key(key), &current);
  }

  current += value;

  MetaValue meta;
  meta.type    = DataType::String;
  meta.version = existing ? existing->version : 1;
  // Redis 互換: 値を書き換えるだけの操作は TTL を維持する
  meta.expiration_ms = existing ? existing->expiration_ms : 0;
  meta.size    = current.size();

  put_meta(txn, key, meta);
  std::ignore = txn.Put(encode_string_key(key), rocksdb::Slice(current));

  return static_cast<uint64_t>(current.size());
}

auto EmbeddedRedis::append(std::string_view key, std::string_view value) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    return impl_->append(txn, key, value);
  });
}

/**
 * @brief 文字列値をインクリメントする
 *
 * @copydoc EmbeddedRedis::incr
 */
auto EmbeddedRedis::incr(std::string_view key) -> Result<int64_t> {
  return incrby(key, 1);
}

/**
 * @brief 文字列値をデクリメントする
 *
 * @copydoc EmbeddedRedis::decr
 */
auto EmbeddedRedis::decr(std::string_view key) -> Result<int64_t> {
  return incrby(key, -1);
}

/**
 * @brief 文字列値を指定量インクリメントする
 *
 * @copydoc EmbeddedRedis::incrby
 */
auto EmbeddedRedis::incrby(std::string_view key, int64_t delta) -> Result<int64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<int64_t> {
    auto existing = impl_->get_meta(txn, key);
    if (existing && existing->type != DataType::String) {
      return std::unexpected(ErrorCode::WrongType);
    }

    int64_t val = 0;
    if (existing) {
      std::string raw;
      if (txn.Get(Impl::snap_opts(txn), encode_string_key(key), &raw).ok()) {
        if (!parse_number_strict(raw, val)) {
          return std::unexpected(ErrorCode::WrongType);
        }
      }
    }

    val += delta;
    auto const new_str = std::to_string(val);

    MetaValue meta;
    meta.type    = DataType::String;
    meta.version = existing ? existing->version : 1;
    // Redis 互換: 値を書き換えるだけの操作は TTL を維持する
    meta.expiration_ms = existing ? existing->expiration_ms : 0;
    meta.size    = new_str.size();

    impl_->put_meta(txn, key, meta);
    std::ignore = txn.Put(encode_string_key(key), new_str);

    return val;
  });
}

/**
 * @brief 文字列値を指定量デクリメントする
 *
 * @copydoc EmbeddedRedis::decrby
 */
auto EmbeddedRedis::decrby(std::string_view key, int64_t delta) -> Result<int64_t> {
  return incrby(key, -delta);
}

/**
 * @brief 文字列値の長さを取得する
 *
 * @copydoc EmbeddedRedis::strlen
 */
auto EmbeddedRedis::strlen(std::string_view key) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::String) {
      return std::unexpected(ErrorCode::WrongType);
    }
    return meta->size;
  });
}

// ---- New String Functions ----

/**
 * @brief 値を設定し、古い値を返す
 *
 * @copydoc EmbeddedRedis::getset
 */
auto EmbeddedRedis::getset(std::string_view key, std::string_view value) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto existing = impl_->get_meta(txn, key);
    if (existing && existing->type != DataType::String) {
      return std::unexpected(ErrorCode::WrongType);
    }

    std::string old;
    if (!existing) {
      return std::unexpected(ErrorCode::NotFound);
    }

    std::ignore = txn.Get(Impl::snap_opts(txn), encode_string_key(key), &old);

    MetaValue meta      = *existing;
    meta.size           = value.size();
    meta.expiration_ms  = 0; // Redis 互換: GETSET は値を置き換えるので TTL を破棄する
    impl_->put_meta(txn, key, meta);
    std::ignore = txn.Put(encode_string_key(key), rocksdb::Slice(value.data(), value.size()));

    return old;
  });
}

/**
 * @brief キーが存在しない場合のみ設定する
 *
 * @copydoc EmbeddedRedis::setnx
 */
auto EmbeddedRedis::setnx(std::string_view key, std::string_view value) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    auto existing = impl_->get_meta(txn, key);
    if (existing) {
      if (existing->type != DataType::String) {
        return std::unexpected(ErrorCode::WrongType);
      }
      return false;
    }

    MetaValue meta;
    meta.type    = DataType::String;
    meta.version = 1;
    meta.size    = value.size();

    impl_->put_meta(txn, key, meta);
    std::ignore = txn.Put(encode_string_key(key), rocksdb::Slice(value.data(), value.size()));

    return true;
  });
}

/**
 * @brief 文字列値を浮動小数点数でインクリメントする
 *
 * @copydoc EmbeddedRedis::incrbyfloat
 */
auto EmbeddedRedis::incrbyfloat(std::string_view key, double delta) -> Result<double> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<double> {
    auto existing = impl_->get_meta(txn, key);
    if (existing && existing->type != DataType::String) {
      return std::unexpected(ErrorCode::WrongType);
    }

    double val = 0.0;
    if (existing) {
      std::string raw;
      if (txn.Get(Impl::snap_opts(txn), encode_string_key(key), &raw).ok()) {
        if (!parse_number_strict(raw, val)) {
          return std::unexpected(ErrorCode::WrongType);
        }
      }
    }

    val += delta;
    auto const new_str = [&] {
      // double を文字列に変換（冗長な桁を削減）
      auto s = std::format("{}", val);
      // 小数点を含み末尾が 0 の場合はトリム
      if (s.find('.') != std::string::npos) {
        while (s.size() > 1 && s.back() == '0') s.pop_back();
        if (s.back() == '.') s.pop_back();
      }
      return s;
    }();

    MetaValue meta;
    meta.type    = DataType::String;
    meta.version = existing ? existing->version : 1;
    // Redis 互換: 値を書き換えるだけの操作は TTL を維持する
    meta.expiration_ms = existing ? existing->expiration_ms : 0;
    meta.size    = new_str.size();
    impl_->put_meta(txn, key, meta);
    std::ignore = txn.Put(encode_string_key(key), new_str);

    return val;
  });
}

/**
 * @brief 文字列値の部分範囲を取得する
 *
 * @copydoc EmbeddedRedis::getrange
 */
/** @brief getrange の実体（引数を書き換えるため、再試行ごとに新しいコピーで実行する） */
auto EmbeddedRedis::Impl::getrange(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t end) -> Result<std::string> {
  auto const meta = get_meta_ro(txn, key);
  if (!meta) {
    return std::string{};
  }
  if (meta->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  std::string raw;
  if (!txn.Get(snap_opts(txn), encode_string_key(key), &raw).ok()) {
    return std::string{};
  }

  auto const len = static_cast<int64_t>(raw.size());
  if (start < 0) start += len;
  if (end < 0) end += len;
  if (start < 0) start = 0;
  if (start >= len || start > end) {
    return std::string{};
  }
  if (end >= len) end = len - 1;
  return raw.substr(static_cast<std::size_t>(start), static_cast<std::size_t>(end - start + 1));
}

auto EmbeddedRedis::getrange(std::string_view key, int64_t start, int64_t end) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    return impl_->getrange(txn, key, start, end);
  });
}

/**
 * @brief 文字列値の指定位置から上書きする
 *
 * @copydoc EmbeddedRedis::setrange
 */
auto EmbeddedRedis::setrange(std::string_view key, uint64_t offset, std::string_view value) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto existing = impl_->get_meta(txn, key);
    if (existing && existing->type != DataType::String) {
      return std::unexpected(ErrorCode::WrongType);
    }

    std::string current;
    if (existing) {
      std::ignore = txn.Get(Impl::snap_opts(txn), encode_string_key(key), &current);
    }

    auto const needed = offset + value.size();
    if (current.size() < needed) {
      current.resize(needed, '\0');
    }
    current.replace(offset, value.size(), value);

    MetaValue meta;
    meta.type    = DataType::String;
    meta.version = existing ? existing->version : 1;
    // Redis 互換: 値を書き換えるだけの操作は TTL を維持する
    meta.expiration_ms = existing ? existing->expiration_ms : 0;
    meta.size    = current.size();
    impl_->put_meta(txn, key, meta);
    std::ignore = txn.Put(encode_string_key(key), rocksdb::Slice(current));

    return static_cast<uint64_t>(current.size());
  });
}

// ============================================================
// Hashes
// ============================================================

/**
 * @brief ハッシュフィールドに値を設定する
 *
 * @copydoc EmbeddedRedis::hset
 */
/** @brief hset の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::hset(rocksdb::Transaction& txn, std::string_view key, std::string_view field, std::string_view value) -> Result<bool> {
  auto existing = get_meta(txn, key);
  if (existing && existing->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }

  MetaValue meta;
  if (existing) {
    meta = *existing;
  } else {
    meta.type = DataType::Hash;
  }

  auto const hk = encode_hash_key(key, meta.version, field);

  // フィールドが既存かどうかを事前確認し、新規ならサイズを増やす
  std::string dummy;
  bool const  is_new = !txn.Get(snap_opts(txn), hk, &dummy).ok();
  if (is_new) {
    meta.size++;
  }

  std::ignore = txn.Put(hk, rocksdb::Slice(value.data(), value.size()));
  put_meta(txn, key, meta);

  return is_new;
}

auto EmbeddedRedis::hset(std::string_view key, std::string_view field, std::string_view value) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->hset(txn, key, field, value);
  });
}

/**
 * @brief ハッシュフィールド値を取得する
 *
 * @copydoc EmbeddedRedis::hget
 */
auto EmbeddedRedis::hget(std::string_view key, std::string_view field) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }

    std::string value;
    if (!txn.Get(Impl::snap_opts(txn), encode_hash_key(key, meta->version, field), &value).ok()) {
      return std::unexpected(ErrorCode::NotFound);
    }
    return value;
  });
}

/**
 * @brief ハッシュの全フィールドを取得する
 *
 * @copydoc EmbeddedRedis::hgetall
 */
auto EmbeddedRedis::hgetall(std::string_view key) -> Result<std::unordered_map<std::string, std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::unordered_map<std::string, std::string>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unordered_map<std::string, std::string>{}; // Redis 互換: 存在しないキーは空
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const pfx = encode_hash_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::unordered_map<std::string, std::string> result;
    result.reserve(meta->size);

    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      auto const field = extract_suffix(std::string_view(k.data(), k.size()), 1, key);
      result.emplace(std::string(field), it->value().ToString());
    }
    return result;
  });
}

/**
 * @brief ハッシュのフィールドを削除する
 *
 * @copydoc EmbeddedRedis::hdel
 */
/** @brief hdel の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::hdel(rocksdb::Transaction& txn, std::string_view key, std::string_view field) -> Result<bool> {
  auto meta = get_meta(txn, key);
  if (!meta) {
    return false;
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const hk = encode_hash_key(key, meta->version, field);
  std::string exist;
  if (!txn.Get(snap_opts(txn), hk, &exist).ok()) {
    return false;
  }

  std::ignore = txn.Delete(hk);
  meta->size--;
  if (meta->size == 0) {
    std::ignore = txn.Delete(encode_meta_key(key));
  } else {
    put_meta(txn, key, *meta);
  }

  return true;
}

auto EmbeddedRedis::hdel(std::string_view key, std::string_view field) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->hdel(txn, key, field);
  });
}

/**
 * @brief ハッシュのフィールド存在確認
 *
 * @copydoc EmbeddedRedis::hexists
 */
auto EmbeddedRedis::hexists(std::string_view key, std::string_view field) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return false;
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }
    std::string v;
    return txn.Get(Impl::snap_opts(txn), encode_hash_key(key, meta->version, field), &v).ok();
  });
}

/**
 * @brief ハッシュのフィールド数を取得する
 *
 * @copydoc EmbeddedRedis::hlen
 */
auto EmbeddedRedis::hlen(std::string_view key) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }
    return meta->size;
  });
}

/**
 * @brief ハッシュの全フィールド名を取得する
 *
 * @copydoc EmbeddedRedis::hkeys
 */
auto EmbeddedRedis::hkeys(std::string_view key) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::vector<std::string>{};
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const pfx = encode_hash_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::vector<std::string> result;
    result.reserve(meta->size);

    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      result.emplace_back(extract_suffix(std::string_view(k.data(), k.size()), 1, key));
    }
    return result;
  });
}

/**
 * @brief ハッシュの全フィールド値を取得する
 *
 * @copydoc EmbeddedRedis::hvals
 */
auto EmbeddedRedis::hvals(std::string_view key) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::vector<std::string>{};
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const pfx = encode_hash_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::vector<std::string> result;
    result.reserve(meta->size);

    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      result.emplace_back(it->value().ToString());
    }
    return result;
  });
}

// ---- New Hash Functions ----

/**
 * @brief フィールドが存在しない場合のみ設定する
 *
 * @copydoc EmbeddedRedis::hsetnx
 */
auto EmbeddedRedis::hsetnx(std::string_view key, std::string_view field, std::string_view value) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    auto existing = impl_->get_meta(txn, key);
    if (existing && existing->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }

    MetaValue meta;
    if (existing) {
      meta = *existing;
    } else {
      meta.type = DataType::Hash;
    }

    auto const hk = encode_hash_key(key, meta.version, field);
    std::string dummy;
    if (txn.Get(Impl::snap_opts(txn), hk, &dummy).ok()) {
      return false;
    }

    if (!existing) {
      meta.size = 1;
    } else {
      meta.size++;
    }

    std::ignore = txn.Put(hk, rocksdb::Slice(value.data(), value.size()));
    impl_->put_meta(txn, key, meta);

    return true;
  });
}

/**
 * @brief ハッシュフィールドの値を整数でインクリメントする
 *
 * @copydoc EmbeddedRedis::hincrby
 */
auto EmbeddedRedis::hincrby(std::string_view key, std::string_view field, int64_t delta) -> Result<int64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<int64_t> {
    auto existing = impl_->get_meta(txn, key);
    if (existing && existing->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }

    MetaValue meta;
    if (existing) {
      meta = *existing;
    } else {
      meta.type = DataType::Hash;
    }

    auto const hk = encode_hash_key(key, meta.version, field);
    std::string raw;
    int64_t val = 0;
    bool const found = txn.Get(Impl::snap_opts(txn), hk, &raw).ok();
    if (found) {
      if (!parse_number_strict(raw, val)) {
        return std::unexpected(ErrorCode::WrongType);
      }
    } else {
      meta.size++;
    }

    val += delta;
    auto const new_str = std::to_string(val);

    std::ignore = txn.Put(hk, new_str);
    impl_->put_meta(txn, key, meta);

    return val;
  });
}

/**
 * @brief 複数フィールドの値を取得する
 *
 * @copydoc EmbeddedRedis::hmget
 */
auto EmbeddedRedis::hmget(std::string_view key, std::vector<std::string_view> const& fields) -> Result<std::vector<std::optional<std::string>>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::optional<std::string>>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::vector<std::optional<std::string>>(fields.size(), std::nullopt);
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }

    std::vector<std::optional<std::string>> result;
    result.reserve(fields.size());
    for (auto const& field : fields) {
      std::string raw;
      if (txn.Get(Impl::snap_opts(txn), encode_hash_key(key, meta->version, field), &raw).ok()) {
        result.emplace_back(std::move(raw));
      } else {
        result.emplace_back(std::nullopt);
      }
    }
    return result;
  });
}

/**
 * @brief ハッシュフィールド値の長さを取得する
 *
 * @copydoc EmbeddedRedis::hstrlen
 */
auto EmbeddedRedis::hstrlen(std::string_view key, std::string_view field) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }
    std::string raw;
    if (!txn.Get(Impl::snap_opts(txn), encode_hash_key(key, meta->version, field), &raw).ok()) {
      return 0;
    }
    return static_cast<uint64_t>(raw.size());
  });
}

/**
 * @brief ハッシュからランダムなフィールド名を取得する
 *
 * @copydoc EmbeddedRedis::hrandfield
 */
auto EmbeddedRedis::hrandfield(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::Hash) {
      return std::unexpected(ErrorCode::WrongType);
    }
    if (meta->size == 0) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const pfx = encode_hash_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    // 最初のフィールドを返す（組み込み用途では十分ランダム）
    it->Seek(pfx_slice);
    if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
      return std::unexpected(ErrorCode::NotFound);
    }
    return std::string(extract_suffix(std::string_view(it->key().data(), it->key().size()), 1, key));
  });
}

// ============================================================
// Lists
// ============================================================

/**
 * @brief リストメタデータを初期化する
 * @param existing 既存のメタデータ（なければ新規作成）
 * @return 初期化済みメタデータ
 * @note 初期シーケンス番号は uint64_t の中央値を使用し、lpush/rpush 両方向への拡張を許容する
 */
static MetaValue init_list_meta(std::optional<MetaValue> const& existing) {
  if (existing) {
    return *existing;
  }
  MetaValue m;
  m.type     = DataType::List;
  m.head_seq = std::numeric_limits<uint64_t>::max() / 2;
  m.tail_seq = std::numeric_limits<uint64_t>::max() / 2;
  return m;
}

/**
 * @brief リスト先頭に要素を追加する
 *
 * @copydoc EmbeddedRedis::lpush
 */
/** @brief lpush の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::lpush(rocksdb::Transaction& txn, std::string_view key, std::string_view value) -> Result<uint64_t> {
  auto const existing = get_meta(txn, key);
  if (existing && existing->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto meta = init_list_meta(existing);
  meta.head_seq--;
  meta.size++;

  std::ignore = txn.Put(encode_list_key(key, meta.version, meta.head_seq), rocksdb::Slice(value.data(), value.size()));
  put_meta(txn, key, meta);

  return meta.size;
}

auto EmbeddedRedis::lpush(std::string_view key, std::string_view value) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    return impl_->lpush(txn, key, value);
  });
}

/**
 * @brief リスト末尾に要素を追加する
 *
 * @copydoc EmbeddedRedis::rpush
 */
/** @brief rpush の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::rpush(rocksdb::Transaction& txn, std::string_view key, std::string_view value) -> Result<uint64_t> {
  auto const existing = get_meta(txn, key);
  if (existing && existing->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto meta = init_list_meta(existing);

  std::ignore = txn.Put(encode_list_key(key, meta.version, meta.tail_seq), rocksdb::Slice(value.data(), value.size()));
  meta.tail_seq++;
  meta.size++;
  put_meta(txn, key, meta);

  return meta.size;
}

auto EmbeddedRedis::rpush(std::string_view key, std::string_view value) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    return impl_->rpush(txn, key, value);
  });
}

/**
 * @brief リスト先頭要素を削除して取得する
 *
 * @copydoc EmbeddedRedis::lpop
 */
auto EmbeddedRedis::lpop(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::List) {
      return std::unexpected(ErrorCode::WrongType);
    }
    if (meta->size == 0) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const lk = encode_list_key(key, meta->version, meta->head_seq);
    std::string value;
    if (!txn.Get(Impl::snap_opts(txn), lk, &value).ok()) {
      return std::unexpected(ErrorCode::NotFound);
    }

    std::ignore = txn.Delete(lk);
    meta->head_seq++;
    meta->size--;

    // 空になったらメタごと削除、そうでなければ更新
    if (meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(key));
    } else {
      impl_->put_meta(txn, key, *meta);
    }

    return value;
  });
}

/**
 * @brief リスト末尾要素を削除して取得する
 *
 * @copydoc EmbeddedRedis::rpop
 */
auto EmbeddedRedis::rpop(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::List) {
      return std::unexpected(ErrorCode::WrongType);
    }
    if (meta->size == 0) {
      return std::unexpected(ErrorCode::NotFound);
    }

    meta->tail_seq--;
    auto const lk = encode_list_key(key, meta->version, meta->tail_seq);
    std::string value;
    if (!txn.Get(Impl::snap_opts(txn), lk, &value).ok()) {
      return std::unexpected(ErrorCode::NotFound);
    }

    std::ignore = txn.Delete(lk);
    meta->size--;

    if (meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(key));
    } else {
      impl_->put_meta(txn, key, *meta);
    }

    return value;
  });
}

/**
 * @brief リストの範囲内の要素を取得する
 *
 * @copydoc EmbeddedRedis::lrange
 */
/** @brief lrange の実体（引数を書き換えるため、再試行ごとに新しいコピーで実行する） */
auto EmbeddedRedis::Impl::lrange(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  auto const meta = get_meta_ro(txn, key);
  if (!meta) {
    // Redis 互換: 存在しないキーに対しては空リストを返す
    return std::vector<std::string>{};
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const size = static_cast<int64_t>(meta->size);
  if (size == 0) {
    return std::vector<std::string>{};
  }

  // 負のインデックスを正に変換
  if (start < 0) start += size;
  if (stop < 0) stop += size;

  // クランプ
  if (start < 0) start = 0;
  if (stop < 0) stop = 0;
  if (start >= size) start = size;
  if (stop >= size) stop = size - 1;

  std::vector<std::string> result;
  if (start > stop) {
    return result;
  }
  result.reserve(static_cast<std::size_t>(stop - start + 1));

  // 実在する要素を head から順に数えて位置を決める。
  // lrem はシーケンス番号を振り直さないため「head_seq + index」の算術は成立せず、
  // 穴の空いたシーケンス空間では別の要素を指してしまう（lindex/lset と同じ走査方式に揃える）。
  auto const           pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const           seek_key   = encode_list_key(key, meta->version, meta->head_seq);
  auto const           seq_offset = suffix_offset(key);

  std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));

  int64_t pos = 0;
  for (it->Seek(seek_key); it->Valid() && pos <= stop; it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) {
      break;
    }
    if (static_cast<std::size_t>(k.size()) < seq_offset + 8) {
      break;
    }
    auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
    if (seq >= meta->tail_seq) {
      break;
    }
    if (pos >= start) {
      result.emplace_back(it->value().ToString());
    }
    pos++;
  }
  return result;
}

auto EmbeddedRedis::lrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {
    return impl_->lrange(txn, key, start, stop);
  });
}

/**
 * @brief リストから指定値を削除する
 *
 * @copydoc EmbeddedRedis::lrem
 */
/** @brief lrem の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::lrem(rocksdb::Transaction& txn, std::string_view key, int64_t count, std::string_view value) -> Result<uint64_t> {
  auto meta = get_meta(txn, key);
  if (!meta) {
    return 0; // Redis 互換: 存在しないキーに対しては 0
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }
  if (meta->size == 0) {
    return 0;
  }

  uint64_t const limit = (count == 0) ? std::numeric_limits<uint64_t>::max() : static_cast<uint64_t>(std::abs(count));

  auto const pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const seq_offset = suffix_offset(key);

  std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));

  // 全要素を (シーケンス, 値) のリストとして取得
  std::vector<std::pair<uint64_t, std::string>> all;
  all.reserve(meta->size);
  for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) {
      break;
    }
    auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
    all.emplace_back(seq, it->value().ToString());
  }

  // 削除対象のシーケンスを決定
  std::vector<uint64_t> to_delete;
  uint64_t removed = 0;
  if (count >= 0) {
    // 先頭からスキャン
    for (auto const& [seq, v] : all) {
      if (removed >= limit) {
        break;
      }
      if (v == value) {
        to_delete.push_back(seq);
        removed++;
      }
    }
  } else {
    // 末尾からスキャン
    for (auto rit = all.rbegin(); rit != all.rend() && removed < limit; ++rit) {
      if (rit->second == value) {
        to_delete.push_back(rit->first);
        removed++;
      }
    }
  }

  if (to_delete.empty()) {
    return 0;
  }

  for (auto const seq : to_delete) {
    std::ignore = txn.Delete(encode_list_key(key, meta->version, seq));
  }

  meta->size -= removed;
  if (meta->size == 0) {
    std::ignore = txn.Delete(encode_meta_key(key));
  } else {
    // 残存要素から head_seq / tail_seq を再計算
    // （先頭・末尾の要素が削除された場合に対応するため）
    std::unordered_set<uint64_t> const deleted_seqs(to_delete.begin(), to_delete.end());
    std::optional<uint64_t>            new_head;
    std::optional<uint64_t>            new_tail;
    for (auto const& [seq, v] : all) {
      if (!deleted_seqs.contains(seq)) {
        if (!new_head) {
          new_head = seq;
        }
        new_tail = seq;
      }
    }
    if (new_head && new_tail) {
      meta->head_seq = *new_head;
      meta->tail_seq = *new_tail + 1;
    }
    put_meta(txn, key, *meta);
  }

  return removed;
}

auto EmbeddedRedis::lrem(std::string_view key, int64_t count, std::string_view value) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    return impl_->lrem(txn, key, count, value);
  });
}

/**
 * @brief リストを範囲でトリムする（範囲外を削除）
 *
 * @copydoc EmbeddedRedis::ltrim
 */
/** @brief ltrim の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::ltrim(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<void> {
  auto meta = get_meta(txn, key);
  if (!meta) {
    return {}; // Redis 互換: 存在しないキーへの LTRIM は成功扱い
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const size = static_cast<int64_t>(meta->size);

  // 負のインデックスを正に変換
  if (start < 0) start += size;
  if (stop < 0) stop += size;

  // クランプ
  if (start < 0) start = 0;
  if (stop >= size) stop = size - 1;

  if (start > stop || size == 0) {
    // 全削除
    erase_key_data(txn, key, *meta);
    return {};
  }

  // lrem が残したシーケンス空間の穴を跨ぐため、実在する要素を走査して位置を数える。
  // 「head_seq + index」で範囲を決めると穴の分だけ境界がずれ、残すべき要素まで消える。
  auto const           pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const           seek_key   = encode_list_key(key, meta->version, meta->head_seq);
  auto const           seq_offset = suffix_offset(key);

  std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));

  std::optional<uint64_t> new_head;
  std::optional<uint64_t> new_tail;
  int64_t                 pos = 0;
  for (it->Seek(seek_key); it->Valid(); it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) {
      break;
    }
    if (static_cast<std::size_t>(k.size()) < seq_offset + 8) {
      break;
    }
    auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
    if (seq >= meta->tail_seq) {
      break;
    }
    if (pos < start || pos > stop) {
      std::ignore = txn.Delete(k);
    } else {
      if (!new_head) {
        new_head = seq;
      }
      new_tail = seq;
    }
    pos++;
  }

  if (!new_head) {
    // 残る要素がない（メタの size と実データが食い違っていた場合を含む）
    erase_key_data(txn, key, *meta);
    return {};
  }

  meta->head_seq = *new_head;
  meta->tail_seq = *new_tail + 1;
  meta->size     = static_cast<uint64_t>(stop - start + 1);
  put_meta(txn, key, *meta);
  return {};
}

auto EmbeddedRedis::ltrim(std::string_view key, int64_t start, int64_t stop) -> Result<void> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<void> {
    return impl_->ltrim(txn, key, start, stop);
  });
}

/**
 * @brief リストの長さを取得する
 *
 * @copydoc EmbeddedRedis::llen
 */
auto EmbeddedRedis::llen(std::string_view key) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::List) {
      return std::unexpected(ErrorCode::WrongType);
    }
    return meta->size;
  });
}

/**
 * @brief リストの指定インデックスの要素を取得する
 *
 * @copydoc EmbeddedRedis::lindex
 */
/** @brief lindex の実体（引数を書き換えるため、再試行ごとに新しいコピーで実行する） */
auto EmbeddedRedis::Impl::lindex(rocksdb::Transaction& txn, std::string_view key, int64_t index) -> Result<std::string> {
  auto const meta = get_meta_ro(txn, key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const size = static_cast<int64_t>(meta->size);
  if (size == 0) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (index < 0) index += size;
  if (index < 0 || index >= size) {
    return std::unexpected(ErrorCode::NotFound);
  }

  auto const pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const seek_key = encode_list_key(key, meta->version, meta->head_seq);
  auto const seq_offset = suffix_offset(key);

  std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));

  int64_t pos = 0;
  for (it->Seek(seek_key); it->Valid(); it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) {
      break;
    }
    auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
    if (seq >= meta->tail_seq) {
      break;
    }
    if (pos == index) {
      return it->value().ToString();
    }
    pos++;
  }
  return std::unexpected(ErrorCode::NotFound);
}

auto EmbeddedRedis::lindex(std::string_view key, int64_t index) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    return impl_->lindex(txn, key, index);
  });
}

// ---- New List Functions ----

/**
 * @brief リストの指定インデックスに値を設定する
 *
 * @copydoc EmbeddedRedis::lset
 */
/** @brief lset の実体（引数を書き換えるため、再試行ごとに新しいコピーで実行する） */
auto EmbeddedRedis::Impl::lset(rocksdb::Transaction& txn, std::string_view key, int64_t index, std::string_view value) -> Result<void> {
  auto meta = get_meta(txn, key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const size = static_cast<int64_t>(meta->size);
  if (size == 0) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (index < 0) index += size;
  if (index < 0 || index >= size) {
    return std::unexpected(ErrorCode::NotFound);
  }

  auto const pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const seek_key = encode_list_key(key, meta->version, meta->head_seq);
  auto const seq_offset = suffix_offset(key);

  std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));

  int64_t pos = 0;
  for (it->Seek(seek_key); it->Valid(); it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) {
      break;
    }
    auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
    if (seq >= meta->tail_seq) {
      break;
    }
    if (pos == index) {
      std::ignore = txn.Put(k, rocksdb::Slice(value.data(), value.size()));
      return {};
    }
    pos++;
  }
  return std::unexpected(ErrorCode::NotFound);
}

auto EmbeddedRedis::lset(std::string_view key, int64_t index, std::string_view value) -> Result<void> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<void> {
    return impl_->lset(txn, key, index, value);
  });
}

/**
 * @brief リスト内の要素の位置を検索する
 *
 * @copydoc EmbeddedRedis::lpos
 */
auto EmbeddedRedis::lpos(std::string_view key, std::string_view element) -> Result<int64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<int64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::List) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const pfx = encode_list_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);
    auto const seek_key = encode_list_key(key, meta->version, meta->head_seq);
    auto const seq_offset = suffix_offset(key);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    int64_t pos = 0;
    for (it->Seek(seek_key); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
      if (seq >= meta->tail_seq) {
        break;
      }
      if (it->value().ToString() == element) {
        return pos;
      }
      pos++;
    }
    return std::unexpected(ErrorCode::NotFound);
  });
}

/**
 * @brief リストが存在する場合のみ先頭に追加する
 *
 * @copydoc EmbeddedRedis::lpushx
 */
auto EmbeddedRedis::lpushx(std::string_view key, std::string_view value) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::List) {
      return std::unexpected(ErrorCode::WrongType);
    }

    meta->head_seq--;
    meta->size++;

    std::ignore = txn.Put(encode_list_key(key, meta->version, meta->head_seq), rocksdb::Slice(value.data(), value.size()));
    impl_->put_meta(txn, key, *meta);

    return meta->size;
  });
}

/**
 * @brief リストが存在する場合のみ末尾に追加する
 *
 * @copydoc EmbeddedRedis::rpushx
 */
auto EmbeddedRedis::rpushx(std::string_view key, std::string_view value) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::List) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const seq = meta->tail_seq;
    meta->tail_seq++;
    meta->size++;

    std::ignore = txn.Put(encode_list_key(key, meta->version, seq), rocksdb::Slice(value.data(), value.size()));
    impl_->put_meta(txn, key, *meta);

    return meta->size;
  });
}

/**
 * @brief ピボット値の前後に要素を挿入する
 *
 * @copydoc EmbeddedRedis::linsert
 */
auto EmbeddedRedis::linsert(std::string_view key, InsertPosition pos, std::string_view pivot, std::string_view value) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) return 0; // Redis 互換: 存在しないキーは 0
    if (meta->type != DataType::List) return std::unexpected(ErrorCode::WrongType);
    if (meta->size == 0) return 0;

    auto const pfx = encode_list_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);
    auto const seq_offset = suffix_offset(key);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::vector<uint64_t> seqs;
    std::vector<std::string> vals;
    seqs.reserve(meta->size);
    vals.reserve(meta->size);
    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      if (static_cast<std::size_t>(k.size()) < seq_offset + 8) break;
      auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
      seqs.push_back(seq);
      vals.emplace_back(it->value().data(), it->value().size());
    }

    auto const pivot_it = std::find(vals.begin(), vals.end(), pivot);
    if (pivot_it == vals.end()) return std::unexpected(ErrorCode::NotFound);

    auto const pivot_idx = static_cast<std::size_t>(std::distance(vals.begin(), pivot_it));
    auto const insert_idx = (pos == InsertPosition::Before) ? pivot_idx : pivot_idx + 1;

    vals.insert(vals.begin() + static_cast<ptrdiff_t>(insert_idx), std::string(value));

    // Bump version to avoid seq collisions and delete old elements by prefix
    auto const old_version = meta->version;
    meta->version = meta->version + 1;
    auto const new_version = meta->version;

    impl_->delete_by_prefix(txn, encode_list_prefix(key, old_version));

    // write new elements under new_version starting at a midpoint to allow lpush/rpush
    auto const base = std::numeric_limits<uint64_t>::max() / 2;
    for (std::size_t i = 0; i < vals.size(); ++i) {
      std::ignore = txn.Put(encode_list_key(key, new_version, base + i), vals[i]);
    }

    meta->head_seq = base;
    meta->tail_seq = base + vals.size();
    meta->size = vals.size();
    impl_->put_meta(txn, key, *meta);

    // 並行更新の検出はトランザクションが行う。メタキーは get_meta が GetForUpdate で
    // 読んでいるため、他スレッドが割り込んだ場合はコミットが競合し run_txn が再実行する。
    return meta->size;
  });
}

/**
 * @brief ソースリストから要素を取り出し、宛先リストに挿入する
 *
 * @copydoc EmbeddedRedis::lmove
 */
auto EmbeddedRedis::lmove(std::string_view source, std::string_view destination, ListSide from, ListSide to) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {

    auto src_meta = impl_->get_meta(txn, source);
    if (!src_meta) return std::unexpected(ErrorCode::NotFound);
    if (src_meta->type != DataType::List) return std::unexpected(ErrorCode::WrongType);
    if (src_meta->size == 0) return std::unexpected(ErrorCode::NotFound);

    if (source == destination) {
      auto const pfx = encode_list_prefix(source, src_meta->version);
      rocksdb::Slice const pfx_slice(pfx);
      std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

      std::vector<std::string> values;
      values.reserve(src_meta->size);
      for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
        auto const k = it->key();
        if (!k.starts_with(pfx_slice)) {
          break;
        }
        values.emplace_back(it->value().data(), it->value().size());
      }
      if (values.empty()) {
        return std::unexpected(ErrorCode::NotFound);
      }

      auto const moved = from == ListSide::Left ? values.front() : values.back();
      if (values.size() == 1 || from == to) {
        return moved;
      }

      if (from == ListSide::Left) {
        std::rotate(values.begin(), values.begin() + 1, values.end());
      } else {
        std::rotate(values.rbegin(), values.rbegin() + 1, values.rend());
      }

      auto const old_version = src_meta->version;
      src_meta->version = old_version + 1;
      impl_->delete_by_prefix(txn, encode_list_prefix(source, old_version));

      auto const base = std::numeric_limits<uint64_t>::max() / 2;
      for (std::size_t i = 0; i < values.size(); ++i) {
        std::ignore = txn.Put(encode_list_key(source, src_meta->version, base + i), values[i]);
      }
      src_meta->head_seq = base;
      src_meta->tail_seq = base + values.size();
      impl_->put_meta(txn, source, *src_meta);

      return moved;
    }

    auto dst_meta = impl_->get_meta(txn, destination);
    if (dst_meta && dst_meta->type != DataType::List) return std::unexpected(ErrorCode::WrongType);

    std::string value;

    // pop from source
    uint64_t popped_seq = 0;
    if (from == ListSide::Left) {
      popped_seq = src_meta->head_seq;
      auto const lk = encode_list_key(source, src_meta->version, popped_seq);
      std::string tmp;
      if (!txn.Get(Impl::snap_opts(txn), lk, &tmp).ok()) return std::unexpected(ErrorCode::NotFound);
      value = std::move(tmp);
      std::ignore = txn.Delete(lk);
      src_meta->head_seq++;
    } else {
      src_meta->tail_seq--;
      popped_seq = src_meta->tail_seq;
      auto const lk = encode_list_key(source, src_meta->version, popped_seq);
      std::string tmp;
      if (!txn.Get(Impl::snap_opts(txn), lk, &tmp).ok()) return std::unexpected(ErrorCode::NotFound);
      value = std::move(tmp);
      std::ignore = txn.Delete(lk);
    }
    src_meta->size--;

    if (src_meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(source));
    } else {
      impl_->put_meta(txn, source, *src_meta);
    }

    // push to destination
    // 新規リストは init_list_meta と同じ中央値から開始する。0 から始めると
    // 直後の lpush で head_seq が 0 を下回って uint64_t が一周し、キー順が崩れる。
    if (!dst_meta) {
      dst_meta = init_list_meta(std::nullopt);
    }
    if (to == ListSide::Left) {
      dst_meta->head_seq--;
      std::ignore = txn.Put(encode_list_key(destination, dst_meta->version, dst_meta->head_seq), value);
    } else {
      std::ignore = txn.Put(encode_list_key(destination, dst_meta->version, dst_meta->tail_seq), value);
      dst_meta->tail_seq++;
    }
    dst_meta->size++;
    impl_->put_meta(txn, destination, *dst_meta);

    return value;
  });
}

/**
 * @brief ソースリストの末尾から要素を取り出し、宛先リストの先頭に挿入する
 *
 * @copydoc EmbeddedRedis::rpoplpush
 */
auto EmbeddedRedis::rpoplpush(std::string_view source, std::string_view destination) -> Result<std::string> {
  return lmove(source, destination, ListSide::Right, ListSide::Left);
}

// ============================================================
// Sets
// ============================================================

/**
 * @brief セットにメンバーを追加する
 *
 * @copydoc EmbeddedRedis::sadd
 */
/** @brief sadd の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::sadd(rocksdb::Transaction& txn, std::string_view key, std::string_view member) -> Result<bool> {
  auto const existing = get_meta(txn, key);
  if (existing && existing->type != DataType::Set) {
    return std::unexpected(ErrorCode::WrongType);
  }

  MetaValue meta;
  if (existing) {
    meta = *existing;
  } else {
    meta.type = DataType::Set;
  }

  auto const sk = encode_set_key(key, meta.version, member);

  // メンバーの重複チェック
  std::string dummy;
  bool const  is_new = !txn.Get(snap_opts(txn), sk, &dummy).ok();
  if (is_new) {
    meta.size++;
  }

  std::ignore = txn.Put(sk, rocksdb::Slice());
  put_meta(txn, key, meta);

  return is_new;
}

auto EmbeddedRedis::sadd(std::string_view key, std::string_view member) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->sadd(txn, key, member);
  });
}

/**
 * @brief セットの全メンバーを取得する
 *
 * @copydoc EmbeddedRedis::smembers
 */
auto EmbeddedRedis::smembers(std::string_view key) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::vector<std::string>{}; // Redis 互換: 存在しないキーは空
    }
    if (meta->type != DataType::Set) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const pfx = encode_set_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::vector<std::string> result;
    result.reserve(meta->size);

    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      result.emplace_back(extract_suffix(std::string_view(k.data(), k.size()), 1, key));
    }
    return result;
  });
}

/**
 * @brief セットからメンバーを削除する
 *
 * @copydoc EmbeddedRedis::srem
 */
/** @brief srem の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::srem(rocksdb::Transaction& txn, std::string_view key, std::string_view member) -> Result<bool> {
  auto meta = get_meta(txn, key);
  if (!meta) {
    return false; // Redis 互換: 存在しないキーは削除なし
  }
  if (meta->type != DataType::Set) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const sk = encode_set_key(key, meta->version, member);

  // メンバーの存在確認
  std::string dummy;
  if (!txn.Get(snap_opts(txn), sk, &dummy).ok()) {
    return false;
  }

  std::ignore = txn.Delete(sk);

  meta->size--;
  if (meta->size == 0) {
    std::ignore = txn.Delete(encode_meta_key(key));
  } else {
    put_meta(txn, key, *meta);
  }

  return true;
}

auto EmbeddedRedis::srem(std::string_view key, std::string_view member) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->srem(txn, key, member);
  });
}

/**
 * @brief セットのメンバー数を取得する
 *
 * @copydoc EmbeddedRedis::scard
 */
auto EmbeddedRedis::scard(std::string_view key) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::Set) {
      return std::unexpected(ErrorCode::WrongType);
    }
    return meta->size;
  });
}

/**
 * @brief セットのメンバー存在確認
 *
 * @copydoc EmbeddedRedis::sismember
 */
auto EmbeddedRedis::sismember(std::string_view key, std::string_view member) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return false;
    }
    if (meta->type != DataType::Set) {
      return std::unexpected(ErrorCode::WrongType);
    }
    std::string v;
    return txn.Get(Impl::snap_opts(txn), encode_set_key(key, meta->version, member), &v).ok();
  });
}

/**
 * @brief セットからランダムなメンバーを削除して取得する
 *
 * @copydoc EmbeddedRedis::spop
 */
auto EmbeddedRedis::spop(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::Set) {
      return std::unexpected(ErrorCode::WrongType);
    }
    if (meta->size == 0) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const pfx = encode_set_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    it->Seek(pfx_slice);
    if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const member_key = it->key();
    auto const member = extract_suffix(std::string_view(member_key.data(), member_key.size()), 1, key);

    std::ignore = txn.Delete(member_key);
    meta->size--;
    if (meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(key));
    } else {
      impl_->put_meta(txn, key, *meta);
    }

    return std::string(member);
  });
}

/**
 * @brief セットからランダムなメンバーを取得する（削除しない）
 *
 * @copydoc EmbeddedRedis::srandmember
 */
auto EmbeddedRedis::srandmember(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::Set) {
      return std::unexpected(ErrorCode::WrongType);
    }
    if (meta->size == 0) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const pfx = encode_set_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    it->Seek(pfx_slice);
    if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
      return std::unexpected(ErrorCode::NotFound);
    }
    return std::string(extract_suffix(std::string_view(it->key().data(), it->key().size()), 1, key));
  });
}

/**
 * @brief セット間でメンバーを移動する
 *
 * @copydoc EmbeddedRedis::smove
 */
auto EmbeddedRedis::smove(std::string_view source, std::string_view destination, std::string_view member) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {

    auto src_meta = impl_->get_meta(txn, source);
    if (!src_meta) return false;
    if (src_meta->type != DataType::Set) return std::unexpected(ErrorCode::WrongType);

    auto const src_key = encode_set_key(source, src_meta->version, member);
    std::string dummy;
    if (!txn.Get(Impl::snap_opts(txn), src_key, &dummy).ok()) return false;

    auto dst_meta = impl_->get_meta(txn, destination);
    if (dst_meta && dst_meta->type != DataType::Set) return std::unexpected(ErrorCode::WrongType);

    std::ignore = txn.Delete(src_key);
    src_meta->size--;
    if (src_meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(source));
    } else {
      impl_->put_meta(txn, source, *src_meta);
    }

    if (!dst_meta) {
      dst_meta = MetaValue{};
      dst_meta->type = DataType::Set;
    }
    auto const dst_key = encode_set_key(destination, dst_meta->version, member);
    std::string dst_dummy;
    bool const is_new_dst = !txn.Get(Impl::snap_opts(txn), dst_key, &dst_dummy).ok();
    if (is_new_dst) {
      dst_meta->size++;
    }
    std::ignore = txn.Put(dst_key, rocksdb::Slice());
    impl_->put_meta(txn, destination, *dst_meta);

    return true;
  });
}

// ============================================================
// Sorted Sets
// ============================================================

/**
 * @brief ソート済みセットにスコア付きメンバーを追加する
 *
 * @copydoc EmbeddedRedis::zadd
 */
/** @brief zadd の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::zadd(rocksdb::Transaction& txn, std::string_view key, double score, std::string_view member) -> Result<bool> {
  auto const existing = get_meta(txn, key);
  if (existing && existing->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }

  MetaValue meta;
  if (existing) {
    meta = *existing;
  } else {
    meta.type = DataType::ZSet;
  }

  auto const mk = encode_zset_member_key(key, meta.version, member);

  // 既存スコアがあれば、新しいスコアキーと衝突しないよう古いスコアキーを削除する
  std::string old_score_raw;
  bool const  is_new = !txn.Get(snap_opts(txn), mk, &old_score_raw).ok();

  if (!is_new && old_score_raw.size() == 8) {
    auto const old_score = decode_score(reinterpret_cast<uint8_t const*>(old_score_raw.data()));
    std::ignore = txn.Delete(encode_zset_score_key(key, meta.version, old_score, member));
  }
  if (is_new) {
    meta.size++;
  }

  // member key → score(8byte encoded)
  auto const sb = encode_score(score);
  std::ignore = txn.Put(mk, rocksdb::Slice(reinterpret_cast<char const*>(sb.data()), 8));

  // score key → empty
  std::ignore = txn.Put(encode_zset_score_key(key, meta.version, score, member), rocksdb::Slice());
  put_meta(txn, key, meta);

  return is_new;
}

auto EmbeddedRedis::zadd(std::string_view key, double score, std::string_view member) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->zadd(txn, key, score, member);
  });
}

/**
 * @brief スコア範囲でメンバーを取得する
 *
 * @copydoc EmbeddedRedis::zrangebyscore
 */
auto EmbeddedRedis::zrangebyscore(std::string_view key, double min_score, double max_score) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::vector<std::string>{}; // Redis 互換: 存在しないキーは空
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const range_pfx  = encode_zset_score_range_prefix(key, meta->version);
    auto const seek_key   = encode_zset_score_seek_key(key, meta->version, min_score);
    rocksdb::Slice const pfx_slice(range_pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::vector<std::string> result;
    auto const               score_offset = suffix_offset(key); // prefix(1) + key + version(8)

    for (it->Seek(seek_key); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      if (static_cast<std::size_t>(k.size()) < score_offset + 8) {
        break;
      }
      auto const sc = decode_score(reinterpret_cast<uint8_t const*>(k.data() + score_offset));
      if (sc > max_score) {
        break;
      }
      result.emplace_back(k.data() + score_offset + 8, k.size() - score_offset - 8);
    }
    return result;
  });
}

/**
 * @brief ソート済みセットからメンバーを削除する
 *
 * @copydoc EmbeddedRedis::zrem
 */
/** @brief zrem の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::zrem(rocksdb::Transaction& txn, std::string_view key, std::string_view member) -> Result<bool> {
  auto meta = get_meta(txn, key);
  if (!meta) {
    return false;
  }
  if (meta->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const mk = encode_zset_member_key(key, meta->version, member);
  std::string score_raw;
  if (!txn.Get(snap_opts(txn), mk, &score_raw).ok()) {
    return false;
  }
  if (score_raw.size() != 8) {
    return false;
  }

  auto const old_score = decode_score(reinterpret_cast<uint8_t const*>(score_raw.data()));

  std::ignore = txn.Delete(mk);
  std::ignore = txn.Delete(encode_zset_score_key(key, meta->version, old_score, member));
  meta->size--;
  if (meta->size == 0) {
    std::ignore = txn.Delete(encode_meta_key(key));
  } else {
    put_meta(txn, key, *meta);
  }

  return true;
}

auto EmbeddedRedis::zrem(std::string_view key, std::string_view member) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->zrem(txn, key, member);
  });
}

/**
 * @brief ソート済みセットのメンバー数を取得する
 *
 * @copydoc EmbeddedRedis::zcard
 */
auto EmbeddedRedis::zcard(std::string_view key) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }
    return meta->size;
  });
}

/**
 * @brief スコア範囲内のメンバー数を取得する
 *
 * @copydoc EmbeddedRedis::zcount
 */
auto EmbeddedRedis::zcount(std::string_view key, double min_score, double max_score) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
    auto const seek_key  = encode_zset_score_seek_key(key, meta->version, min_score);
    rocksdb::Slice const pfx_slice(range_pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    uint64_t count = 0;
    auto const score_offset = suffix_offset(key);

    for (it->Seek(seek_key); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      if (static_cast<std::size_t>(k.size()) < score_offset + 8) {
        break;
      }
      auto const sc = decode_score(reinterpret_cast<uint8_t const*>(k.data() + score_offset));
      if (sc > max_score) {
        break;
      }
      count++;
    }
    return count;
  });
}

/**
 * @brief メンバーのスコアを取得する
 *
 * @copydoc EmbeddedRedis::zscore
 */
auto EmbeddedRedis::zscore(std::string_view key, std::string_view member) -> Result<double> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<double> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }

    std::string score_raw;
    if (!txn.Get(Impl::snap_opts(txn), encode_zset_member_key(key, meta->version, member), &score_raw).ok()) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (score_raw.size() != 8) {
      return std::unexpected(ErrorCode::NotFound);
    }
    return decode_score(reinterpret_cast<uint8_t const*>(score_raw.data()));
  });
}

/**
 * @brief メンバーのランクを取得する（昇順、0 ベース）
 *
 * @copydoc EmbeddedRedis::zrank
 */
auto EmbeddedRedis::zrank(std::string_view key, std::string_view member) -> Result<int64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<int64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(range_pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    auto const score_offset = suffix_offset(key);
    int64_t rank = 0;

    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      if (static_cast<std::size_t>(k.size()) < score_offset + 8) {
        break;
      }
      auto const m = std::string_view(k.data() + score_offset + 8, k.size() - score_offset - 8);
      if (m == member) {
        return rank;
      }
      rank++;
    }
    return std::unexpected(ErrorCode::NotFound);
  });
}

/**
 * @brief ランク範囲でメンバーを取得する（昇順）
 *
 * @copydoc EmbeddedRedis::zrange
 */
/** @brief zrange の実体（引数を書き換えるため、再試行ごとに新しいコピーで実行する） */
auto EmbeddedRedis::Impl::zrange(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  auto const meta = get_meta_ro(txn, key);
  if (!meta) {
    return std::vector<std::string>{};
  }
  if (meta->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto size = static_cast<int64_t>(meta->size);
  if (size == 0) {
    return std::vector<std::string>{};
  }
  if (start < 0) start += size;
  if (stop < 0) stop += size;
  if (start < 0) start = 0;
  if (stop < 0) stop = 0;
  if (start >= size) start = size;
  if (stop >= size) stop = size - 1;
  if (start > stop) {
    return std::vector<std::string>{};
  }

  auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(range_pfx);

  std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));

  std::vector<std::string> result;
  result.reserve(static_cast<std::size_t>(stop - start + 1));

  auto const score_offset = suffix_offset(key);
  int64_t pos = 0;

  for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) {
      break;
    }
    if (static_cast<std::size_t>(k.size()) < score_offset + 8) {
      break;
    }
    if (pos >= start && pos <= stop) {
      result.emplace_back(k.data() + score_offset + 8, k.size() - score_offset - 8);
    }
    pos++;
    if (pos > stop) {
      break;
    }
  }
  return result;
}

auto EmbeddedRedis::zrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {
    return impl_->zrange(txn, key, start, stop);
  });
}

// ---- New ZSet Functions ----

/**
 * @brief メンバーのスコアを指定量インクリメントする
 *
 * @copydoc EmbeddedRedis::zincrby
 */
auto EmbeddedRedis::zincrby(std::string_view key, std::string_view member, double delta) -> Result<double> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<double> {
    auto existing = impl_->get_meta(txn, key);
    if (existing && existing->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }

    MetaValue meta;
    if (existing) {
      meta = *existing;
    } else {
      meta.type = DataType::ZSet;
    }

    auto const mk = encode_zset_member_key(key, meta.version, member);
    std::string old_raw;
    bool const found = txn.Get(Impl::snap_opts(txn), mk, &old_raw).ok();

    double new_score = delta;
    if (found) {
      if (old_raw.size() == 8) {
        new_score = decode_score(reinterpret_cast<uint8_t const*>(old_raw.data())) + delta;
      }
    }
    if (!found) {
      meta.size++;
    }

    if (found && old_raw.size() == 8) {
      auto const old_score = decode_score(reinterpret_cast<uint8_t const*>(old_raw.data()));
      std::ignore = txn.Delete(encode_zset_score_key(key, meta.version, old_score, member));
    }
    auto const sb = encode_score(new_score);
    std::ignore = txn.Put(mk, rocksdb::Slice(reinterpret_cast<char const*>(sb.data()), 8));
    std::ignore = txn.Put(encode_zset_score_key(key, meta.version, new_score, member), rocksdb::Slice());
    impl_->put_meta(txn, key, meta);

    return new_score;
  });
}

/**
 * @brief メンバーのランクを取得する（降順、0 ベース）
 *
 * @copydoc EmbeddedRedis::zrevrank
 */
auto EmbeddedRedis::zrevrank(std::string_view key, std::string_view member) -> Result<int64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<int64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }

    auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(range_pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    auto const score_offset = suffix_offset(key);
    int64_t total = 0;
    int64_t target_rank = -1;

    // First pass: count total + find target
    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      if (static_cast<std::size_t>(k.size()) < score_offset + 8) break;
      auto const m = std::string_view(k.data() + score_offset + 8, k.size() - score_offset - 8);
      if (m == member) target_rank = total;
      total++;
    }
    if (target_rank < 0) return std::unexpected(ErrorCode::NotFound);
    return total - 1 - target_rank;
  });
}

/**
 * @brief ランク範囲でメンバーを取得する（降順）
 *
 * @copydoc EmbeddedRedis::zrevrange
 */
/** @brief zrevrange の実体（引数を書き換えるため、再試行ごとに新しいコピーで実行する） */
auto EmbeddedRedis::Impl::zrevrange(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  auto const meta = get_meta_ro(txn, key);
  if (!meta) {
    return std::vector<std::string>{};
  }
  if (meta->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(range_pfx);
  auto const           score_offset = suffix_offset(key);

  std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));

  // 昇順に全メンバーを集めてから反転スライスする
  std::vector<std::string> all;
  all.reserve(meta->size);
  for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) {
      break;
    }
    if (static_cast<std::size_t>(k.size()) < score_offset + 8) {
      break;
    }
    all.emplace_back(k.data() + score_offset + 8, k.size() - score_offset - 8);
  }

  auto const size = static_cast<int64_t>(all.size());
  if (size == 0) {
    return std::vector<std::string>{};
  }

  // クランプ規則は zrange と揃える。start を size-1 に丸めると
  // 範囲外指定（例: size=3 で zrevrange 5 10）が空にならず 1 件返ってしまう。
  if (start < 0) start += size;
  if (stop < 0) stop += size;
  if (start < 0) start = 0;
  if (stop < 0) stop = 0;
  if (start >= size) start = size;
  if (stop >= size) stop = size - 1;
  if (start > stop) {
    return std::vector<std::string>{};
  }

  auto const rev_start = size - 1 - stop;
  auto const rev_stop  = size - 1 - start;

  std::vector<std::string> result;
  result.reserve(static_cast<std::size_t>(rev_stop - rev_start + 1));
  for (auto i = rev_stop; i >= rev_start; --i) {
    result.emplace_back(std::move(all[static_cast<std::size_t>(i)]));
  }
  return result;
}

auto EmbeddedRedis::zrevrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {
    return impl_->zrevrange(txn, key, start, stop);
  });
}

/**
 * @brief 最小スコアのメンバーを削除して取得する
 *
 * @copydoc EmbeddedRedis::zpopmin
 */
auto EmbeddedRedis::zpopmin(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }
    if (meta->size == 0) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(range_pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    it->Seek(pfx_slice);
    if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const k = it->key();
    auto const score_offset = suffix_offset(key);
    auto const member = std::string(k.data() + score_offset + 8, k.size() - score_offset - 8);

    // Get score from member key
    auto const mk = encode_zset_member_key(key, meta->version, member);
    std::string score_raw;
    if (!txn.Get(Impl::snap_opts(txn), mk, &score_raw).ok() || score_raw.size() != 8) {
      return std::unexpected(ErrorCode::NotFound);
    }
    auto const score = decode_score(reinterpret_cast<uint8_t const*>(score_raw.data()));

    std::ignore = txn.Delete(mk);
    std::ignore = txn.Delete(encode_zset_score_key(key, meta->version, score, member));
    meta->size--;
    if (meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(key));
    } else {
      impl_->put_meta(txn, key, *meta);
    }

    return member;
  });
}

/**
 * @brief 最大スコアのメンバーを削除して取得する
 *
 * @copydoc EmbeddedRedis::zpopmax
 */
auto EmbeddedRedis::zpopmax(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return std::unexpected(ErrorCode::NotFound);
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }
    if (meta->size == 0) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(range_pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    // Seek to last entry in the sorted set
    it->SeekForPrev(encode_zset_score_seek_key(key, meta->version, std::numeric_limits<double>::max()));
    // Walk backward to find the last entry within our prefix
    while (it->Valid() && !it->key().starts_with(pfx_slice)) {
      it->Prev();
    }
    if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
      return std::unexpected(ErrorCode::NotFound);
    }

    auto const k = it->key();
    auto const score_offset = suffix_offset(key);
    auto const member = std::string(k.data() + score_offset + 8, k.size() - score_offset - 8);

    auto const mk = encode_zset_member_key(key, meta->version, member);

    std::ignore = txn.Delete(mk);
    std::ignore = txn.Delete(k);
    meta->size--;
    if (meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(key));
    } else {
      impl_->put_meta(txn, key, *meta);
    }

    return member;
  });
}

/**
 * @brief 複数メンバーのスコアを取得する
 *
 * @copydoc EmbeddedRedis::zmscore
 */
auto EmbeddedRedis::zmscore(std::string_view key, std::vector<std::string_view> const& members) -> Result<std::vector<std::optional<double>>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::optional<double>>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::vector<std::optional<double>>(members.size(), std::nullopt);
    }
    if (meta->type != DataType::ZSet) {
      return std::unexpected(ErrorCode::WrongType);
    }

    std::vector<std::optional<double>> result;
    result.reserve(members.size());
    for (auto const& member : members) {
      std::string raw;
      if (txn.Get(Impl::snap_opts(txn), encode_zset_member_key(key, meta->version, member), &raw).ok() && raw.size() == 8) {
        result.emplace_back(decode_score(reinterpret_cast<uint8_t const*>(raw.data())));
      } else {
        result.emplace_back(std::nullopt);
      }
    }
    return result;
  });
}

// ---- New ZSet Functions (Medium) ----

/**
 * @brief 辞書式順序でメンバー範囲を取得する
 *
 * @copydoc EmbeddedRedis::zrangebylex
 */
auto EmbeddedRedis::zrangebylex(std::string_view key, std::string_view min, std::string_view max) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) return std::vector<std::string>{};
    if (meta->type != DataType::ZSet) return std::unexpected(ErrorCode::WrongType);

    auto const pfx = encode_zset_member_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);
    auto const member_offset = suffix_offset(key);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::vector<std::string> result;
    bool const no_min = (min == "-");
    bool const no_max = (max == "+");

    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      auto const member = std::string_view(k.data() + member_offset, k.size() - member_offset);
      if (!no_min && member < min) continue;
      if (!no_max && member > max) break;
      result.emplace_back(member);
    }
    return result;
  });
}

/**
 * @brief 辞書式順序で範囲内のメンバー数をカウントする
 *
 * @copydoc EmbeddedRedis::zlexcount
 */
auto EmbeddedRedis::zlexcount(std::string_view key, std::string_view min, std::string_view max) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) return 0;
    if (meta->type != DataType::ZSet) return std::unexpected(ErrorCode::WrongType);

    auto const pfx = encode_zset_member_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);
    auto const member_offset = suffix_offset(key);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    uint64_t count = 0;
    bool const no_min = (min == "-");
    bool const no_max = (max == "+");

    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      auto const member = std::string_view(k.data() + member_offset, k.size() - member_offset);
      if (!no_min && member < min) continue;
      if (!no_max && member > max) break;
      count++;
    }
    return count;
  });
}

/**
 * @brief ランク範囲でメンバーを削除する
 *
 * @copydoc EmbeddedRedis::zremrangebyrank
 */
/** @brief zremrangebyrank の実体（引数を書き換えるため、再試行ごとに新しいコピーで実行する） */
auto EmbeddedRedis::Impl::zremrangebyrank(rocksdb::Transaction& txn, std::string_view key, int64_t start, int64_t stop) -> Result<uint64_t> {
  auto meta = get_meta(txn, key);
  if (!meta) return 0;
  if (meta->type != DataType::ZSet) return std::unexpected(ErrorCode::WrongType);
  if (meta->size == 0) return 0;

  auto size = static_cast<int64_t>(meta->size);
  if (start < 0) start += size;
  if (stop < 0) stop += size;
  if (start < 0) start = 0;
  if (stop >= size) stop = size - 1;
  if (start > stop) return 0;

  auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(range_pfx);
  auto const score_offset = suffix_offset(key);

  std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(snap_iter_opts(txn)));

  std::vector<std::string> to_delete;
  std::vector<double> scores;
  int64_t pos = 0;

  for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) break;
    if (static_cast<std::size_t>(k.size()) < score_offset + 8) break;
    if (pos >= start && pos <= stop) {
      auto const member = std::string(k.data() + score_offset + 8, k.size() - score_offset - 8);
      to_delete.push_back(std::move(member));
      scores.push_back(decode_score(reinterpret_cast<uint8_t const*>(k.data() + score_offset)));
    }
    pos++;
    if (pos > stop) break;
  }

  if (to_delete.empty()) return 0;

  for (std::size_t i = 0; i < to_delete.size(); ++i) {
    std::ignore = txn.Delete(encode_zset_member_key(key, meta->version, to_delete[i]));
    std::ignore = txn.Delete(encode_zset_score_key(key, meta->version, scores[i], to_delete[i]));
  }
  meta->size -= to_delete.size();
  if (meta->size == 0) {
    std::ignore = txn.Delete(encode_meta_key(key));
  } else {
    put_meta(txn, key, *meta);
  }

  return static_cast<uint64_t>(to_delete.size());
}

auto EmbeddedRedis::zremrangebyrank(std::string_view key, int64_t start, int64_t stop) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    return impl_->zremrangebyrank(txn, key, start, stop);
  });
}

/**
 * @brief スコア範囲でメンバーを削除する
 *
 * @copydoc EmbeddedRedis::zremrangebyscore
 */
auto EmbeddedRedis::zremrangebyscore(std::string_view key, double min, double max) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) return 0;
    if (meta->type != DataType::ZSet) return std::unexpected(ErrorCode::WrongType);
    if (meta->size == 0) return 0;

    auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
    auto const seek_key  = encode_zset_score_seek_key(key, meta->version, min);
    rocksdb::Slice const pfx_slice(range_pfx);
    auto const score_offset = suffix_offset(key);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::vector<std::string> to_delete;
    std::vector<double> scores;

    for (it->Seek(seek_key); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      if (static_cast<std::size_t>(k.size()) < score_offset + 8) break;
      auto const sc = decode_score(reinterpret_cast<uint8_t const*>(k.data() + score_offset));
      if (sc > max) break;
      to_delete.push_back(std::string(k.data() + score_offset + 8, k.size() - score_offset - 8));
      scores.push_back(sc);
    }

    if (to_delete.empty()) return 0;

    for (std::size_t i = 0; i < to_delete.size(); ++i) {
      std::ignore = txn.Delete(encode_zset_member_key(key, meta->version, to_delete[i]));
      std::ignore = txn.Delete(encode_zset_score_key(key, meta->version, scores[i], to_delete[i]));
    }
    meta->size -= to_delete.size();
    if (meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(key));
    } else {
      impl_->put_meta(txn, key, *meta);
    }

    return static_cast<uint64_t>(to_delete.size());
  });
}

// ============================================================
// Streams
// ============================================================

/**
 * @brief ストリームにエントリを追加する
 *
 * @copydoc EmbeddedRedis::xadd
 */
/** @brief xadd の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::xadd(rocksdb::Transaction& txn, std::string_view key, std::string_view id, std::vector<std::pair<std::string, std::string>> const& fields) -> Result<std::string> {
  auto const existing = get_meta(txn, key);
  if (existing && existing->type != DataType::Stream) {
    return std::unexpected(ErrorCode::WrongType);
  }

  MetaValue meta;
  if (existing) {
    meta = *existing;
  } else {
    meta.type = DataType::Stream;
  }

  uint64_t ms  = 0;
  uint64_t seq = 0;

  if (id == "*") {
    // 自動 ID 生成: 現在時刻。同一ミリ秒内ならシーケンスをインクリメント
    ms = now_ms();
    if (ms > meta.last_ms) {
      seq = 0;
    } else {
      ms  = meta.last_ms;
      seq = meta.last_seq + 1;
    }
  } else {
    // "ms-seq" 書式のパース
    auto const dash = id.find('-');
    if (dash == std::string_view::npos) {
      return std::unexpected(ErrorCode::InvalidArgument);
    }
    auto const ms_sv  = id.substr(0, dash);
    auto const seq_sv = id.substr(dash + 1);
    auto       r1     = std::from_chars(ms_sv.data(), ms_sv.data() + ms_sv.size(), ms);
    auto       r2     = std::from_chars(seq_sv.data(), seq_sv.data() + seq_sv.size(), seq);
    if (r1.ec != std::errc{} || r2.ec != std::errc{}) {
      return std::unexpected(ErrorCode::InvalidArgument);
    }
    // 単調増加チェック: 以前の ID より大きくなければならない
    if (ms < meta.last_ms || (ms == meta.last_ms && seq <= meta.last_seq)) {
      return std::unexpected(ErrorCode::InvalidArgument);
    }
  }

  // フィールドのシリアライズ: [count(4BE)] + { [klen(4BE)][key][vlen(4BE)][val] }
  auto const  fc   = static_cast<uint32_t>(fields.size());
  std::string data;
  data.resize(4);
  write_u32be(reinterpret_cast<uint8_t*>(data.data()), fc);

  for (auto const& [fk, fv] : fields) {
    auto const klen = static_cast<uint32_t>(fk.size());
    auto const vlen = static_cast<uint32_t>(fv.size());
    data.resize(data.size() + 4);
    write_u32be(reinterpret_cast<uint8_t*>(data.data() + data.size() - 4), klen);
    data.append(fk);
    data.resize(data.size() + 4);
    write_u32be(reinterpret_cast<uint8_t*>(data.data() + data.size() - 4), vlen);
    data.append(fv);
  }

  meta.last_ms  = ms;
  meta.last_seq = seq;
  meta.size++;

  std::ignore = txn.Put(encode_stream_key(key, meta.version, ms, seq), data);
  put_meta(txn, key, meta);

  return std::format("{}-{}", ms, seq);
}

auto EmbeddedRedis::xadd(std::string_view key, std::string_view id, std::vector<std::pair<std::string, std::string>> const& fields) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    return impl_->xadd(txn, key, id, fields);
  });
}

// ---- New Stream Functions ----

/**
 * @brief ストリームの長さを取得する
 *
 * @copydoc EmbeddedRedis::xlen
 */
auto EmbeddedRedis::xlen(std::string_view key) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::Stream) {
      return std::unexpected(ErrorCode::WrongType);
    }
    return meta->size;
  });
}

/**
 * @brief ストリームからエントリを削除する
 *
 * @copydoc EmbeddedRedis::xdel
 */
auto EmbeddedRedis::xdel(std::string_view key, std::vector<std::string_view> const& ids) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return 0;
    }
    if (meta->type != DataType::Stream) {
      return std::unexpected(ErrorCode::WrongType);
    }

    uint64_t removed = 0;

    for (auto const& id : ids) {
      auto const dash = id.find('-');
      if (dash == std::string_view::npos) continue;
      uint64_t ms = 0, seq = 0;
      auto r1 = std::from_chars(id.data(), id.data() + dash, ms);
      auto r2 = std::from_chars(id.data() + dash + 1, id.data() + id.size(), seq);
      if (r1.ec != std::errc{} || r2.ec != std::errc{}) continue;

      auto const sk = encode_stream_key(key, meta->version, ms, seq);
      std::string exist;
      if (txn.Get(Impl::snap_opts(txn), sk, &exist).ok()) {
        std::ignore = txn.Delete(sk);
        removed++;
      }
    }

    if (removed == 0) {
      return 0;
    }

    // guard against inconsistent counts
    if (removed > meta->size) removed = meta->size;
    meta->size -= removed;
    if (meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(key));
    } else {
      impl_->put_meta(txn, key, *meta);
    }

    return removed;
  });
}

// ---- New Stream Functions (Medium) ----

namespace {

bool parse_stream_id(std::string_view id, uint64_t& ms, uint64_t& seq) {
  if (id == "-" || id == "+") return false;
  auto const dash = id.find('-');
  if (dash == std::string_view::npos) return false;
  auto r1 = std::from_chars(id.data(), id.data() + dash, ms);
  auto r2 = std::from_chars(id.data() + dash + 1, id.data() + id.size(), seq);
  return r1.ec == std::errc{} && r2.ec == std::errc{};
}

int compare_stream_id(uint64_t ms1, uint64_t seq1, uint64_t ms2, uint64_t seq2) {
  if (ms1 < ms2) return -1;
  if (ms1 > ms2) return 1;
  if (seq1 < seq2) return -1;
  if (seq1 > seq2) return 1;
  return 0;
}

} // anonymous namespace

/**
 * @brief ストリームのエントリ範囲を取得する（昇順）
 *
 * @copydoc EmbeddedRedis::xrange
 */
auto EmbeddedRedis::xrange(std::string_view key, std::string_view start, std::string_view end) -> Result<std::vector<StreamEntry>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<StreamEntry>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) return std::vector<StreamEntry>{};
    if (meta->type != DataType::Stream) return std::unexpected(ErrorCode::WrongType);
    if (meta->size == 0) return std::vector<StreamEntry>{};

    uint64_t start_ms = 0, start_seq = 0;
    uint64_t end_ms = 0, end_seq = 0;
    bool const has_start = parse_stream_id(start, start_ms, start_seq);
    bool const has_end = parse_stream_id(end, end_ms, end_seq);

    auto const pfx = encode_stream_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);
    auto const ms_offset = suffix_offset(key);
    auto const seq_offset = ms_offset + 8;

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    // Seek to start position
    if (has_start) {
      it->Seek(encode_stream_key(key, meta->version, start_ms, start_seq));
    } else {
      it->Seek(pfx_slice);
    }

    std::vector<StreamEntry> result;
    for (; it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      if (static_cast<std::size_t>(k.size()) < seq_offset) break;

      auto const cur_ms  = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + ms_offset));
      auto const cur_seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));

      if (has_end && compare_stream_id(cur_ms, cur_seq, end_ms, end_seq) > 0) break;
      if (has_start && compare_stream_id(cur_ms, cur_seq, start_ms, start_seq) < 0) continue;

      // decode entry data
      auto const v = it->value();
      auto const* data = reinterpret_cast<uint8_t const*>(v.data());
      auto const field_count = read_u32be(data);
      std::size_t offset = 4;

      StreamEntry entry;
      entry.id = std::format("{}-{}", cur_ms, cur_seq);
      for (uint32_t i = 0; i < field_count; ++i) {
        if (offset + 4 > static_cast<std::size_t>(v.size())) break;
        auto const klen = read_u32be(data + offset); offset += 4;
        if (offset + klen > static_cast<std::size_t>(v.size())) break;
        auto fk = std::string(v.data() + offset, klen);
        offset += klen;
        if (offset + 4 > static_cast<std::size_t>(v.size())) break;
        auto const vlen = read_u32be(data + offset); offset += 4;
        if (offset + vlen > static_cast<std::size_t>(v.size())) break;
        auto fv = std::string(v.data() + offset, vlen);
        offset += vlen;
        entry.fields.emplace_back(std::move(fk), std::move(fv));
      }
      result.push_back(std::move(entry));
    }
    return result;
  });
}

/**
 * @brief ストリームのエントリ範囲を取得する（降順）
 *
 * @copydoc EmbeddedRedis::xrevrange
 */
auto EmbeddedRedis::xrevrange(std::string_view key, std::string_view start, std::string_view end) -> Result<std::vector<StreamEntry>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<StreamEntry>> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) return std::vector<StreamEntry>{};
    if (meta->type != DataType::Stream) return std::unexpected(ErrorCode::WrongType);
    if (meta->size == 0) return std::vector<StreamEntry>{};

    uint64_t start_ms = 0, start_seq = 0;
    uint64_t end_ms = 0, end_seq = 0;
    bool const has_start = parse_stream_id(start, start_ms, start_seq);
    bool const has_end = parse_stream_id(end, end_ms, end_seq);

    auto const pfx = encode_stream_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);
    auto const ms_offset = suffix_offset(key);
    auto const seq_offset = ms_offset + 8;

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    // Seek to end (or max)
    if (has_start) {
      it->Seek(encode_stream_key(key, meta->version, start_ms, start_seq));
      // Go past the first matching entry so we start from the one before
      it->SeekForPrev(encode_stream_key(key, meta->version, start_ms, start_seq));
    } else {
      it->SeekForPrev(encode_stream_key(key, meta->version, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()));
    }
    // Walk back to prefix
    while (it->Valid() && !it->key().starts_with(pfx_slice)) {
      it->Prev();
    }
    if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
      return std::vector<StreamEntry>{};
    }

    std::vector<StreamEntry> result;
    for (; it->Valid(); it->Prev()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      if (static_cast<std::size_t>(k.size()) < seq_offset) continue;

      auto const cur_ms  = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + ms_offset));
      auto const cur_seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));

      if (has_end && compare_stream_id(cur_ms, cur_seq, end_ms, end_seq) < 0) break;
      if (has_start && compare_stream_id(cur_ms, cur_seq, start_ms, start_seq) > 0) continue;

      auto const v = it->value();
      auto const* data = reinterpret_cast<uint8_t const*>(v.data());
      auto const field_count = read_u32be(data);
      std::size_t offset = 4;

      StreamEntry entry;
      entry.id = std::format("{}-{}", cur_ms, cur_seq);
      for (uint32_t i = 0; i < field_count; ++i) {
        if (offset + 4 > static_cast<std::size_t>(v.size())) break;
        auto const klen = read_u32be(data + offset); offset += 4;
        if (offset + klen > static_cast<std::size_t>(v.size())) break;
        auto fk = std::string(v.data() + offset, klen);
        offset += klen;
        if (offset + 4 > static_cast<std::size_t>(v.size())) break;
        auto const vlen = read_u32be(data + offset); offset += 4;
        if (offset + vlen > static_cast<std::size_t>(v.size())) break;
        auto fv = std::string(v.data() + offset, vlen);
        offset += vlen;
        entry.fields.emplace_back(std::move(fk), std::move(fv));
      }
      result.push_back(std::move(entry));
    }
    return result;
  });
}

/**
 * @brief ストリームを最大長でトリムする
 *
 * @copydoc EmbeddedRedis::xtrim
 */
auto EmbeddedRedis::xtrim(std::string_view key, uint64_t maxlen) -> Result<uint64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<uint64_t> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) return 0;
    if (meta->type != DataType::Stream) return std::unexpected(ErrorCode::WrongType);
    if (meta->size <= maxlen) return 0;

    auto const pfx = encode_stream_prefix(key, meta->version);
    rocksdb::Slice const pfx_slice(pfx);

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    uint64_t to_remove = meta->size - maxlen;

    uint64_t removed = 0;
    for (it->Seek(pfx_slice); it->Valid() && removed < to_remove; it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      std::ignore = txn.Delete(k);
      removed++;
    }

    if (removed == 0) return 0;

    meta->size -= removed;
    if (meta->size == 0) {
      std::ignore = txn.Delete(encode_meta_key(key));
    } else {
      impl_->put_meta(txn, key, *meta);
    }

    return removed;
  });
}

// ============================================================
// Generic
// ============================================================

/**
 * @brief キーとそのデータを削除する
 *
 * @copydoc EmbeddedRedis::del
 */
/** @brief del の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::del(rocksdb::Transaction& txn, std::string_view key) -> Result<bool> {
  auto const meta = get_meta(txn, key);
  if (!meta) {
    return false;
  }

  erase_key_data(txn, key, *meta);

  return true;
}

auto EmbeddedRedis::del(std::string_view key) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->del(txn, key);
  });
}

/** @brief キーの存在確認 @copydoc EmbeddedRedis::exists */
auto EmbeddedRedis::exists(std::string_view key) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->get_meta_ro(txn, key).has_value();
  });
}

/**
 * @brief キーのデータ型を返す
 *
 * @copydoc EmbeddedRedis::type
 */
auto EmbeddedRedis::type(std::string_view key) -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return std::string("none");
    }
    switch (meta->type) {
    case DataType::String: return std::string("string");
    case DataType::Hash:   return std::string("hash");
    case DataType::List:   return std::string("list");
    case DataType::Set:    return std::string("set");
    case DataType::ZSet:   return std::string("zset");
    case DataType::Stream: return std::string("stream");
    }
    return std::string("none");
  });
}

/**
 * @brief キーに有効期限を設定する（Unix タイムスタンプ・秒）
 *
 * @copydoc EmbeddedRedis::expireat
 */
auto EmbeddedRedis::expireat(std::string_view key, uint64_t unix_time) -> Result<bool> {
  return pexpireat(key, unix_time * 1000);
}

/**
 * @brief キーに有効期限を設定する（Unix タイムスタンプ・ミリ秒）
 *
 * @copydoc EmbeddedRedis::pexpireat
 */
auto EmbeddedRedis::pexpireat(std::string_view key, uint64_t unix_time) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    auto meta = impl_->get_meta(txn, key);
    if (!meta) {
      return false;
    }

    meta->expiration_ms = unix_time;

    impl_->put_meta(txn, key, *meta);

    return true;
  });
}

/**
 * @brief キーの最終アクセス時刻を更新する
 *
 * @copydoc EmbeddedRedis::touch
 */
auto EmbeddedRedis::touch(std::string_view key) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    auto meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return false;
    }
    // RocksDB ベースの組み込み DB では touch の効果は限定的だが、TTL 期限切れは検出する
    return true;
  });
}

// ============================================================
// Expiration
// ============================================================

/**
 * @brief キーに有効期限を設定する（秒）
 *
 * @copydoc EmbeddedRedis::expire
 */
auto EmbeddedRedis::expire(std::string_view key, uint64_t seconds) -> Result<bool> {
  return pexpire(key, seconds * 1000);
}

/**
 * @brief キーの残り有効期限を取得する（秒）
 *
 * @copydoc EmbeddedRedis::ttl
 */
auto EmbeddedRedis::ttl(std::string_view key) -> Result<int64_t> {
  auto const ms = pttl(key);
  if (!ms) {
    return ms;
  }
  if (*ms < 0) {
    return *ms;
  }
  return *ms / 1000;
}

/**
 * @brief キーに有効期限を設定する（ミリ秒）
 * @copydoc EmbeddedRedis::pexpire
 */
/** @brief pexpire の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::pexpire(rocksdb::Transaction& txn, std::string_view key, uint64_t milliseconds) -> Result<bool> {
  auto const meta = get_meta(txn, key);
  if (!meta) {
    return false;
  }

  auto updated = *meta;
  updated.expiration_ms = now_ms() + milliseconds;

  put_meta(txn, key, updated);

  return true;
}

auto EmbeddedRedis::pexpire(std::string_view key, uint64_t milliseconds) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->pexpire(txn, key, milliseconds);
  });
}

/**
 * @brief キーの残り有効期限を取得する（ミリ秒）
 *
 * @copydoc EmbeddedRedis::pttl
 */
auto EmbeddedRedis::pttl(std::string_view key) -> Result<int64_t> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<int64_t> {
    auto const meta = impl_->get_meta_ro(txn, key);
    if (!meta) {
      return -1;
    }
    if (meta->expiration_ms == 0) {
      return -1;
    }

    auto const now = now_ms();
    if (now >= meta->expiration_ms) {
      return -2;
    }
    return static_cast<int64_t>(meta->expiration_ms - now);
  });
}

/**
 * @brief キーの有効期限を削除する
 *
 * @copydoc EmbeddedRedis::persist
 */
/** @brief persist の実体（Pipeline と共有） */
auto EmbeddedRedis::Impl::persist(rocksdb::Transaction& txn, std::string_view key) -> Result<bool> {
  auto const meta = get_meta(txn, key);
  if (!meta) {
    return false;
  }
  if (meta->expiration_ms == 0) {
    return false;
  }

  auto updated = *meta;
  updated.expiration_ms = 0;

  put_meta(txn, key, updated);

  return true;
}

auto EmbeddedRedis::persist(std::string_view key) -> Result<bool> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<bool> {
    return impl_->persist(txn, key);
  });
}

// ---- New Generic Functions (Medium) ----

namespace {

bool glob_match(std::string_view pattern, std::string_view str) {
  std::size_t pi = 0, si = 0;
  std::size_t star_pi = std::string_view::npos, star_si = std::string_view::npos;

  while (si < str.size()) {
    if (pi < pattern.size() && pattern[pi] == '*') {
      star_pi = pi;
      star_si = si;
      pi++;
    } else if (pi < pattern.size() && (pattern[pi] == '?' || pattern[pi] == str[si])) {
      pi++;
      si++;
    } else if (pi < pattern.size() && pattern[pi] == '\\' && pi + 1 < pattern.size() && pattern[pi + 1] == str[si]) {
      pi += 2;
      si++;
    } else if (star_pi != std::string_view::npos) {
      pi = star_pi + 1;
      star_si++;
      si = star_si;
    } else {
      return false;
    }
  }
  while (pi < pattern.size() && pattern[pi] == '*') pi++;
  return pi == pattern.size();
}

} // anonymous namespace

/**
 * @brief ランダムなキーを取得する
 *
 * @copydoc EmbeddedRedis::randomkey
 */
auto EmbeddedRedis::randomkey() -> Result<std::string> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::string> {

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::string const meta_pfx(1, static_cast<char>(prefix::Meta));
    rocksdb::Slice const pfx_slice(meta_pfx);

    // Reservoir sampling: pick one random key
    std::string result;
    uint64_t count = 0;

    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      if (k.size() < 2) continue;
      count++;
      if (count == 1) {
        result.assign(k.data() + 1, k.size() - 1);
      } else if (random_below(count) == 0) {
        result.assign(k.data() + 1, k.size() - 1);
      }
    }
    if (count == 0) return std::unexpected(ErrorCode::NotFound);
    return result;
  });
}

/**
 * @brief パターンに一致するキーを検索する
 *
 * @copydoc EmbeddedRedis::keys
 */
auto EmbeddedRedis::keys(std::string_view pattern) -> Result<std::vector<std::string>> {
  return impl_->run_txn([&](rocksdb::Transaction& txn) -> Result<std::vector<std::string>> {

    std::unique_ptr<rocksdb::Iterator> it(txn.GetIterator(Impl::snap_iter_opts(txn)));

    std::string const meta_pfx(1, static_cast<char>(prefix::Meta));
    rocksdb::Slice const pfx_slice(meta_pfx);

    std::vector<std::string> result;
    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) break;
      if (k.size() < 2) continue;
      auto const key_str = std::string_view(k.data() + 1, k.size() - 1);
      if (glob_match(pattern, key_str)) {
        result.emplace_back(key_str);
      }
    }
    return result;
  });
}

// ============================================================
// Pipeline
// ============================================================

auto EmbeddedRedis::pipeline() -> Pipeline {
  return Pipeline(*this);
}

/**
 * @brief Pipeline を構築し、専用のトランザクションを開始する
 * @note DB が開けていない場合はトランザクションを持たず、以降の操作は何もしない。
 *   exec() は RocksDBError を返す（以前は null 参照でクラッシュしていた）。
 */
Pipeline<EmbeddedRedis>::Pipeline(EmbeddedRedis& db) : db_(db) {
  if (!db_.is_open()) {
    error_ = ErrorCode::RocksDBError;
    return;
  }
  rocksdb::OptimisticTransactionOptions txn_opts;
  txn_opts.set_snapshot = true;
  txn_.reset(db_.impl_->txn_db->BeginTransaction(db_.impl_->write_opts, txn_opts));
}

Pipeline<EmbeddedRedis>::~Pipeline() {
  if (txn_) {
    std::ignore = txn_->Rollback();
  }
}

Pipeline<EmbeddedRedis>::Pipeline(Pipeline&&) noexcept = default;

bool Pipeline<EmbeddedRedis>::usable() const noexcept {
  return txn_ != nullptr && !error_;
}

/**
 * @brief Impl の共有実装を呼び出し、失敗したらエラーを記録する
 * @details Pipeline 版と単発版でロジックが分岐しないよう、実体は必ず Impl 側を使う
 */
#define REDISMM_PIPELINE_OP(EXPR)                                                                                      \
  do {                                                                                                                 \
    if (!usable()) {                                                                                                   \
      return *this;                                                                                                    \
    }                                                                                                                  \
    if (auto const r = (EXPR); !r) {                                                                                   \
      set_error(r.error());                                                                                            \
    }                                                                                                                  \
    return *this;                                                                                                      \
  } while (false)

auto Pipeline<EmbeddedRedis>::set(std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->set(*txn_, key, value, ttl_ms));
}

auto Pipeline<EmbeddedRedis>::hset(std::string_view key, std::string_view field, std::string_view value) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->hset(*txn_, key, field, value));
}

auto Pipeline<EmbeddedRedis>::lpush(std::string_view key, std::string_view value) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->lpush(*txn_, key, value));
}

auto Pipeline<EmbeddedRedis>::rpush(std::string_view key, std::string_view value) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->rpush(*txn_, key, value));
}

auto Pipeline<EmbeddedRedis>::sadd(std::string_view key, std::string_view member) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->sadd(*txn_, key, member));
}

auto Pipeline<EmbeddedRedis>::srem(std::string_view key, std::string_view member) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->srem(*txn_, key, member));
}

auto Pipeline<EmbeddedRedis>::zadd(std::string_view key, double score, std::string_view member) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->zadd(*txn_, key, score, member));
}

auto Pipeline<EmbeddedRedis>::xadd(std::string_view key, std::string_view id,
                                   std::vector<std::pair<std::string, std::string>> const& fields) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->xadd(*txn_, key, id, fields));
}

auto Pipeline<EmbeddedRedis>::del(std::string_view key) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->del(*txn_, key));
}

auto Pipeline<EmbeddedRedis>::expire(std::string_view key, uint64_t seconds) -> Pipeline& {
  return pexpire(key, seconds * 1000);
}

auto Pipeline<EmbeddedRedis>::pexpire(std::string_view key, uint64_t milliseconds) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->pexpire(*txn_, key, milliseconds));
}

auto Pipeline<EmbeddedRedis>::persist(std::string_view key) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->persist(*txn_, key));
}

auto Pipeline<EmbeddedRedis>::append(std::string_view key, std::string_view value) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->append(*txn_, key, value));
}

auto Pipeline<EmbeddedRedis>::hdel(std::string_view key, std::string_view field) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->hdel(*txn_, key, field));
}

auto Pipeline<EmbeddedRedis>::zrem(std::string_view key, std::string_view member) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->zrem(*txn_, key, member));
}

auto Pipeline<EmbeddedRedis>::lrem(std::string_view key, int64_t count, std::string_view value) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->lrem(*txn_, key, count, value));
}

auto Pipeline<EmbeddedRedis>::ltrim(std::string_view key, int64_t start, int64_t stop) -> Pipeline& {
  REDISMM_PIPELINE_OP(db_.impl_->ltrim(*txn_, key, start, stop));
}

#undef REDISMM_PIPELINE_OP

auto Pipeline<EmbeddedRedis>::exec() -> Result<void> {
  if (error_) {
    auto const e = *error_;
    clear();
    return std::unexpected(e);
  }
  if (!txn_) {
    return std::unexpected(ErrorCode::RocksDBError);
  }

  auto const s = txn_->Commit();
  clear();
  if (s.ok()) {
    return {};
  }
  // 他スレッドと競合した場合、どの操作をやり直すかは利用者しか決められない
  if (s.IsBusy() || s.IsTryAgain()) {
    return std::unexpected(ErrorCode::Busy);
  }
  return std::unexpected(ErrorCode::RocksDBError);
}

void Pipeline<EmbeddedRedis>::clear() {
  error_.reset();
  if (!db_.is_open()) {
    error_ = ErrorCode::RocksDBError;
    txn_.reset();
    return;
  }
  rocksdb::OptimisticTransactionOptions txn_opts;
  txn_opts.set_snapshot = true;
  // 既存の Transaction オブジェクトを再利用する（内部状態はリセットされる）
  txn_.reset(db_.impl_->txn_db->BeginTransaction(db_.impl_->write_opts, txn_opts, txn_.release()));
}

} // namespace redismm
