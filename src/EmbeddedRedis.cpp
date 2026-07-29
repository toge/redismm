#include "redismm/EmbeddedRedis.hpp"
#include "redismm/Encoder.hpp"

#include <charconv>
#include <format>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

namespace redismm {

/** @brief Pimpl 実装：RocksDB のラッパーと内部操作 */
struct EmbeddedRedis::Impl {
  rocksdb::DB*          db         = nullptr; ///< RocksDB インスタンス
  rocksdb::WriteOptions write_opts = {};       ///< 書き込みオプション
  rocksdb::ReadOptions  read_opts  = {};       ///< 読み取りオプション

  ~Impl() {
    delete db;
  }

  /**
   * @brief キーのメタデータを取得する（期限切れなら遅延削除後 nullopt を返す）
   *
   * @param key ユーザーキー
   * @return メタデータ。キーが存在しないか期限切れなら nullopt
   */
  std::optional<MetaValue> get_meta(std::string_view key) {
    auto const mk = encode_meta_key(key);
    std::string raw;
    auto const s = db->Get(read_opts, mk, &raw);
    if (!s.ok()) {
      return std::nullopt;
    }
    auto meta = decode_meta_value(raw);
    if (!meta) {
      return std::nullopt;
    }
    if (is_expired(*meta)) {
      rocksdb::WriteBatch batch;
      erase_key_data(batch, key, *meta);
      std::ignore = db->Write(write_opts, &batch);
      return std::nullopt;
    }
    return meta;
  }

  /**
   * @brief メタデータを WriteBatch に書き込む
   *
   * @param batch 書き込みバッチ
   * @param key ユーザーキー
   * @param meta メタデータ
   */
  void put_meta(rocksdb::WriteBatch& batch, std::string_view key, MetaValue const& meta) {
    batch.Put(encode_meta_key(key), encode_meta_value(meta));
  }

  /**
   * @brief プレフィックスに合致する全キーを削除する（WriteBatch に積む）
   *
   * @param batch 書き込みバッチ
   * @param pfx プレフィックス
   */
  void delete_by_prefix(rocksdb::WriteBatch& batch, std::string const& pfx) {
    rocksdb::Slice const pfx_slice(pfx);
    rocksdb::ReadOptions opts;
    opts.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(opts));
    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      if (!it->key().starts_with(pfx_slice)) {
        break;
      }
      batch.Delete(it->key());
    }
  }

  /**
   * @brief キーに関連する全データ（メタ＋データ）を削除する
   *
   * @param batch 書き込みバッチ
   * @param key ユーザーキー
   * @param meta メタデータ
   */
  void erase_key_data(rocksdb::WriteBatch& batch, std::string_view key, MetaValue const& meta) {
    batch.Delete(encode_meta_key(key));
    switch (meta.type) {
    case DataType::String:
      batch.Delete(encode_string_key(key));
      break;
    case DataType::Hash:
      delete_by_prefix(batch, encode_hash_prefix(key, meta.version));
      break;
    case DataType::List:
      delete_by_prefix(batch, encode_list_prefix(key, meta.version));
      break;
    case DataType::Set:
      delete_by_prefix(batch, encode_set_prefix(key, meta.version));
      break;
    case DataType::ZSet:
      delete_by_prefix(batch, encode_zset_member_prefix(key, meta.version));
      delete_by_prefix(batch, encode_zset_score_range_prefix(key, meta.version));
      break;
    case DataType::Stream:
      delete_by_prefix(batch, encode_stream_prefix(key, meta.version));
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
  rocksdb::Options              opts;
  opts.create_if_missing = true;
  opts.compression       = rocksdb::kNoCompression;

  std::unique_ptr<rocksdb::DB> raw_db;
  auto const                   status = rocksdb::DB::Open(opts, std::string(db_path), &raw_db);
  if (!status.ok()) {
    std::cerr << std::format("[EmbeddedRedis] DB::Open failed: {}\n", status.ToString());
    return;
  }
  impl_->db = raw_db.release();
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
  return impl_ && impl_->db != nullptr;
}

// ============================================================
// Strings
// ============================================================

/**
 * @brief 文字列値を格納する
 *
 * @copydoc EmbeddedRedis::set
 */
auto EmbeddedRedis::set(std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms) -> Result<void> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }

  auto existing = impl_->get_meta(key);

  rocksdb::WriteBatch batch;

  // 既存キーが別の型なら先に全削除する
  if (existing && existing->type != DataType::String) {
    impl_->erase_key_data(batch, key, *existing);
    existing = std::nullopt;
  }

  MetaValue meta;
  meta.type          = DataType::String;
  meta.version       = 1;
  meta.size          = value.size();
  meta.expiration_ms = ttl_ms ? now_ms() + *ttl_ms : 0;

  impl_->put_meta(batch, key, meta);
  batch.Put(encode_string_key(key), rocksdb::Slice(value.data(), value.size()));

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return {};
}

/**
 * @brief 文字列値を取得する
 *
 * @copydoc EmbeddedRedis::get
 */
auto EmbeddedRedis::get(std::string_view key) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  std::string value;
  if (!impl_->db->Get(impl_->read_opts, encode_string_key(key), &value).ok()) {
    return std::unexpected(ErrorCode::NotFound);
  }
  return value;
}

// ============================================================
// Hashes
// ============================================================

/**
 * @brief ハッシュフィールドに値を設定する
 *
 * @copydoc EmbeddedRedis::hset
 */
auto EmbeddedRedis::hset(std::string_view key, std::string_view field, std::string_view value) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto existing = impl_->get_meta(key);
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
  bool const  is_new = !impl_->db->Get(impl_->read_opts, hk, &dummy).ok();
  if (is_new) {
    meta.size++;
  }

  rocksdb::WriteBatch batch;
  batch.Put(hk, rocksdb::Slice(value.data(), value.size()));
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return is_new;
}

/**
 * @brief ハッシュフィールド値を取得する
 *
 * @copydoc EmbeddedRedis::hget
 */
auto EmbeddedRedis::hget(std::string_view key, std::string_view field) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }

  std::string value;
  if (!impl_->db->Get(impl_->read_opts, encode_hash_key(key, meta->version, field), &value).ok()) {
    return std::unexpected(ErrorCode::NotFound);
  }
  return value;
}

/**
 * @brief ハッシュの全フィールドを取得する
 *
 * @copydoc EmbeddedRedis::hgetall
 */
auto EmbeddedRedis::hgetall(std::string_view key) -> Result<std::unordered_map<std::string, std::string>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const pfx = encode_hash_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
auto EmbeddedRedis::lpush(std::string_view key, std::string_view value) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const existing = impl_->get_meta(key);
  if (existing && existing->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto meta = init_list_meta(existing);
  meta.head_seq--;
  meta.size++;

  rocksdb::WriteBatch batch;
  batch.Put(encode_list_key(key, meta.version, meta.head_seq), rocksdb::Slice(value.data(), value.size()));
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return meta.size;
}

/**
 * @brief リスト末尾に要素を追加する
 *
 * @copydoc EmbeddedRedis::rpush
 */
auto EmbeddedRedis::rpush(std::string_view key, std::string_view value) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const existing = impl_->get_meta(key);
  if (existing && existing->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto meta = init_list_meta(existing);

  rocksdb::WriteBatch batch;
  batch.Put(encode_list_key(key, meta.version, meta.tail_seq), rocksdb::Slice(value.data(), value.size()));
  meta.tail_seq++;
  meta.size++;
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return meta.size;
}

/**
 * @brief リスト先頭要素を削除して取得する
 *
 * @copydoc EmbeddedRedis::lpop
 */
auto EmbeddedRedis::lpop(std::string_view key) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
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
  if (!impl_->db->Get(impl_->read_opts, lk, &value).ok()) {
    return std::unexpected(ErrorCode::NotFound);
  }

  rocksdb::WriteBatch batch;
  batch.Delete(lk);
  meta->head_seq++;
  meta->size--;

  // 空になったらメタごと削除、そうでなければ更新
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return value;
}

/**
 * @brief リスト末尾要素を削除して取得する
 *
 * @copydoc EmbeddedRedis::rpop
 */
auto EmbeddedRedis::rpop(std::string_view key) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
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
  if (!impl_->db->Get(impl_->read_opts, lk, &value).ok()) {
    return std::unexpected(ErrorCode::NotFound);
  }

  rocksdb::WriteBatch batch;
  batch.Delete(lk);
  meta->size--;

  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return value;
}

// ============================================================
// Sets
// ============================================================

/**
 * @brief セットにメンバーを追加する
 *
 * @copydoc EmbeddedRedis::sadd
 */
auto EmbeddedRedis::sadd(std::string_view key, std::string_view member) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const existing = impl_->get_meta(key);
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
  bool const  is_new = !impl_->db->Get(impl_->read_opts, sk, &dummy).ok();
  if (is_new) {
    meta.size++;
  }

  rocksdb::WriteBatch batch;
  batch.Put(sk, rocksdb::Slice());
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return is_new;
}

/**
 * @brief セットの全メンバーを取得する
 *
 * @copydoc EmbeddedRedis::smembers
 */
auto EmbeddedRedis::smembers(std::string_view key) -> Result<std::vector<std::string>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::Set) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const pfx = encode_set_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
}

// ============================================================
// Sorted Sets
// ============================================================

/**
 * @brief ソート済みセットにスコア付きメンバーを追加する
 *
 * @copydoc EmbeddedRedis::zadd
 */
auto EmbeddedRedis::zadd(std::string_view key, double score, std::string_view member) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const existing = impl_->get_meta(key);
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
  bool const  is_new = !impl_->db->Get(impl_->read_opts, mk, &old_score_raw).ok();

  rocksdb::WriteBatch batch;

  if (!is_new && old_score_raw.size() == 8) {
    auto const old_score = decode_score(reinterpret_cast<uint8_t const*>(old_score_raw.data()));
    batch.Delete(encode_zset_score_key(key, meta.version, old_score, member));
  }
  if (is_new) {
    meta.size++;
  }

  // member key → score(8byte encoded)
  auto const sb = encode_score(score);
  batch.Put(mk, rocksdb::Slice(reinterpret_cast<char const*>(sb.data()), 8));

  // score key → empty
  batch.Put(encode_zset_score_key(key, meta.version, score, member), rocksdb::Slice());
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return is_new;
}

/**
 * @brief スコア範囲でメンバーを取得する
 *
 * @copydoc EmbeddedRedis::zrangebyscore
 */
auto EmbeddedRedis::zrangebyscore(std::string_view key, double min_score, double max_score) -> Result<std::vector<std::string>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const range_pfx  = encode_zset_score_range_prefix(key, meta->version);
  auto const seek_key   = encode_zset_score_seek_key(key, meta->version, min_score);
  rocksdb::Slice const pfx_slice(range_pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  std::vector<std::string> result;
  auto const               score_offset = 1 + key.size() + 8; // prefix(1) + key + version(8)

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
}

// ============================================================
// Streams
// ============================================================

/**
 * @brief ストリームにエントリを追加する
 *
 * @copydoc EmbeddedRedis::xadd
 */
auto EmbeddedRedis::xadd(std::string_view key, std::string_view id, std::vector<std::pair<std::string, std::string>> const& fields) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const existing = impl_->get_meta(key);
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

  rocksdb::WriteBatch batch;
  batch.Put(encode_stream_key(key, meta.version, ms, seq), data);
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return std::format("{}-{}", ms, seq);
}

// ============================================================
// Generic
// ============================================================

/**
 * @brief キーとそのデータを削除する
 *
 * @copydoc EmbeddedRedis::del
 */
auto EmbeddedRedis::del(std::string_view key) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }

  rocksdb::WriteBatch batch;
  impl_->erase_key_data(batch, key, *meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

/** @brief キーの存在確認 @copydoc EmbeddedRedis::exists */
auto EmbeddedRedis::exists(std::string_view key) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return impl_->get_meta(key).has_value();
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
auto EmbeddedRedis::pexpire(std::string_view key, uint64_t milliseconds) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }

  auto updated = *meta;
  updated.expiration_ms = now_ms() + milliseconds;

  rocksdb::WriteBatch batch;
  impl_->put_meta(batch, key, updated);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

/**
 * @brief キーの残り有効期限を取得する（ミリ秒）
 *
 * @copydoc EmbeddedRedis::pttl
 */
auto EmbeddedRedis::pttl(std::string_view key) -> Result<int64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
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
}

/**
 * @brief キーの有効期限を削除する
 *
 * @copydoc EmbeddedRedis::persist
 */
auto EmbeddedRedis::persist(std::string_view key) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }
  if (meta->expiration_ms == 0) {
    return false;
  }

  auto updated = *meta;
  updated.expiration_ms = 0;

  rocksdb::WriteBatch batch;
  impl_->put_meta(batch, key, updated);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

} // namespace redismm
