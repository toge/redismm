#include "redismm/EmbeddedRedis.hpp"
#include "redismm/Encoder.hpp"

#include <algorithm>
#include <charconv>
#include <cstdlib>
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

/**
 * @brief 文字列値を末尾に追加する
 *
 * @copydoc EmbeddedRedis::append
 */
auto EmbeddedRedis::append(std::string_view key, std::string_view value) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto existing = impl_->get_meta(key);
  if (existing && existing->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  std::string current;
  if (existing) {
    impl_->db->Get(impl_->read_opts, encode_string_key(key), &current);
  }

  current += value;

  rocksdb::WriteBatch batch;
  MetaValue meta;
  meta.type    = DataType::String;
  meta.version = existing ? existing->version : 1;
  meta.size    = current.size();

  impl_->put_meta(batch, key, meta);
  batch.Put(encode_string_key(key), rocksdb::Slice(current));

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return static_cast<uint64_t>(current.size());
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
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto existing = impl_->get_meta(key);
  if (existing && existing->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  int64_t val = 0;
  if (existing) {
    std::string raw;
    if (impl_->db->Get(impl_->read_opts, encode_string_key(key), &raw).ok()) {
      auto [ptr, ec] = std::from_chars(raw.data(), raw.data() + raw.size(), val);
      if (ec != std::errc{}) {
        return std::unexpected(ErrorCode::WrongType);
      }
    }
  }

  val += delta;
  auto const new_str = std::to_string(val);

  rocksdb::WriteBatch batch;
  MetaValue meta;
  meta.type    = DataType::String;
  meta.version = existing ? existing->version : 1;
  meta.size    = new_str.size();

  impl_->put_meta(batch, key, meta);
  batch.Put(encode_string_key(key), new_str);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return val;
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
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }
  return meta->size;
}

// ---- New String Functions ----

/**
 * @brief 値を設定し、古い値を返す
 *
 * @copydoc EmbeddedRedis::getset
 */
auto EmbeddedRedis::getset(std::string_view key, std::string_view value) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto existing = impl_->get_meta(key);
  if (!existing) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (existing->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  std::string old;
  impl_->db->Get(impl_->read_opts, encode_string_key(key), &old);

  rocksdb::WriteBatch batch;
  MetaValue meta = *existing;
  meta.size = value.size();
  impl_->put_meta(batch, key, meta);
  batch.Put(encode_string_key(key), rocksdb::Slice(value.data(), value.size()));

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return old;
}

/**
 * @brief キーが存在しない場合のみ設定する
 *
 * @copydoc EmbeddedRedis::setnx
 */
auto EmbeddedRedis::setnx(std::string_view key, std::string_view value) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto existing = impl_->get_meta(key);
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

  rocksdb::WriteBatch batch;
  impl_->put_meta(batch, key, meta);
  batch.Put(encode_string_key(key), rocksdb::Slice(value.data(), value.size()));

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

/**
 * @brief 文字列値を浮動小数点数でインクリメントする
 *
 * @copydoc EmbeddedRedis::incrbyfloat
 */
auto EmbeddedRedis::incrbyfloat(std::string_view key, double delta) -> Result<double> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto existing = impl_->get_meta(key);
  if (existing && existing->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  double val = 0.0;
  if (existing) {
    std::string raw;
    if (impl_->db->Get(impl_->read_opts, encode_string_key(key), &raw).ok()) {
      auto [ptr, ec] = std::from_chars(raw.data(), raw.data() + raw.size(), val);
      if (ec != std::errc{}) {
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

  rocksdb::WriteBatch batch;
  MetaValue meta;
  meta.type    = DataType::String;
  meta.version = existing ? existing->version : 1;
  meta.size    = new_str.size();
  impl_->put_meta(batch, key, meta);
  batch.Put(encode_string_key(key), new_str);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return val;
}

/**
 * @brief 文字列値の部分範囲を取得する
 *
 * @copydoc EmbeddedRedis::getrange
 */
auto EmbeddedRedis::getrange(std::string_view key, int64_t start, int64_t end) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::string{};
  }
  if (meta->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  std::string raw;
  if (!impl_->db->Get(impl_->read_opts, encode_string_key(key), &raw).ok()) {
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

/**
 * @brief 文字列値の指定位置から上書きする
 *
 * @copydoc EmbeddedRedis::setrange
 */
auto EmbeddedRedis::setrange(std::string_view key, uint64_t offset, std::string_view value) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto existing = impl_->get_meta(key);
  if (existing && existing->type != DataType::String) {
    return std::unexpected(ErrorCode::WrongType);
  }

  std::string current;
  if (existing) {
    impl_->db->Get(impl_->read_opts, encode_string_key(key), &current);
  }

  auto const needed = offset + value.size();
  if (current.size() < needed) {
    current.resize(needed, '\0');
  }
  current.replace(offset, value.size(), value);

  rocksdb::WriteBatch batch;
  MetaValue meta;
  meta.type    = DataType::String;
  meta.version = existing ? existing->version : 1;
  meta.size    = current.size();
  impl_->put_meta(batch, key, meta);
  batch.Put(encode_string_key(key), rocksdb::Slice(current));

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return static_cast<uint64_t>(current.size());
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

/**
 * @brief ハッシュのフィールドを削除する
 *
 * @copydoc EmbeddedRedis::hdel
 */
auto EmbeddedRedis::hdel(std::string_view key, std::string_view field) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const hk = encode_hash_key(key, meta->version, field);
  std::string exist;
  if (!impl_->db->Get(impl_->read_opts, hk, &exist).ok()) {
    return false;
  }

  rocksdb::WriteBatch batch;
  batch.Delete(hk);
  meta->size--;
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

/**
 * @brief ハッシュのフィールド存在確認
 *
 * @copydoc EmbeddedRedis::hexists
 */
auto EmbeddedRedis::hexists(std::string_view key, std::string_view field) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }
  std::string v;
  return impl_->db->Get(impl_->read_opts, encode_hash_key(key, meta->version, field), &v).ok();
}

/**
 * @brief ハッシュのフィールド数を取得する
 *
 * @copydoc EmbeddedRedis::hlen
 */
auto EmbeddedRedis::hlen(std::string_view key) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }
  return meta->size;
}

/**
 * @brief ハッシュの全フィールド名を取得する
 *
 * @copydoc EmbeddedRedis::hkeys
 */
auto EmbeddedRedis::hkeys(std::string_view key) -> Result<std::vector<std::string>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::vector<std::string>{};
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const pfx = encode_hash_prefix(key, meta->version);
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

/**
 * @brief ハッシュの全フィールド値を取得する
 *
 * @copydoc EmbeddedRedis::hvals
 */
auto EmbeddedRedis::hvals(std::string_view key) -> Result<std::vector<std::string>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::vector<std::string>{};
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const pfx = encode_hash_prefix(key, meta->version);
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
    result.emplace_back(it->value().ToString());
  }
  return result;
}

// ---- New Hash Functions ----

/**
 * @brief フィールドが存在しない場合のみ設定する
 *
 * @copydoc EmbeddedRedis::hsetnx
 */
auto EmbeddedRedis::hsetnx(std::string_view key, std::string_view field, std::string_view value) -> Result<bool> {
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
  std::string dummy;
  if (impl_->db->Get(impl_->read_opts, hk, &dummy).ok()) {
    return false;
  }

  if (!existing) {
    meta.size = 1;
  } else {
    meta.size++;
  }

  rocksdb::WriteBatch batch;
  batch.Put(hk, rocksdb::Slice(value.data(), value.size()));
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

/**
 * @brief ハッシュフィールドの値を整数でインクリメントする
 *
 * @copydoc EmbeddedRedis::hincrby
 */
auto EmbeddedRedis::hincrby(std::string_view key, std::string_view field, int64_t delta) -> Result<int64_t> {
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
  std::string raw;
  int64_t val = 0;
  bool const found = impl_->db->Get(impl_->read_opts, hk, &raw).ok();
  if (found) {
    auto [ptr, ec] = std::from_chars(raw.data(), raw.data() + raw.size(), val);
    if (ec != std::errc{}) {
      return std::unexpected(ErrorCode::WrongType);
    }
  } else {
    meta.size++;
  }

  val += delta;
  auto const new_str = std::to_string(val);

  rocksdb::WriteBatch batch;
  batch.Put(hk, new_str);
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return val;
}

/**
 * @brief 複数フィールドの値を取得する
 *
 * @copydoc EmbeddedRedis::hmget
 */
auto EmbeddedRedis::hmget(std::string_view key, std::vector<std::string_view> const& fields) -> Result<std::vector<std::optional<std::string>>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
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
    if (impl_->db->Get(impl_->read_opts, encode_hash_key(key, meta->version, field), &raw).ok()) {
      result.emplace_back(std::move(raw));
    } else {
      result.emplace_back(std::nullopt);
    }
  }
  return result;
}

/**
 * @brief ハッシュフィールド値の長さを取得する
 *
 * @copydoc EmbeddedRedis::hstrlen
 */
auto EmbeddedRedis::hstrlen(std::string_view key, std::string_view field) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::Hash) {
    return std::unexpected(ErrorCode::WrongType);
  }
  std::string raw;
  if (!impl_->db->Get(impl_->read_opts, encode_hash_key(key, meta->version, field), &raw).ok()) {
    return 0;
  }
  return static_cast<uint64_t>(raw.size());
}

/**
 * @brief ハッシュからランダムなフィールド名を取得する
 *
 * @copydoc EmbeddedRedis::hrandfield
 */
auto EmbeddedRedis::hrandfield(std::string_view key) -> Result<std::string> {
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
  if (meta->size == 0) {
    return std::unexpected(ErrorCode::NotFound);
  }

  auto const pfx = encode_hash_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  // 最初のフィールドを返す（組み込み用途では十分ランダム）
  it->Seek(pfx_slice);
  if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
    return std::unexpected(ErrorCode::NotFound);
  }
  return std::string(extract_suffix(std::string_view(it->key().data(), it->key().size()), 1, key));
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

/**
 * @brief リストの範囲内の要素を取得する
 *
 * @copydoc EmbeddedRedis::lrange
 */
auto EmbeddedRedis::lrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
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

  // 内部シーケンス番号に変換
  auto const head = static_cast<int64_t>(meta->head_seq);
  auto const seq_start = static_cast<uint64_t>(head + start);
  auto const count_needed = static_cast<uint64_t>(stop - start + 1);

  auto const pfx = encode_list_prefix(key, meta->version);
  auto const seek_key = encode_list_key(key, meta->version, seq_start);
  rocksdb::Slice const pfx_slice(pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  auto const seq_offset = 1 + key.size() + 8; // prefix(1) + key + version(8)

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
    result.emplace_back(it->value().ToString());
    if (result.size() >= count_needed) {
      break;
    }
  }
  return result;
}

/**
 * @brief リストから指定値を削除する
 *
 * @copydoc EmbeddedRedis::lrem
 */
auto EmbeddedRedis::lrem(std::string_view key, int64_t count, std::string_view value) -> Result<uint64_t> {
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
    return 0;
  }

  uint64_t const limit = (count == 0) ? std::numeric_limits<uint64_t>::max() : static_cast<uint64_t>(std::abs(count));

  auto const pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const seq_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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

  rocksdb::WriteBatch batch;
  for (auto const seq : to_delete) {
    batch.Delete(encode_list_key(key, meta->version, seq));
  }

  meta->size -= removed;
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    // 残存要素から head_seq / tail_seq を再計算
    // （先頭・末尾の要素が削除された場合に対応するため）
    std::optional<uint64_t> new_head;
    std::optional<uint64_t> new_tail;
    for (auto const& [seq, v] : all) {
      // 削除対象かどうかを判定
      bool const deleted = std::find(to_delete.begin(), to_delete.end(), seq) != to_delete.end();
      if (!deleted) {
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
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return removed;
}

/**
 * @brief リストを範囲でトリムする（範囲外を削除）
 *
 * @copydoc EmbeddedRedis::ltrim
 */
auto EmbeddedRedis::ltrim(std::string_view key, int64_t start, int64_t stop) -> Result<void> {
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

  auto const size = static_cast<int64_t>(meta->size);

  // 負のインデックスを正に変換
  if (start < 0) start += size;
  if (stop < 0) stop += size;

  // クランプ
  if (start < 0) start = 0;
  if (stop >= size) stop = size - 1;

  rocksdb::WriteBatch batch;

  if (start > stop || size == 0) {
    // 全削除
    impl_->erase_key_data(batch, key, *meta);
  } else {
    auto const head = static_cast<int64_t>(meta->head_seq);
    auto const new_head = static_cast<uint64_t>(head + start);
    auto const new_tail = static_cast<uint64_t>(head + stop + 1); // tail_seq は次の位置
    auto const new_size = static_cast<uint64_t>(stop - start + 1);

    // 範囲外を削除
    for (uint64_t seq = meta->head_seq; seq < new_head; ++seq) {
      batch.Delete(encode_list_key(key, meta->version, seq));
    }
    for (uint64_t seq = new_tail; seq < meta->tail_seq; ++seq) {
      batch.Delete(encode_list_key(key, meta->version, seq));
    }

    meta->head_seq = new_head;
    meta->tail_seq = new_tail;
    meta->size     = new_size;
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return {};
}

/**
 * @brief リストの長さを取得する
 *
 * @copydoc EmbeddedRedis::llen
 */
auto EmbeddedRedis::llen(std::string_view key) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }
  return meta->size;
}

/**
 * @brief リストの指定インデックスの要素を取得する
 *
 * @copydoc EmbeddedRedis::lindex
 */
auto EmbeddedRedis::lindex(std::string_view key, int64_t index) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
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
  auto const seq_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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

// ---- New List Functions ----

/**
 * @brief リストの指定インデックスに値を設定する
 *
 * @copydoc EmbeddedRedis::lset
 */
auto EmbeddedRedis::lset(std::string_view key, int64_t index, std::string_view value) -> Result<void> {
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
  auto const seq_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
      rocksdb::WriteBatch batch;
      batch.Put(k, rocksdb::Slice(value.data(), value.size()));
      if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
        return std::unexpected(ErrorCode::RocksDBError);
      }
      return {};
    }
    pos++;
  }
  return std::unexpected(ErrorCode::NotFound);
}

/**
 * @brief リスト内の要素の位置を検索する
 *
 * @copydoc EmbeddedRedis::lpos
 */
auto EmbeddedRedis::lpos(std::string_view key, std::string_view element) -> Result<int64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const seek_key = encode_list_key(key, meta->version, meta->head_seq);
  auto const seq_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
}

/**
 * @brief リストが存在する場合のみ先頭に追加する
 *
 * @copydoc EmbeddedRedis::lpushx
 */
auto EmbeddedRedis::lpushx(std::string_view key, std::string_view value) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  meta->head_seq--;
  meta->size++;

  rocksdb::WriteBatch batch;
  batch.Put(encode_list_key(key, meta->version, meta->head_seq), rocksdb::Slice(value.data(), value.size()));
  impl_->put_meta(batch, key, *meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return meta->size;
}

/**
 * @brief リストが存在する場合のみ末尾に追加する
 *
 * @copydoc EmbeddedRedis::rpushx
 */
auto EmbeddedRedis::rpushx(std::string_view key, std::string_view value) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::List) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const seq = meta->tail_seq;
  meta->tail_seq++;
  meta->size++;

  rocksdb::WriteBatch batch;
  batch.Put(encode_list_key(key, meta->version, seq), rocksdb::Slice(value.data(), value.size()));
  impl_->put_meta(batch, key, *meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return meta->size;
}

/**
 * @brief ピボット値の前後に要素を挿入する
 *
 * @copydoc EmbeddedRedis::linsert
 */
auto EmbeddedRedis::linsert(std::string_view key, InsertPosition pos, std::string_view pivot, std::string_view value) -> Result<uint64_t> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);
  auto meta = impl_->get_meta(key);
  if (!meta) return std::unexpected(ErrorCode::NotFound);
  if (meta->type != DataType::List) return std::unexpected(ErrorCode::WrongType);
  if (meta->size == 0) return std::unexpected(ErrorCode::NotFound);

  auto const pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const seq_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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

  rocksdb::WriteBatch batch;
  // delete old elements
  for (auto const s : seqs) {
    batch.Delete(encode_list_key(key, meta->version, s));
  }
  // write new elements with fresh seqs
  auto const base = meta->head_seq + meta->size + 1000; // avoid collision
  for (std::size_t i = 0; i < vals.size(); ++i) {
    batch.Put(encode_list_key(key, meta->version, base + i), vals[i]);
  }
  meta->head_seq = base;
  meta->tail_seq = base + vals.size();
  meta->size = vals.size();
  impl_->put_meta(batch, key, *meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return meta->size;
}

/**
 * @brief ソースリストから要素を取り出し、宛先リストに挿入する
 *
 * @copydoc EmbeddedRedis::lmove
 */
auto EmbeddedRedis::lmove(std::string_view source, std::string_view destination, ListSide from, ListSide to) -> Result<std::string> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);

  auto src_meta = impl_->get_meta(source);
  if (!src_meta) return std::unexpected(ErrorCode::NotFound);
  if (src_meta->type != DataType::List) return std::unexpected(ErrorCode::WrongType);
  if (src_meta->size == 0) return std::unexpected(ErrorCode::NotFound);

  auto dst_meta = impl_->get_meta(destination);
  if (dst_meta && dst_meta->type != DataType::List) return std::unexpected(ErrorCode::WrongType);

  std::string value;

  rocksdb::WriteBatch batch;

  // pop from source
  uint64_t popped_seq = 0;
  if (from == ListSide::Left) {
    popped_seq = src_meta->head_seq;
    auto const lk = encode_list_key(source, src_meta->version, popped_seq);
    std::string tmp;
    if (!impl_->db->Get(impl_->read_opts, lk, &tmp).ok()) return std::unexpected(ErrorCode::NotFound);
    value = std::move(tmp);
    batch.Delete(lk);
    src_meta->head_seq++;
  } else {
    src_meta->tail_seq--;
    popped_seq = src_meta->tail_seq;
    auto const lk = encode_list_key(source, src_meta->version, popped_seq);
    std::string tmp;
    if (!impl_->db->Get(impl_->read_opts, lk, &tmp).ok()) return std::unexpected(ErrorCode::NotFound);
    value = std::move(tmp);
    batch.Delete(lk);
  }
  src_meta->size--;

  if (src_meta->size == 0) {
    batch.Delete(encode_meta_key(source));
  } else {
    impl_->put_meta(batch, source, *src_meta);
  }

  // push to destination
  if (!dst_meta) {
    dst_meta = MetaValue{};
    dst_meta->type = DataType::List;
  }
  if (to == ListSide::Left) {
    if (dst_meta->size == 0) {
      dst_meta->head_seq = 0;
      dst_meta->tail_seq = 1;
      batch.Put(encode_list_key(destination, dst_meta->version, 0), value);
    } else {
      dst_meta->head_seq--;
      batch.Put(encode_list_key(destination, dst_meta->version, dst_meta->head_seq), value);
    }
  } else {
    if (dst_meta->size == 0) {
      dst_meta->head_seq = 0;
    }
    batch.Put(encode_list_key(destination, dst_meta->version, dst_meta->tail_seq), value);
    dst_meta->tail_seq++;
  }
  dst_meta->size++;
  impl_->put_meta(batch, destination, *dst_meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return value;
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

/**
 * @brief セットからメンバーを削除する
 *
 * @copydoc EmbeddedRedis::srem
 */
auto EmbeddedRedis::srem(std::string_view key, std::string_view member) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
  if (!meta) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (meta->type != DataType::Set) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const sk = encode_set_key(key, meta->version, member);

  // メンバーの存在確認
  std::string dummy;
  if (!impl_->db->Get(impl_->read_opts, sk, &dummy).ok()) {
    return false;
  }

  rocksdb::WriteBatch batch;
  batch.Delete(sk);

  meta->size--;
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

/**
 * @brief セットのメンバー数を取得する
 *
 * @copydoc EmbeddedRedis::scard
 */
auto EmbeddedRedis::scard(std::string_view key) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::Set) {
    return std::unexpected(ErrorCode::WrongType);
  }
  return meta->size;
}

/**
 * @brief セットのメンバー存在確認
 *
 * @copydoc EmbeddedRedis::sismember
 */
auto EmbeddedRedis::sismember(std::string_view key, std::string_view member) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }
  if (meta->type != DataType::Set) {
    return std::unexpected(ErrorCode::WrongType);
  }
  std::string v;
  return impl_->db->Get(impl_->read_opts, encode_set_key(key, meta->version, member), &v).ok();
}

/**
 * @brief セットからランダムなメンバーを削除して取得する
 *
 * @copydoc EmbeddedRedis::spop
 */
auto EmbeddedRedis::spop(std::string_view key) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
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

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  it->Seek(pfx_slice);
  if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
    return std::unexpected(ErrorCode::NotFound);
  }

  auto const member_key = it->key();
  auto const member = extract_suffix(std::string_view(member_key.data(), member_key.size()), 1, key);

  rocksdb::WriteBatch batch;
  batch.Delete(member_key);
  meta->size--;
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return std::string(member);
}

/**
 * @brief セットからランダムなメンバーを取得する（削除しない）
 *
 * @copydoc EmbeddedRedis::srandmember
 */
auto EmbeddedRedis::srandmember(std::string_view key) -> Result<std::string> {
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
  if (meta->size == 0) {
    return std::unexpected(ErrorCode::NotFound);
  }

  auto const pfx = encode_set_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  it->Seek(pfx_slice);
  if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
    return std::unexpected(ErrorCode::NotFound);
  }
  return std::string(extract_suffix(std::string_view(it->key().data(), it->key().size()), 1, key));
}

/**
 * @brief セット間でメンバーを移動する
 *
 * @copydoc EmbeddedRedis::smove
 */
auto EmbeddedRedis::smove(std::string_view source, std::string_view destination, std::string_view member) -> Result<bool> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);

  auto src_meta = impl_->get_meta(source);
  if (!src_meta) return false;
  if (src_meta->type != DataType::Set) return std::unexpected(ErrorCode::WrongType);

  auto const src_key = encode_set_key(source, src_meta->version, member);
  std::string dummy;
  if (!impl_->db->Get(impl_->read_opts, src_key, &dummy).ok()) return false;

  auto dst_meta = impl_->get_meta(destination);
  if (dst_meta && dst_meta->type != DataType::Set) return std::unexpected(ErrorCode::WrongType);

  rocksdb::WriteBatch batch;
  batch.Delete(src_key);
  src_meta->size--;
  if (src_meta->size == 0) {
    batch.Delete(encode_meta_key(source));
  } else {
    impl_->put_meta(batch, source, *src_meta);
  }

  if (!dst_meta) {
    dst_meta = MetaValue{};
    dst_meta->type = DataType::Set;
  }
  auto const dst_key = encode_set_key(destination, dst_meta->version, member);
  std::string dst_dummy;
  bool const is_new_dst = !impl_->db->Get(impl_->read_opts, dst_key, &dst_dummy).ok();
  if (is_new_dst) {
    dst_meta->size++;
  }
  batch.Put(dst_key, rocksdb::Slice());
  impl_->put_meta(batch, destination, *dst_meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
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

/**
 * @brief ソート済みセットからメンバーを削除する
 *
 * @copydoc EmbeddedRedis::zrem
 */
auto EmbeddedRedis::zrem(std::string_view key, std::string_view member) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }
  if (meta->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const mk = encode_zset_member_key(key, meta->version, member);
  std::string score_raw;
  if (!impl_->db->Get(impl_->read_opts, mk, &score_raw).ok()) {
    return false;
  }
  if (score_raw.size() != 8) {
    return false;
  }

  auto const old_score = decode_score(reinterpret_cast<uint8_t const*>(score_raw.data()));

  rocksdb::WriteBatch batch;
  batch.Delete(mk);
  batch.Delete(encode_zset_score_key(key, meta->version, old_score, member));
  meta->size--;
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

/**
 * @brief ソート済みセットのメンバー数を取得する
 *
 * @copydoc EmbeddedRedis::zcard
 */
auto EmbeddedRedis::zcard(std::string_view key) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }
  return meta->size;
}

/**
 * @brief スコア範囲内のメンバー数を取得する
 *
 * @copydoc EmbeddedRedis::zcount
 */
auto EmbeddedRedis::zcount(std::string_view key, double min_score, double max_score) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }

  auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
  auto const seek_key  = encode_zset_score_seek_key(key, meta->version, min_score);
  rocksdb::Slice const pfx_slice(range_pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  uint64_t count = 0;
  auto const score_offset = 1 + key.size() + 8;

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
}

/**
 * @brief メンバーのスコアを取得する
 *
 * @copydoc EmbeddedRedis::zscore
 */
auto EmbeddedRedis::zscore(std::string_view key, std::string_view member) -> Result<double> {
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

  std::string score_raw;
  if (!impl_->db->Get(impl_->read_opts, encode_zset_member_key(key, meta->version, member), &score_raw).ok()) {
    return std::unexpected(ErrorCode::NotFound);
  }
  if (score_raw.size() != 8) {
    return std::unexpected(ErrorCode::NotFound);
  }
  return decode_score(reinterpret_cast<uint8_t const*>(score_raw.data()));
}

/**
 * @brief メンバーのランクを取得する（昇順、0 ベース）
 *
 * @copydoc EmbeddedRedis::zrank
 */
auto EmbeddedRedis::zrank(std::string_view key, std::string_view member) -> Result<int64_t> {
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

  auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(range_pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  auto const score_offset = 1 + key.size() + 8;
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
}

/**
 * @brief ランク範囲でメンバーを取得する（昇順）
 *
 * @copydoc EmbeddedRedis::zrange
 */
auto EmbeddedRedis::zrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
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

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  std::vector<std::string> result;
  result.reserve(static_cast<std::size_t>(stop - start + 1));

  auto const score_offset = 1 + key.size() + 8;
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

// ---- New ZSet Functions ----

/**
 * @brief メンバーのスコアを指定量インクリメントする
 *
 * @copydoc EmbeddedRedis::zincrby
 */
auto EmbeddedRedis::zincrby(std::string_view key, std::string_view member, double delta) -> Result<double> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto existing = impl_->get_meta(key);
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
  bool const found = impl_->db->Get(impl_->read_opts, mk, &old_raw).ok();

  double new_score = delta;
  if (found) {
    if (old_raw.size() == 8) {
      new_score = decode_score(reinterpret_cast<uint8_t const*>(old_raw.data())) + delta;
    }
  }
  if (!found) {
    meta.size++;
  }

  rocksdb::WriteBatch batch;
  if (found && old_raw.size() == 8) {
    auto const old_score = decode_score(reinterpret_cast<uint8_t const*>(old_raw.data()));
    batch.Delete(encode_zset_score_key(key, meta.version, old_score, member));
  }
  auto const sb = encode_score(new_score);
  batch.Put(mk, rocksdb::Slice(reinterpret_cast<char const*>(sb.data()), 8));
  batch.Put(encode_zset_score_key(key, meta.version, new_score, member), rocksdb::Slice());
  impl_->put_meta(batch, key, meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return new_score;
}

/**
 * @brief メンバーのランクを取得する（降順、0 ベース）
 *
 * @copydoc EmbeddedRedis::zrevrank
 */
auto EmbeddedRedis::zrevrank(std::string_view key, std::string_view member) -> Result<int64_t> {
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

  auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(range_pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  auto const score_offset = 1 + key.size() + 8;
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
}

/**
 * @brief ランク範囲でメンバーを取得する（降順）
 *
 * @copydoc EmbeddedRedis::zrevrange
 */
auto EmbeddedRedis::zrevrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return std::vector<std::string>{};
  }
  if (meta->type != DataType::ZSet) {
    return std::unexpected(ErrorCode::WrongType);
  }

  // Get all members first, then reverse slice
  auto all = zrange(key, 0, -1);
  if (!all) return all;
  auto const size = static_cast<int64_t>(all->size());
  if (size == 0) return std::vector<std::string>{};

  if (start < 0) start += size;
  if (stop < 0) stop += size;
  if (start < 0) start = 0;
  if (stop < 0) stop = 0;
  if (start >= size) start = size - 1;
  if (stop >= size) stop = size - 1;
  if (start > stop) return std::vector<std::string>{};

  auto const rev_start = size - 1 - stop;
  auto const rev_stop  = size - 1 - start;
  auto const count = rev_stop - rev_start + 1;

  std::vector<std::string> result;
  result.reserve(static_cast<std::size_t>(count));
  for (auto i = rev_stop; i >= rev_start && i >= 0; --i) {
    result.emplace_back(std::move((*all)[static_cast<std::size_t>(i)]));
  }
  return result;
}

/**
 * @brief 最小スコアのメンバーを削除して取得する
 *
 * @copydoc EmbeddedRedis::zpopmin
 */
auto EmbeddedRedis::zpopmin(std::string_view key) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
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

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  it->Seek(pfx_slice);
  if (!it->Valid() || !it->key().starts_with(pfx_slice)) {
    return std::unexpected(ErrorCode::NotFound);
  }

  auto const k = it->key();
  auto const score_offset = 1 + key.size() + 8;
  auto const member = std::string(k.data() + score_offset + 8, k.size() - score_offset - 8);

  // Get score from member key
  auto const mk = encode_zset_member_key(key, meta->version, member);
  std::string score_raw;
  if (!impl_->db->Get(impl_->read_opts, mk, &score_raw).ok() || score_raw.size() != 8) {
    return std::unexpected(ErrorCode::NotFound);
  }
  auto const score = decode_score(reinterpret_cast<uint8_t const*>(score_raw.data()));

  rocksdb::WriteBatch batch;
  batch.Delete(mk);
  batch.Delete(encode_zset_score_key(key, meta->version, score, member));
  meta->size--;
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return member;
}

/**
 * @brief 最大スコアのメンバーを削除して取得する
 *
 * @copydoc EmbeddedRedis::zpopmax
 */
auto EmbeddedRedis::zpopmax(std::string_view key) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
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

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
  auto const score_offset = 1 + key.size() + 8;
  auto const member = std::string(k.data() + score_offset + 8, k.size() - score_offset - 8);

  auto const mk = encode_zset_member_key(key, meta->version, member);

  rocksdb::WriteBatch batch;
  batch.Delete(mk);
  batch.Delete(k);
  meta->size--;
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return member;
}

/**
 * @brief 複数メンバーのスコアを取得する
 *
 * @copydoc EmbeddedRedis::zmscore
 */
auto EmbeddedRedis::zmscore(std::string_view key, std::vector<std::string_view> const& members) -> Result<std::vector<std::optional<double>>> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
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
    if (impl_->db->Get(impl_->read_opts, encode_zset_member_key(key, meta->version, member), &raw).ok() && raw.size() == 8) {
      result.emplace_back(decode_score(reinterpret_cast<uint8_t const*>(raw.data())));
    } else {
      result.emplace_back(std::nullopt);
    }
  }
  return result;
}

// ---- New ZSet Functions (Medium) ----

/**
 * @brief 辞書式順序でメンバー範囲を取得する
 *
 * @copydoc EmbeddedRedis::zrangebylex
 */
auto EmbeddedRedis::zrangebylex(std::string_view key, std::string_view min, std::string_view max) -> Result<std::vector<std::string>> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);
  auto const meta = impl_->get_meta(key);
  if (!meta) return std::vector<std::string>{};
  if (meta->type != DataType::ZSet) return std::unexpected(ErrorCode::WrongType);

  auto const pfx = encode_zset_member_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const member_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
}

/**
 * @brief 辞書式順序で範囲内のメンバー数をカウントする
 *
 * @copydoc EmbeddedRedis::zlexcount
 */
auto EmbeddedRedis::zlexcount(std::string_view key, std::string_view min, std::string_view max) -> Result<uint64_t> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);
  auto const meta = impl_->get_meta(key);
  if (!meta) return 0;
  if (meta->type != DataType::ZSet) return std::unexpected(ErrorCode::WrongType);

  auto const pfx = encode_zset_member_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const member_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
}

/**
 * @brief ランク範囲でメンバーを削除する
 *
 * @copydoc EmbeddedRedis::zremrangebyrank
 */
auto EmbeddedRedis::zremrangebyrank(std::string_view key, int64_t start, int64_t stop) -> Result<uint64_t> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);
  auto meta = impl_->get_meta(key);
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
  auto const score_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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

  rocksdb::WriteBatch batch;
  for (std::size_t i = 0; i < to_delete.size(); ++i) {
    batch.Delete(encode_zset_member_key(key, meta->version, to_delete[i]));
    batch.Delete(encode_zset_score_key(key, meta->version, scores[i], to_delete[i]));
  }
  meta->size -= to_delete.size();
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return static_cast<uint64_t>(to_delete.size());
}

/**
 * @brief スコア範囲でメンバーを削除する
 *
 * @copydoc EmbeddedRedis::zremrangebyscore
 */
auto EmbeddedRedis::zremrangebyscore(std::string_view key, double min, double max) -> Result<uint64_t> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);
  auto meta = impl_->get_meta(key);
  if (!meta) return 0;
  if (meta->type != DataType::ZSet) return std::unexpected(ErrorCode::WrongType);
  if (meta->size == 0) return 0;

  auto const range_pfx = encode_zset_score_range_prefix(key, meta->version);
  auto const seek_key  = encode_zset_score_seek_key(key, meta->version, min);
  rocksdb::Slice const pfx_slice(range_pfx);
  auto const score_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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

  rocksdb::WriteBatch batch;
  for (std::size_t i = 0; i < to_delete.size(); ++i) {
    batch.Delete(encode_zset_member_key(key, meta->version, to_delete[i]));
    batch.Delete(encode_zset_score_key(key, meta->version, scores[i], to_delete[i]));
  }
  meta->size -= to_delete.size();
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return static_cast<uint64_t>(to_delete.size());
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

// ---- New Stream Functions ----

/**
 * @brief ストリームの長さを取得する
 *
 * @copydoc EmbeddedRedis::xlen
 */
auto EmbeddedRedis::xlen(std::string_view key) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::Stream) {
    return std::unexpected(ErrorCode::WrongType);
  }
  return meta->size;
}

/**
 * @brief ストリームからエントリを削除する
 *
 * @copydoc EmbeddedRedis::xdel
 */
auto EmbeddedRedis::xdel(std::string_view key, std::vector<std::string_view> const& ids) -> Result<uint64_t> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
  if (!meta) {
    return 0;
  }
  if (meta->type != DataType::Stream) {
    return std::unexpected(ErrorCode::WrongType);
  }

  rocksdb::WriteBatch batch;
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
    if (impl_->db->Get(impl_->read_opts, sk, &exist).ok()) {
      batch.Delete(sk);
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
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return removed;
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
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);
  auto const meta = impl_->get_meta(key);
  if (!meta) return std::vector<StreamEntry>{};
  if (meta->type != DataType::Stream) return std::unexpected(ErrorCode::WrongType);
  if (meta->size == 0) return std::vector<StreamEntry>{};

  uint64_t start_ms = 0, start_seq = 0;
  uint64_t end_ms = 0, end_seq = 0;
  bool const has_start = parse_stream_id(start, start_ms, start_seq);
  bool const has_end = parse_stream_id(end, end_ms, end_seq);

  auto const pfx = encode_stream_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const ms_offset = 1 + key.size() + 8;
  auto const seq_offset = ms_offset + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
}

/**
 * @brief ストリームのエントリ範囲を取得する（降順）
 *
 * @copydoc EmbeddedRedis::xrevrange
 */
auto EmbeddedRedis::xrevrange(std::string_view key, std::string_view start, std::string_view end) -> Result<std::vector<StreamEntry>> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);
  auto const meta = impl_->get_meta(key);
  if (!meta) return std::vector<StreamEntry>{};
  if (meta->type != DataType::Stream) return std::unexpected(ErrorCode::WrongType);
  if (meta->size == 0) return std::vector<StreamEntry>{};

  uint64_t start_ms = 0, start_seq = 0;
  uint64_t end_ms = 0, end_seq = 0;
  bool const has_start = parse_stream_id(start, start_ms, start_seq);
  bool const has_end = parse_stream_id(end, end_ms, end_seq);

  auto const pfx = encode_stream_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const ms_offset = 1 + key.size() + 8;
  auto const seq_offset = ms_offset + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

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
}

/**
 * @brief ストリームを最大長でトリムする
 *
 * @copydoc EmbeddedRedis::xtrim
 */
auto EmbeddedRedis::xtrim(std::string_view key, uint64_t maxlen) -> Result<uint64_t> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);
  auto meta = impl_->get_meta(key);
  if (!meta) return 0;
  if (meta->type != DataType::Stream) return std::unexpected(ErrorCode::WrongType);
  if (meta->size <= maxlen) return 0;

  auto const pfx = encode_stream_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(iter_opts));

  uint64_t to_remove = meta->size - maxlen;
  rocksdb::WriteBatch batch;

  uint64_t removed = 0;
  for (it->Seek(pfx_slice); it->Valid() && removed < to_remove; it->Next()) {
    auto const k = it->key();
    if (!k.starts_with(pfx_slice)) break;
    batch.Delete(k);
    removed++;
  }

  if (removed == 0) return 0;

  meta->size -= removed;
  if (meta->size == 0) {
    batch.Delete(encode_meta_key(key));
  } else {
    impl_->put_meta(batch, key, *meta);
  }

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return removed;
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

/**
 * @brief キーのデータ型を返す
 *
 * @copydoc EmbeddedRedis::type
 */
auto EmbeddedRedis::type(std::string_view key) -> Result<std::string> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto const meta = impl_->get_meta(key);
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
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }

  meta->expiration_ms = unix_time;

  rocksdb::WriteBatch batch;
  impl_->put_meta(batch, key, *meta);

  if (!impl_->db->Write(impl_->write_opts, &batch).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  return true;
}

/**
 * @brief キーの最終アクセス時刻を更新する
 *
 * @copydoc EmbeddedRedis::touch
 */
auto EmbeddedRedis::touch(std::string_view key) -> Result<bool> {
  if (!is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  auto meta = impl_->get_meta(key);
  if (!meta) {
    return false;
  }
  // RocksDB ベースの組み込み DB では touch の効果は限定的だが、TTL 期限切れは検出する
  return true;
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
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);

  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(opts));

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
    } else if (rand() % count == 0) {
      result.assign(k.data() + 1, k.size() - 1);
    }
  }
  if (count == 0) return std::unexpected(ErrorCode::NotFound);
  return result;
}

/**
 * @brief パターンに一致するキーを検索する
 *
 * @copydoc EmbeddedRedis::keys
 */
auto EmbeddedRedis::keys(std::string_view pattern) -> Result<std::vector<std::string>> {
  if (!is_open()) return std::unexpected(ErrorCode::RocksDBError);

  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(opts));

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
}

// ============================================================
// Pipeline
// ============================================================

auto EmbeddedRedis::pipeline() -> Pipeline {
  return Pipeline(*this);
}

auto EmbeddedRedis::Pipeline::get_meta_cached(std::string_view key) -> std::optional<MetaValue> {
  auto it = meta_cache_.find(std::string(key));
  if (it != meta_cache_.end()) {
    return it->second;
  }
  auto meta = db_.impl_->get_meta(key);
  if (meta) {
    meta_cache_[std::string(key)] = *meta;
  }
  return meta;
}

void EmbeddedRedis::Pipeline::update_meta_cache(std::string_view key, MetaValue const& meta) {
  meta_cache_[std::string(key)] = meta;
}

auto EmbeddedRedis::Pipeline::set(std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms) -> Pipeline& {
  auto existing = get_meta_cached(key);

  // 既存キーが別の型なら先に全削除する
  if (existing && existing->type != DataType::String) {
    db_.impl_->erase_key_data(batch_, key, *existing);
    existing = std::nullopt;
  }

  MetaValue meta;
  meta.type          = DataType::String;
  meta.version       = 1;
  meta.size          = value.size();
  meta.expiration_ms = ttl_ms ? now_ms() + *ttl_ms : 0;

  db_.impl_->put_meta(batch_, key, meta);
  batch_.Put(encode_string_key(key), rocksdb::Slice(value.data(), value.size()));
  update_meta_cache(key, meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::hset(std::string_view key, std::string_view field, std::string_view value) -> Pipeline& {
  auto existing = get_meta_cached(key);
  if (existing && existing->type != DataType::Hash) {
    set_error(ErrorCode::WrongType);
    return *this;
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
  bool const  is_new = !db_.impl_->db->Get(db_.impl_->read_opts, hk, &dummy).ok();
  if (is_new) {
    meta.size++;
  }

  batch_.Put(hk, rocksdb::Slice(value.data(), value.size()));
  db_.impl_->put_meta(batch_, key, meta);
  update_meta_cache(key, meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::lpush(std::string_view key, std::string_view value) -> Pipeline& {
  auto existing = get_meta_cached(key);
  if (existing && existing->type != DataType::List) {
    set_error(ErrorCode::WrongType);
    return *this;
  }

  auto meta = init_list_meta(existing);
  meta.head_seq--;
  meta.size++;

  batch_.Put(encode_list_key(key, meta.version, meta.head_seq), rocksdb::Slice(value.data(), value.size()));
  db_.impl_->put_meta(batch_, key, meta);
  update_meta_cache(key, meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::rpush(std::string_view key, std::string_view value) -> Pipeline& {
  auto existing = get_meta_cached(key);
  if (existing && existing->type != DataType::List) {
    set_error(ErrorCode::WrongType);
    return *this;
  }

  auto meta = init_list_meta(existing);

  batch_.Put(encode_list_key(key, meta.version, meta.tail_seq), rocksdb::Slice(value.data(), value.size()));
  meta.tail_seq++;
  meta.size++;
  db_.impl_->put_meta(batch_, key, meta);
  update_meta_cache(key, meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::sadd(std::string_view key, std::string_view member) -> Pipeline& {
  auto existing = get_meta_cached(key);
  if (existing && existing->type != DataType::Set) {
    set_error(ErrorCode::WrongType);
    return *this;
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
  bool const  is_new = !db_.impl_->db->Get(db_.impl_->read_opts, sk, &dummy).ok();
  if (is_new) {
    meta.size++;
  }

  batch_.Put(sk, rocksdb::Slice());
  db_.impl_->put_meta(batch_, key, meta);
  update_meta_cache(key, meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::srem(std::string_view key, std::string_view member) -> Pipeline& {
  auto meta = get_meta_cached(key);
  if (!meta) {
    return *this;
  }
  if (meta->type != DataType::Set) {
    set_error(ErrorCode::WrongType);
    return *this;
  }

  auto const sk = encode_set_key(key, meta->version, member);

  // メンバーの存在確認
  std::string dummy;
  if (!db_.impl_->db->Get(db_.impl_->read_opts, sk, &dummy).ok()) {
    return *this;
  }

  batch_.Delete(sk);
  meta->size--;
  if (meta->size == 0) {
    batch_.Delete(encode_meta_key(key));
  } else {
    db_.impl_->put_meta(batch_, key, *meta);
  }
  update_meta_cache(key, *meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::zadd(std::string_view key, double score, std::string_view member) -> Pipeline& {
  auto existing = get_meta_cached(key);
  if (existing && existing->type != DataType::ZSet) {
    set_error(ErrorCode::WrongType);
    return *this;
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
  bool const  is_new = !db_.impl_->db->Get(db_.impl_->read_opts, mk, &old_score_raw).ok();

  if (!is_new && old_score_raw.size() == 8) {
    auto const old_score = decode_score(reinterpret_cast<uint8_t const*>(old_score_raw.data()));
    batch_.Delete(encode_zset_score_key(key, meta.version, old_score, member));
  }
  if (is_new) {
    meta.size++;
  }

  // member key → score(8byte encoded)
  auto const sb = encode_score(score);
  batch_.Put(mk, rocksdb::Slice(reinterpret_cast<char const*>(sb.data()), 8));

  // score key → empty
  batch_.Put(encode_zset_score_key(key, meta.version, score, member), rocksdb::Slice());
  db_.impl_->put_meta(batch_, key, meta);
  update_meta_cache(key, meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::xadd(std::string_view key, std::string_view id, std::vector<std::pair<std::string, std::string>> const& fields) -> Pipeline& {
  auto existing = get_meta_cached(key);
  if (existing && existing->type != DataType::Stream) {
    return *this;
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
      set_error(ErrorCode::InvalidArgument);
      return *this; // InvalidArgument
    }
    auto const ms_sv  = id.substr(0, dash);
    auto const seq_sv = id.substr(dash + 1);
    auto       r1     = std::from_chars(ms_sv.data(), ms_sv.data() + ms_sv.size(), ms);
    auto       r2     = std::from_chars(seq_sv.data(), seq_sv.data() + seq_sv.size(), seq);
    if (r1.ec != std::errc{} || r2.ec != std::errc{}) {
      set_error(ErrorCode::InvalidArgument);
      return *this; // InvalidArgument
    }
    // 単調増加チェック: 以前の ID より大きくなければならない
    if (ms < meta.last_ms || (ms == meta.last_ms && seq <= meta.last_seq)) {
      set_error(ErrorCode::InvalidArgument);
      return *this; // InvalidArgument
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

  batch_.Put(encode_stream_key(key, meta.version, ms, seq), data);
  db_.impl_->put_meta(batch_, key, meta);
  update_meta_cache(key, meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::del(std::string_view key) -> Pipeline& {
  auto meta = get_meta_cached(key);
  if (!meta) {
    return *this;
  }
  db_.impl_->erase_key_data(batch_, key, *meta);
  meta_cache_.erase(std::string(key));
  return *this;
}

auto EmbeddedRedis::Pipeline::expire(std::string_view key, uint64_t seconds) -> Pipeline& {
  return pexpire(key, seconds * 1000);
}

auto EmbeddedRedis::Pipeline::pexpire(std::string_view key, uint64_t milliseconds) -> Pipeline& {
  auto meta = get_meta_cached(key);
  if (!meta) {
    return *this;
  }

  auto updated = *meta;
  updated.expiration_ms = now_ms() + milliseconds;

  db_.impl_->put_meta(batch_, key, updated);
  update_meta_cache(key, updated);
  return *this;
}

auto EmbeddedRedis::Pipeline::persist(std::string_view key) -> Pipeline& {
  auto meta = get_meta_cached(key);
  if (!meta || meta->expiration_ms == 0) {
    return *this;
  }

  auto updated = *meta;
  updated.expiration_ms = 0;

  db_.impl_->put_meta(batch_, key, updated);
  update_meta_cache(key, updated);
  return *this;
}

auto EmbeddedRedis::Pipeline::append(std::string_view key, std::string_view value) -> Pipeline& {
  auto existing = get_meta_cached(key);
  if (existing && existing->type != DataType::String) {
    set_error(ErrorCode::WrongType);
    return *this;
  }

  std::string current;
  if (existing) {
    std::string raw;
    if (db_.impl_->db->Get(db_.impl_->read_opts, encode_string_key(key), &raw).ok()) {
      current = std::move(raw);
    }
  }

  current += value;

  MetaValue meta;
  meta.type    = DataType::String;
  meta.version = existing ? existing->version : 1;
  meta.size    = current.size();

  db_.impl_->put_meta(batch_, key, meta);
  batch_.Put(encode_string_key(key), rocksdb::Slice(current));
  update_meta_cache(key, meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::hdel(std::string_view key, std::string_view field) -> Pipeline& {
  auto meta = get_meta_cached(key);
  if (!meta) {
    return *this;
  }
  if (meta->type != DataType::Hash) {
    set_error(ErrorCode::WrongType);
    return *this;
  }
  if (meta->size == 0) {
    return *this;
  }

  auto const hk = encode_hash_key(key, meta->version, field);
  std::string dummy;
  if (!db_.impl_->db->Get(db_.impl_->read_opts, hk, &dummy).ok()) {
    return *this;
  }

  batch_.Delete(hk);
  meta->size--;
  if (meta->size == 0) {
    batch_.Delete(encode_meta_key(key));
  } else {
    db_.impl_->put_meta(batch_, key, *meta);
  }
  update_meta_cache(key, *meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::zrem(std::string_view key, std::string_view member) -> Pipeline& {
  auto meta = get_meta_cached(key);
  if (!meta) {
    return *this;
  }
  if (meta->type != DataType::ZSet) {
    set_error(ErrorCode::WrongType);
    return *this;
  }

  auto const mk = encode_zset_member_key(key, meta->version, member);
  std::string score_raw;
  if (!db_.impl_->db->Get(db_.impl_->read_opts, mk, &score_raw).ok()) {
    return *this;
  }
  if (score_raw.size() != 8) {
    return *this;
  }

  auto const old_score = decode_score(reinterpret_cast<uint8_t const*>(score_raw.data()));
  batch_.Delete(mk);
  batch_.Delete(encode_zset_score_key(key, meta->version, old_score, member));
  meta->size--;
  if (meta->size == 0) {
    batch_.Delete(encode_meta_key(key));
  } else {
    db_.impl_->put_meta(batch_, key, *meta);
  }
  update_meta_cache(key, *meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::lrem(std::string_view key, int64_t count, std::string_view value) -> Pipeline& {
  auto meta = get_meta_cached(key);
  if (!meta) {
    return *this;
  }
  if (meta->type != DataType::List) {
    set_error(ErrorCode::WrongType);
    return *this;
  }
  if (meta->size == 0) {
    return *this;
  }

  uint64_t const limit = (count == 0) ? std::numeric_limits<uint64_t>::max() : static_cast<uint64_t>(std::abs(count));
  std::vector<uint64_t> to_delete;
  uint64_t removed = 0;

  auto const pfx = encode_list_prefix(key, meta->version);
  rocksdb::Slice const pfx_slice(pfx);
  auto const seq_offset = 1 + key.size() + 8;

  rocksdb::ReadOptions iter_opts;
  iter_opts.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(db_.impl_->db->NewIterator(iter_opts));

  if (count >= 0) {
    // 先頭からスキャン
    for (it->Seek(pfx_slice); it->Valid() && removed < limit; it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      if (it->value().ToString() == value) {
        auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
        to_delete.push_back(seq);
        removed++;
      }
    }
  } else {
    // 末尾からスキャン（全件取得して逆順処理）
    std::vector<std::pair<uint64_t, std::string>> all;
    for (it->Seek(pfx_slice); it->Valid(); it->Next()) {
      auto const k = it->key();
      if (!k.starts_with(pfx_slice)) {
        break;
      }
      auto const seq = read_u64be(reinterpret_cast<uint8_t const*>(k.data() + seq_offset));
      all.emplace_back(seq, it->value().ToString());
    }
    for (auto rit = all.rbegin(); rit != all.rend() && removed < limit; ++rit) {
      if (rit->second == value) {
        to_delete.push_back(rit->first);
        removed++;
      }
    }
  }

  if (to_delete.empty()) {
    return *this;
  }

  for (auto const seq : to_delete) {
    batch_.Delete(encode_list_key(key, meta->version, seq));
  }

  meta->size -= removed;
  if (meta->size == 0) {
    batch_.Delete(encode_meta_key(key));
  } else {
    db_.impl_->put_meta(batch_, key, *meta);
  }
  update_meta_cache(key, *meta);
  return *this;
}

auto EmbeddedRedis::Pipeline::ltrim(std::string_view key, int64_t start, int64_t stop) -> Pipeline& {
  auto meta = get_meta_cached(key);
  if (!meta) {
    return *this;
  }
  if (meta->type != DataType::List) {
    set_error(ErrorCode::WrongType);
    return *this;
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
    db_.impl_->erase_key_data(batch_, key, *meta);
    meta_cache_.erase(std::string(key));
  } else {
    auto const head = static_cast<int64_t>(meta->head_seq);
    auto const new_head = static_cast<uint64_t>(head + start);
    auto const new_tail = static_cast<uint64_t>(head + stop + 1); // tail_seq は次の位置
    auto const new_size = static_cast<uint64_t>(stop - start + 1);

    // 範囲外を削除
    for (uint64_t seq = meta->head_seq; seq < new_head; ++seq) {
      batch_.Delete(encode_list_key(key, meta->version, seq));
    }
    for (uint64_t seq = new_tail; seq < meta->tail_seq; ++seq) {
      batch_.Delete(encode_list_key(key, meta->version, seq));
    }

    meta->head_seq = new_head;
    meta->tail_seq = new_tail;
    meta->size     = new_size;
    db_.impl_->put_meta(batch_, key, *meta);
    update_meta_cache(key, *meta);
  }
  return *this;
}

auto EmbeddedRedis::Pipeline::exec() -> Result<void> {
  if (error_) {
    // Clear pending operations to avoid reuse re-attempting the same batch
    auto const e = *error_;
    clear();
    return std::unexpected(e);
  }
  if (!db_.is_open()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  if (!db_.impl_->db->Write(db_.impl_->write_opts, &batch_).ok()) {
    return std::unexpected(ErrorCode::RocksDBError);
  }
  clear();
  return {};
}

void EmbeddedRedis::Pipeline::clear() {
  batch_.Clear();
  meta_cache_.clear();
  error_.reset();
}

} // namespace redismm
