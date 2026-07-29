#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace redismm {

/** @brief 操作エラーを表す列挙型 */
enum class ErrorCode {
  NotFound,        ///< キーが存在しない
  WrongType,       ///< 操作とキーのデータ型が一致しない
  RocksDBError,    ///< RocksDB の入出力エラー
  InvalidArgument, ///< 引数が不正（Stream ID のフォーマット違反など）
};

/** @brief エラーコードを伴う戻り値型 */
template <typename T>
using Result = std::expected<T, ErrorCode>;

/**
 * @brief Redis 互換のインメモリ操作を提供する組み込み Key-Value ストア
 * @details RocksDB をストレージエンジンとして使用し、文字列・ハッシュ・リスト・セット・
 *   ソート済みセット・ストリームの 6 データ型をサポートする。
 *   コンストラクタで DB を開き、失敗時は is_open() で検出可能。
 */
class EmbeddedRedis {
public:
  /**
   * @brief RocksDB データベースを開く
   *
   * @param db_path データベースファイルのパス
   * @note 失敗しても例外は投げず、is_open() が false を返す
   */
  explicit EmbeddedRedis(std::string_view db_path);
  ~EmbeddedRedis();

  EmbeddedRedis(EmbeddedRedis const&)            = delete;
  EmbeddedRedis& operator=(EmbeddedRedis const&) = delete;
  EmbeddedRedis(EmbeddedRedis&&) noexcept;
  EmbeddedRedis& operator=(EmbeddedRedis&&) noexcept;

  /**
   * @brief DB が正常に開かれているか確認する
   *
   * @return DB が利用可能なら true
   */
  [[nodiscard]] bool is_open() const noexcept;

  // ---- Strings ----

  /**
   * @brief 文字列値を格納する
   *
   * @param key   キー
   * @param value 値
   * @param ttl_ms 有効期限（ミリ秒）。nullopt で無期限
   */
  auto set(std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms = std::nullopt) -> Result<void>;

  /**
   * @brief 文字列値を取得する
   *
   * @param key キー
   * @return 値。キーが存在しない場合は NotFound
   */
  auto get(std::string_view key) -> Result<std::string>;

  // ---- Hashes ----

  /**
   * @brief ハッシュのフィールドに値を設定する
   *
   * @param key   キー
   * @param field フィールド名
   * @param value 値
   * @return 新規フィールドなら true、更新なら false
   */
  auto hset(std::string_view key, std::string_view field, std::string_view value) -> Result<bool>;

  /**
   * @brief ハッシュのフィールド値を取得する
   *
   * @param key   キー
   * @param field フィールド名
   * @return フィールド値。キーまたはフィールドが存在しない場合は NotFound
   */
  auto hget(std::string_view key, std::string_view field) -> Result<std::string>;

  /**
   * @brief ハッシュの全フィールドを取得する
   *
   * @param key キー
   * @return フィールド名→値のマップ
   */
  auto hgetall(std::string_view key) -> Result<std::unordered_map<std::string, std::string>>;

  // ---- Lists ----

  /**
   * @brief リストの先頭に要素を追加する
   *
   * @param key   キー
   * @param value 値
   * @return 追加後のリストサイズ
   */
  auto lpush(std::string_view key, std::string_view value) -> Result<uint64_t>;

  /**
   * @brief リストの末尾に要素を追加する
   *
   * @param key   キー
   * @param value 値
   * @return 追加後のリストサイズ
   */
  auto rpush(std::string_view key, std::string_view value) -> Result<uint64_t>;

  /**
   * @brief リストの先頭要素を削除して取得する
   *
   * @param key キー
   * @return 先頭要素。空リストの場合は NotFound
   */
  auto lpop(std::string_view key) -> Result<std::string>;

  /**
   * @brief リストの末尾要素を削除して取得する
   *
   * @param key キー
   * @return 末尾要素。空リストの場合は NotFound
   */
  auto rpop(std::string_view key) -> Result<std::string>;

  // ---- Sets ----

  /**
   * @brief セットにメンバーを追加する
   *
   * @param key    キー
   * @param member メンバー
   * @return 新規メンバーなら true、既存なら false
   */
  auto sadd(std::string_view key, std::string_view member) -> Result<bool>;

  /**
   * @brief セットの全メンバーを取得する
   *
   * @param key キー
   * @return メンバー一覧
   */
  auto smembers(std::string_view key) -> Result<std::vector<std::string>>;

  // ---- Sorted Sets ----

  /**
   * @brief ソート済みセットにスコア付きメンバーを追加する
   *
   * @param key    キー
   * @param score  スコア
   * @param member メンバー
   * @return 新規メンバーなら true、スコア更新なら false
   */
  auto zadd(std::string_view key, double score, std::string_view member) -> Result<bool>;

  /**
   * @brief スコア範囲でメンバーを取得する（昇順）
   *
   * @param key       キー
   * @param min_score 最小スコア（含む）
   * @param max_score 最大スコア（含む）
   * @return メンバー一覧
   */
  auto zrangebyscore(std::string_view key, double min_score, double max_score) -> Result<std::vector<std::string>>;

  // ---- Streams ----

  /**
   * @brief ストリームにエントリを追加する
   *
   * @param key    キー
   * @param id     エントリ ID。"*" で自動生成、"ms-seq" 形式で明示指定
   * @param fields フィールド名と値のペア配列
   * @return 生成された "ms-seq" 形式の ID
   */
  auto xadd(std::string_view key, std::string_view id, std::vector<std::pair<std::string, std::string>> const& fields) -> Result<std::string>;

  // ---- Generic ----

  /**
   * @brief キーとそのデータを削除する
   *
   * @param key キー
   * @return 削除されたら true、存在しなければ false
   */
  auto del(std::string_view key) -> Result<bool>;

  /**
   * @brief キーの存在確認
   *
   * @param key キー
   * @return 存在すれば true
   */
  auto exists(std::string_view key) -> Result<bool>;

  // ---- Expiration ----

  /**
   * @brief キーに有効期限を設定する（秒）
   *
   * @param key キー
   * @param seconds 有効期限（秒）
   * @return 設定に成功すれば true。キーが存在しなければ false
   */
  auto expire(std::string_view key, uint64_t seconds) -> Result<bool>;

  /**
   * @brief キーの残り有効期限を取得する（秒）
   *
   * @param key キー
   * @return 残り秒数。キーが存在しない・TTL 未設定なら -1。期限切れなら -2
   */
  auto ttl(std::string_view key) -> Result<int64_t>;

  /**
   * @brief キーに有効期限を設定する（ミリ秒）
   *
   * @param key キー
   * @param milliseconds 有効期限（ミリ秒）
   * @return 設定に成功すれば true。キーが存在しなければ false
   */
  auto pexpire(std::string_view key, uint64_t milliseconds) -> Result<bool>;

  /**
   * @brief キーの残り有効期限を取得する（ミリ秒）
   *
   * @param key キー
   * @return 残りミリ秒数。キーが存在しない・TTL 未設定なら -1。期限切れなら -2
   */
  auto pttl(std::string_view key) -> Result<int64_t>;

  /**
   * @brief キーの有効期限を削除する
   *
   * @param key キー
   * @return 削除に成功すれば true。キーが存在しない・TTL 未設定なら false
   */
  auto persist(std::string_view key) -> Result<bool>;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace redismm
