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
  Busy,            ///< 操作が競合している（CAS 失敗など）
};

/** @brief エラーコードを伴う戻り値型 */
template <typename T>
using Result = std::expected<T, ErrorCode>;

/** @brief ストリームエントリ */
struct StreamEntry {
  std::string id;                                         ///< エントリID ("ms-seq")
  std::vector<std::pair<std::string, std::string>> fields; ///< フィールド名と値のペア
};

/** @brief リスト挿入位置 */
enum class InsertPosition { Before, After };

/** @brief リストの端 */
enum class ListSide { Left, Right };

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

  /**
   * @brief 文字列値を末尾に追加する
   *
   * @param key   キー
   * @param value 追加する値
   * @return 追加後の文字列長
   */
  auto append(std::string_view key, std::string_view value) -> Result<uint64_t>;

  /**
   * @brief 文字列値をインクリメントする
   *
   * @param key キー
   * @return インクリメント後の値
   */
  auto incr(std::string_view key) -> Result<int64_t>;

  /**
   * @brief 文字列値をデクリメントする
   *
   * @param key キー
   * @return デクリメント後の値
   */
  auto decr(std::string_view key) -> Result<int64_t>;

  /**
   * @brief 文字列値を指定量インクリメントする
   *
   * @param key   キー
   * @param delta 増分
   * @return インクリメント後の値
   */
  auto incrby(std::string_view key, int64_t delta) -> Result<int64_t>;

  /**
   * @brief 文字列値を指定量デクリメントする
   *
   * @param key   キー
   * @param delta 減分
   * @return デクリメント後の値
   */
  auto decrby(std::string_view key, int64_t delta) -> Result<int64_t>;

  /**
   * @brief 文字列値の長さを取得する
   *
   * @param key キー
   * @return 文字列長。キーが存在しない場合は 0
   */
  auto strlen(std::string_view key) -> Result<uint64_t>;

  /**
   * @brief 値を設定し、古い値を返す
   *
   * @param key   キー
   * @param value 新しい値
   * @return 古い値。キーが存在しない場合は NotFound
   */
  auto getset(std::string_view key, std::string_view value) -> Result<std::string>;

  /**
   * @brief キーが存在しない場合のみ設定する
   *
   * @param key   キー
   * @param value 値
   * @return 設定されたら true、既存なら false
   */
  auto setnx(std::string_view key, std::string_view value) -> Result<bool>;

  /**
   * @brief 文字列値を浮動小数点数でインクリメントする
   *
   * @param key   キー
   * @param delta 増分
   * @return インクリメント後の値
   */
  auto incrbyfloat(std::string_view key, double delta) -> Result<double>;

  /**
   * @brief 文字列値の部分範囲を取得する
   *
   * @param key   キー
   * @param start 開始オフセット（負の値は末尾から）
   * @param end   終了オフセット（負の値は末尾から、含む）
   * @return 部分文字列
   */
  auto getrange(std::string_view key, int64_t start, int64_t end) -> Result<std::string>;

  /**
   * @brief 文字列値の指定位置から上書きする
   *
   * @param key    キー
   * @param offset 開始オフセット
   * @param value  上書きする値
   * @return 設定後の文字列長
   */
  auto setrange(std::string_view key, uint64_t offset, std::string_view value) -> Result<uint64_t>;

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

  /**
   * @brief ハッシュのフィールドを削除する
   *
   * @param key   キー
   * @param field フィールド名
   * @return 削除されたら true、存在しなければ false
   */
  auto hdel(std::string_view key, std::string_view field) -> Result<bool>;

  /**
   * @brief ハッシュのフィールド存在確認
   *
   * @param key   キー
   * @param field フィールド名
   * @return 存在すれば true
   */
  auto hexists(std::string_view key, std::string_view field) -> Result<bool>;

  /**
   * @brief ハッシュのフィールド数を取得する
   *
   * @param key キー
   * @return フィールド数。キーが存在しない場合は 0
   */
  auto hlen(std::string_view key) -> Result<uint64_t>;

  /**
   * @brief ハッシュの全フィールド名を取得する
   *
   * @param key キー
   * @return フィールド名一覧
   */
  auto hkeys(std::string_view key) -> Result<std::vector<std::string>>;

  /**
   * @brief ハッシュの全フィールド値を取得する
   *
   * @param key キー
   * @return フィールド値一覧
   */
  auto hvals(std::string_view key) -> Result<std::vector<std::string>>;

  /**
   * @brief フィールドが存在しない場合のみ設定する
   *
   * @param key   キー
   * @param field フィールド名
   * @param value 値
   * @return 設定されたら true、既存なら false
   */
  auto hsetnx(std::string_view key, std::string_view field, std::string_view value) -> Result<bool>;

  /**
   * @brief ハッシュフィールドの値を整数でインクリメントする
   *
   * @param key   キー
   * @param field フィールド名
   * @param delta 増分
   * @return インクリメント後の値
   */
  auto hincrby(std::string_view key, std::string_view field, int64_t delta) -> Result<int64_t>;

  /**
   * @brief 複数フィールドの値を取得する
   *
   * @param key    キー
   * @param fields フィールド名一覧
   * @return 各フィールドの値（存在しないフィールドは nullopt）
   */
  auto hmget(std::string_view key, std::vector<std::string_view> const& fields) -> Result<std::vector<std::optional<std::string>>>;

  /**
   * @brief ハッシュフィールド値の長さを取得する
   *
   * @param key   キー
   * @param field フィールド名
   * @return フィールド値の長さ。フィールドが存在しない場合は 0
   */
  auto hstrlen(std::string_view key, std::string_view field) -> Result<uint64_t>;

  /**
   * @brief ハッシュからランダムなフィールド名を取得する
   *
   * @param key キー
   * @return ランダムなフィールド名。空の場合は NotFound
   */
  auto hrandfield(std::string_view key) -> Result<std::string>;

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

  /**
   * @brief リストの範囲内の要素を取得する
   *
   * @param key   キー
   * @param start 開始インデックス（0ベース、負の値は末尾から）
   * @param stop  終了インデックス（0ベース、負の値は末尾から、含む）
   * @return 範囲内の要素一覧
   */
  auto lrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>>;

  /**
   * @brief リストから指定値を削除する
   *
   * @param key   キー
   * @param count 削除数（正: 先頭から、負: 末尾から、0: すべて）
   * @param value 削除する値
   * @return 削除された要素数
   */
  auto lrem(std::string_view key, int64_t count, std::string_view value) -> Result<uint64_t>;

  /**
   * @brief リストを範囲でトリムする（範囲外を削除）
   *
   * @param key   キー
   * @param start 開始インデックス（0ベース、負の値は末尾から）
   * @param stop  終了インデックス（0ベース、負の値は末尾から、含む）
   * @return 成功
   */
  auto ltrim(std::string_view key, int64_t start, int64_t stop) -> Result<void>;

  /**
   * @brief リストの長さを取得する
   *
   * @param key キー
   * @return リスト長。キーが存在しない場合は 0
   */
  auto llen(std::string_view key) -> Result<uint64_t>;

  /**
   * @brief リストの指定インデックスの要素を取得する
   *
   * @param key   キー
   * @param index インデックス（0ベース、負の値は末尾から）
   * @return 要素。範囲外の場合は NotFound
   */
  auto lindex(std::string_view key, int64_t index) -> Result<std::string>;

  /**
   * @brief リストの指定インデックスに値を設定する
   *
   * @param key   キー
   * @param index インデックス
   * @param value 値
   * @return 成功
   */
  auto lset(std::string_view key, int64_t index, std::string_view value) -> Result<void>;

  /**
   * @brief リスト内の要素の位置を検索する
   *
   * @param key     キー
   * @param element 検索する値
   * @return 最初に見つかった位置。見つからない場合は NotFound
   */
  auto lpos(std::string_view key, std::string_view element) -> Result<int64_t>;

  /**
   * @brief リストが存在する場合のみ先頭に追加する
   *
   * @param key   キー
   * @param value 値
   * @return 追加後のリストサイズ。キーが存在しない場合は 0
   */
  auto lpushx(std::string_view key, std::string_view value) -> Result<uint64_t>;

  /**
   * @brief リストが存在する場合のみ末尾に追加する
   *
   * @param key   キー
   * @param value 値
   * @return 追加後のリストサイズ。キーが存在しない場合は 0
   */
  auto rpushx(std::string_view key, std::string_view value) -> Result<uint64_t>;

  /**
   * @brief ピボット値の前後に要素を挿入する
   *
   * @param key   キー
   * @param pos   挿入位置（Before または After）
   * @param pivot ピボット値
   * @param value 挿入する値
   * @return 挿入後のリストサイズ。ピボットが見つからない場合は NotFound
   */
  auto linsert(std::string_view key, InsertPosition pos, std::string_view pivot, std::string_view value) -> Result<uint64_t>;

  /**
   * @brief ソースリストから要素を取り出し、宛先リストに挿入する
   *
   * @param source      ソースキー
   * @param destination 宛先キー
   * @param from        取り出す位置
   * @param to          挿入する位置
   * @return 移動された要素。ソースが空の場合は NotFound
   */
  auto lmove(std::string_view source, std::string_view destination, ListSide from, ListSide to) -> Result<std::string>;

  /**
   * @brief ソースリストの末尾から要素を取り出し、宛先リストの先頭に挿入する
   *
   * @param source      ソースキー
   * @param destination 宛先キー
   * @return 移動された要素。ソースが空の場合は NotFound
   */
  auto rpoplpush(std::string_view source, std::string_view destination) -> Result<std::string>;

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

  /**
   * @brief セットからメンバーを削除する
   *
   * @param key    キー
   * @param member メンバー
   * @return 削除されたら true、存在しなければ false
   */
  auto srem(std::string_view key, std::string_view member) -> Result<bool>;

  /**
   * @brief セットのメンバー数を取得する
   *
   * @param key キー
   * @return メンバー数。キーが存在しない場合は 0
   */
  auto scard(std::string_view key) -> Result<uint64_t>;

  /**
   * @brief セットのメンバー存在確認
   *
   * @param key    キー
   * @param member メンバー
   * @return 存在すれば true
   */
  auto sismember(std::string_view key, std::string_view member) -> Result<bool>;

  /**
   * @brief セットからランダムなメンバーを削除して取得する
   *
   * @param key キー
   * @return 削除されたメンバー。空の場合は NotFound
   */
  auto spop(std::string_view key) -> Result<std::string>;

  /**
   * @brief セットからランダムなメンバーを取得する（削除しない）
   *
   * @param key キー
   * @return ランダムなメンバー。空の場合は NotFound
   */
  auto srandmember(std::string_view key) -> Result<std::string>;

  /**
   * @brief セット間でメンバーを移動する
   *
   * @param source      ソースキー
   * @param destination 宛先キー
   * @param member      移動するメンバー
   * @return 移動されたら true、ソースにメンバーが存在しない場合は false
   */
  auto smove(std::string_view source, std::string_view destination, std::string_view member) -> Result<bool>;

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

  /**
   * @brief ソート済みセットからメンバーを削除する
   *
   * @param key    キー
   * @param member メンバー
   * @return 削除されたら true、存在しなければ false
   */
  auto zrem(std::string_view key, std::string_view member) -> Result<bool>;

  /**
   * @brief ソート済みセットのメンバー数を取得する
   *
   * @param key キー
   * @return メンバー数。キーが存在しない場合は 0
   */
  auto zcard(std::string_view key) -> Result<uint64_t>;

  /**
   * @brief スコア範囲内のメンバー数を取得する
   *
   * @param key       キー
   * @param min_score 最小スコア（含む）
   * @param max_score 最大スコア（含む）
   * @return メンバー数
   */
  auto zcount(std::string_view key, double min_score, double max_score) -> Result<uint64_t>;

  /**
   * @brief メンバーのスコアを取得する
   *
   * @param key    キー
   * @param member メンバー
   * @return スコア。メンバーが存在しない場合は NotFound
   */
  auto zscore(std::string_view key, std::string_view member) -> Result<double>;

  /**
   * @brief メンバーのランクを取得する（昇順、0 ベース）
   *
   * @param key    キー
   * @param member メンバー
   * @return ランク。メンバーが存在しない場合は NotFound
   */
  auto zrank(std::string_view key, std::string_view member) -> Result<int64_t>;

  /**
   * @brief ランク範囲でメンバーを取得する（昇順）
   *
   * @param key   キー
   * @param start 開始ランク（0ベース、負の値は末尾から）
   * @param stop  終了ランク（0ベース、負の値は末尾から、含む）
   * @return メンバー一覧
   */
  auto zrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>>;

  /**
   * @brief メンバーのスコアを指定量インクリメントする
   *
   * @param key    キー
   * @param member メンバー
   * @param delta  増分
   * @return インクリメント後のスコア
   */
  auto zincrby(std::string_view key, std::string_view member, double delta) -> Result<double>;

  /**
   * @brief メンバーのランクを取得する（降順、0 ベース）
   *
   * @param key    キー
   * @param member メンバー
   * @return ランク。メンバーが存在しない場合は NotFound
   */
  auto zrevrank(std::string_view key, std::string_view member) -> Result<int64_t>;

  /**
   * @brief ランク範囲でメンバーを取得する（降順）
   *
   * @param key   キー
   * @param start 開始ランク（0ベース）
   * @param stop  終了ランク（0ベース、含む）
   * @return メンバー一覧
   */
  auto zrevrange(std::string_view key, int64_t start, int64_t stop) -> Result<std::vector<std::string>>;

  /**
   * @brief 最小スコアのメンバーを削除して取得する
   *
   * @param key キー
   * @return 削除されたメンバー。空の場合は NotFound
   */
  auto zpopmin(std::string_view key) -> Result<std::string>;

  /**
   * @brief 最大スコアのメンバーを削除して取得する
   *
   * @param key キー
   * @return 削除されたメンバー。空の場合は NotFound
   */
  auto zpopmax(std::string_view key) -> Result<std::string>;

  /**
   * @brief 複数メンバーのスコアを取得する
   *
   * @param key     キー
   * @param members メンバー一覧
   * @return 各メンバーのスコア（存在しないメンバーは nullopt）
   */
  auto zmscore(std::string_view key, std::vector<std::string_view> const& members) -> Result<std::vector<std::optional<double>>>;

  /**
   * @brief 辞書式順序でメンバー範囲を取得する
   *
   * @param key キー
   * @param min 最小値。"-" で -inf
   * @param max 最大値。"+" で +inf
   * @return メンバー一覧
   */
  auto zrangebylex(std::string_view key, std::string_view min, std::string_view max) -> Result<std::vector<std::string>>;

  /**
   * @brief 辞書式順序で範囲内のメンバー数をカウントする
   *
   * @param key キー
   * @param min 最小値。"-" で -inf
   * @param max 最大値。"+" で +inf
   * @return 範囲内のメンバー数
   */
  auto zlexcount(std::string_view key, std::string_view min, std::string_view max) -> Result<uint64_t>;

  /**
   * @brief ランク範囲でメンバーを削除する
   *
   * @param key   キー
   * @param start 開始ランク（0ベース）
   * @param stop  終了ランク（0ベース、含む）
   * @return 削除された数
   */
  auto zremrangebyrank(std::string_view key, int64_t start, int64_t stop) -> Result<uint64_t>;

  /**
   * @brief スコア範囲でメンバーを削除する
   *
   * @param key キー
   * @param min 最小スコア
   * @param max 最大スコア
   * @return 削除された数
   */
  auto zremrangebyscore(std::string_view key, double min, double max) -> Result<uint64_t>;

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

  /**
   * @brief ストリームの長さを取得する
   *
   * @param key キー
   * @return エントリ数。キーが存在しない場合は 0
   */
  auto xlen(std::string_view key) -> Result<uint64_t>;

  /**
   * @brief ストリームからエントリを削除する
   *
   * @param key キー
   * @param ids 削除するエントリ ID 一覧
   * @return 削除されたエントリ数
   */
  auto xdel(std::string_view key, std::vector<std::string_view> const& ids) -> Result<uint64_t>;

  /**
   * @brief ストリームのエントリ範囲を取得する（昇順）
   *
   * @param key   キー
   * @param start 開始ID。"-" で最小、"-" で先頭
   * @param end   終了ID。"+" で最大
   * @return エントリ一覧
   */
  auto xrange(std::string_view key, std::string_view start, std::string_view end) -> Result<std::vector<StreamEntry>>;

  /**
   * @brief ストリームのエントリ範囲を取得する（降順）
   *
   * @param key   キー
   * @param start 開始ID（降順なので大きい方）
   * @param end   終了ID（降順なので小さい方）
   * @return エントリ一覧
   */
  auto xrevrange(std::string_view key, std::string_view start, std::string_view end) -> Result<std::vector<StreamEntry>>;

  /**
   * @brief ストリームを最大長でトリムする
   *
   * @param key    キー
   * @param maxlen 最大エントリ数
   * @return 削除されたエントリ数
   */
  auto xtrim(std::string_view key, uint64_t maxlen) -> Result<uint64_t>;

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

  /**
   * @brief キーのデータ型を返す
   *
   * @param key キー
   * @return "string"、"hash"、"list"、"set"、"zset"、"stream"、"none" のいずれか
   */
  auto type(std::string_view key) -> Result<std::string>;

  /**
   * @brief キーに有効期限を設定する（Unix タイムスタンプ・秒）
   *
   * @param key       キー
   * @param unix_time Unix タイムスタップ（秒）
   * @return 設定に成功すれば true。キーが存在しなければ false
   */
  auto expireat(std::string_view key, uint64_t unix_time) -> Result<bool>;

  /**
   * @brief キーに有効期限を設定する（Unix タイムスタンプ・ミリ秒）
   *
   * @param key       キー
   * @param unix_time Unix タイムスタンプ（ミリ秒）
   * @return 設定に成功すれば true。キーが存在しなければ false
   */
  auto pexpireat(std::string_view key, uint64_t unix_time) -> Result<bool>;

  /**
   * @brief キーの最終アクセス時刻を更新する
   *
   * @param key キー
   * @return 更新に成功すれば true。キーが存在しなければ false
   */
  auto touch(std::string_view key) -> Result<bool>;

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

  /**
   * @brief ランダムなキーを取得する
   *
   * @return キー名。DB が空の場合は NotFound
   */
  auto randomkey() -> Result<std::string>;

  /**
   * @brief パターンに一致するキーを検索する
   *
   * @param pattern グロブパターン
   * @return 一致したキー一覧
   */
  auto keys(std::string_view pattern) -> Result<std::vector<std::string>>;

  // ---- Pipeline ----

  class Pipeline;

  /**
   * @brief Pipeline を作成する
   *
   * @return Pipeline インスタンス
   */
  auto pipeline() -> Pipeline;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace redismm

// ============================================================
// Pipeline
// ============================================================

#include "redismm/Encoder.hpp"

#include <functional>
#include <unordered_map>
#include <rocksdb/write_batch.h>

namespace redismm {

/**
 * @brief 複数の書き込み操作をバッチで実行する Pipeline
 *
 * @details 操作をキューイングし、exec() で一括実行する。
 *   すべての操作は単一の RocksDB WriteBatch でアトミックに実行される。
 *   同じキーへの連続操作はメタデータキャッシュにより正しく処理される。
 */
class EmbeddedRedis::Pipeline {
public:
  explicit Pipeline(EmbeddedRedis& db) : db_(db) {}
  ~Pipeline() = default;

  Pipeline(Pipeline const&)            = delete;
  Pipeline& operator=(Pipeline const&) = delete;
  Pipeline(Pipeline&&)                 = default;
  Pipeline& operator=(Pipeline&&)      = default;

  /** @brief 文字列値を設定する操作を追加 */
  Pipeline& set(std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms = std::nullopt);

  /** @brief ハッシュフィールドを設定する操作を追加 */
  Pipeline& hset(std::string_view key, std::string_view field, std::string_view value);

  /** @brief リスト先頭に追加する操作を追加 */
  Pipeline& lpush(std::string_view key, std::string_view value);

  /** @brief リスト末尾に追加する操作を追加 */
  Pipeline& rpush(std::string_view key, std::string_view value);

  /** @brief セットにメンバーを追加する操作を追加 */
  Pipeline& sadd(std::string_view key, std::string_view member);

  /** @brief セットからメンバーを削除する操作を追加 */
  Pipeline& srem(std::string_view key, std::string_view member);

  /** @brief ソート済みセットにメンバーを追加する操作を追加 */
  Pipeline& zadd(std::string_view key, double score, std::string_view member);

  /** @brief ストリームにエントリを追加する操作を追加 */
  Pipeline& xadd(std::string_view key, std::string_view id, std::vector<std::pair<std::string, std::string>> const& fields);

  /** @brief キーを削除する操作を追加 */
  Pipeline& del(std::string_view key);

  /** @brief 有効期限を設定する操作を追加（秒） */
  Pipeline& expire(std::string_view key, uint64_t seconds);

  /** @brief 有効期限を設定する操作を追加（ミリ秒） */
  Pipeline& pexpire(std::string_view key, uint64_t milliseconds);

  /** @brief 有効期限を削除する操作を追加 */
  Pipeline& persist(std::string_view key);

  /** @brief 文字列値を末尾に追加する操作を追加 */
  Pipeline& append(std::string_view key, std::string_view value);

  /** @brief ハッシュのフィールドを削除する操作を追加 */
  Pipeline& hdel(std::string_view key, std::string_view field);

  /** @brief ソート済みセットからメンバーを削除する操作を追加 */
  Pipeline& zrem(std::string_view key, std::string_view member);

  /** @brief リストから値を削除する操作を追加 */
  Pipeline& lrem(std::string_view key, int64_t count, std::string_view value);

  /** @brief リストをトリムする操作を追加 */
  Pipeline& ltrim(std::string_view key, int64_t start, int64_t stop);

  /**
   * @brief キューイングされた操作を一括実行する
   *
   * @return すべての操作が成功すれば成功
   */
  auto exec() -> Result<void>;

  /** @brief キューをクリアする */
  void clear();

private:
  EmbeddedRedis& db_;
  rocksdb::WriteBatch batch_;
  std::unordered_map<std::string, MetaValue> meta_cache_;
  std::optional<ErrorCode> error_;

  /** @brief キャッシュ付きメタデータ取得 */
  auto get_meta_cached(std::string_view key) -> std::optional<MetaValue>;

  /** @brief キャッシュを更新 */
  void update_meta_cache(std::string_view key, MetaValue const& meta);

  /** @brief Pipeline 内エラーを記録（最初のエラーのみ保持） */
  void set_error(ErrorCode e) { if (!error_) error_ = e; }
};

} // namespace redismm
