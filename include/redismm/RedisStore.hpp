#pragma once

#include <concepts>
#include <string>

#include "redismm/EmbeddedRedis.hpp"

namespace redismm {

/**
 * @brief バックエンド実装が満たすべきコマンド契約（静的ポリモーフィズム）
 *
 * @details RedisStore 以下の各 concept は、EmbeddedRedis と将来のネットワーククライアント
 *   （例: redis-plus-plus ベースの RedisClient）が同じシグネチャを持つことをコンパイル時に検証する。
 *   戻り値は厳密に `std::same_as<Result<T>>` を要求する。ユーザーコードは `Result<T>` /
 *   `ErrorCode` を明示的に命名するため、正確一致が「同じソースが両バックエンドで動く」保証になる。
 *
 * @note バックエンドの切り替えは `using Store = ...` と `make_store()` の 1 箇所だけで行える。
 *   アプリ本体はテンプレート化せず、非テンプレートのまま `Store` を使う。
 */
// clang-format off

// 戻り値型と引数呼び出しを 1 行で検査するマクロ。
// 型にカンマが含まれる場合（hgetall のみ）はマクロを使わず直接記述する。
#define REDISMM_REQUIRES_RESULT(R, ...) \
  { s.__VA_ARGS__ } -> std::same_as<Result<R>>

/** @brief Strings 系コマンドの契約 */
template <typename S>
concept RedisStrings = requires(
    S& s, std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms,
    int64_t i, uint64_t u, double d) {
  { s.set(key, value, ttl_ms) } -> std::same_as<Result<void>>;
  REDISMM_REQUIRES_RESULT(std::string, get(key));
  REDISMM_REQUIRES_RESULT(std::uint64_t, append(key, value));
  REDISMM_REQUIRES_RESULT(std::int64_t, incr(key));
  REDISMM_REQUIRES_RESULT(std::int64_t, decr(key));
  REDISMM_REQUIRES_RESULT(std::int64_t, incrby(key, i));
  REDISMM_REQUIRES_RESULT(std::int64_t, decrby(key, i));
  REDISMM_REQUIRES_RESULT(std::uint64_t, strlen(key));
  REDISMM_REQUIRES_RESULT(std::string, getset(key, value));
  REDISMM_REQUIRES_RESULT(bool, setnx(key, value));
  REDISMM_REQUIRES_RESULT(double, incrbyfloat(key, d));
  REDISMM_REQUIRES_RESULT(std::string, getrange(key, i, i));
  REDISMM_REQUIRES_RESULT(std::uint64_t, setrange(key, u, value));
};

/** @brief Hashes 系コマンドの契約 */
template <typename S>
concept RedisHashes = requires(
    S& s, std::string_view key, std::string_view field, std::string_view value,
    int64_t i, std::vector<std::string_view> const& fields) {
  REDISMM_REQUIRES_RESULT(bool, hset(key, field, value));
  REDISMM_REQUIRES_RESULT(std::string, hget(key, field));
  { s.hgetall(key) } -> std::same_as<Result<std::unordered_map<std::string, std::string>>>;
  REDISMM_REQUIRES_RESULT(bool, hdel(key, field));
  REDISMM_REQUIRES_RESULT(bool, hexists(key, field));
  REDISMM_REQUIRES_RESULT(std::uint64_t, hlen(key));
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, hkeys(key));
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, hvals(key));
  REDISMM_REQUIRES_RESULT(bool, hsetnx(key, field, value));
  REDISMM_REQUIRES_RESULT(std::int64_t, hincrby(key, field, i));
  REDISMM_REQUIRES_RESULT(std::vector<std::optional<std::string>>, hmget(key, fields));
  REDISMM_REQUIRES_RESULT(std::uint64_t, hstrlen(key, field));
  REDISMM_REQUIRES_RESULT(std::string, hrandfield(key));
};

/** @brief Lists 系コマンドの契約 */
template <typename S>
concept RedisLists = requires(
    S& s, std::string_view key, std::string_view value, std::string_view source,
    std::string_view destination, std::string_view element, std::string_view pivot,
    InsertPosition pos, ListSide from, ListSide to,
    int64_t start, int64_t stop, int64_t count, int64_t index) {
  REDISMM_REQUIRES_RESULT(std::uint64_t, lpush(key, value));
  REDISMM_REQUIRES_RESULT(std::uint64_t, rpush(key, value));
  REDISMM_REQUIRES_RESULT(std::string, lpop(key));
  REDISMM_REQUIRES_RESULT(std::string, rpop(key));
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, lrange(key, start, stop));
  REDISMM_REQUIRES_RESULT(std::uint64_t, lrem(key, count, value));
  REDISMM_REQUIRES_RESULT(void, ltrim(key, start, stop));
  REDISMM_REQUIRES_RESULT(std::uint64_t, llen(key));
  REDISMM_REQUIRES_RESULT(std::string, lindex(key, index));
  REDISMM_REQUIRES_RESULT(void, lset(key, index, value));
  REDISMM_REQUIRES_RESULT(std::int64_t, lpos(key, element));
  REDISMM_REQUIRES_RESULT(std::uint64_t, lpushx(key, value));
  REDISMM_REQUIRES_RESULT(std::uint64_t, rpushx(key, value));
  REDISMM_REQUIRES_RESULT(std::uint64_t, linsert(key, pos, pivot, value));
  REDISMM_REQUIRES_RESULT(std::string, lmove(source, destination, from, to));
  REDISMM_REQUIRES_RESULT(std::string, rpoplpush(source, destination));
};

/** @brief Sets 系コマンドの契約 */
template <typename S>
concept RedisSets = requires(
    S& s, std::string_view key, std::string_view member, std::string_view source,
    std::string_view destination) {
  REDISMM_REQUIRES_RESULT(bool, sadd(key, member));
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, smembers(key));
  REDISMM_REQUIRES_RESULT(bool, srem(key, member));
  REDISMM_REQUIRES_RESULT(std::uint64_t, scard(key));
  REDISMM_REQUIRES_RESULT(bool, sismember(key, member));
  REDISMM_REQUIRES_RESULT(std::string, spop(key));
  REDISMM_REQUIRES_RESULT(std::string, srandmember(key));
  REDISMM_REQUIRES_RESULT(bool, smove(source, destination, member));
};

/** @brief Sorted Sets 系コマンドの契約 */
template <typename S>
concept RedisSortedSets = requires(
    S& s, std::string_view key, std::string_view member, std::string_view lexmin,
    std::string_view lexmax, double min_score, double max_score, double delta,
    std::vector<std::string_view> const& members,
    int64_t start, int64_t stop) {
  REDISMM_REQUIRES_RESULT(bool, zadd(key, min_score, member));
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, zrangebyscore(key, min_score, max_score));
  REDISMM_REQUIRES_RESULT(bool, zrem(key, member));
  REDISMM_REQUIRES_RESULT(std::uint64_t, zcard(key));
  REDISMM_REQUIRES_RESULT(std::uint64_t, zcount(key, min_score, max_score));
  REDISMM_REQUIRES_RESULT(double, zscore(key, member));
  REDISMM_REQUIRES_RESULT(std::int64_t, zrank(key, member));
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, zrange(key, start, stop));
  REDISMM_REQUIRES_RESULT(double, zincrby(key, member, delta));
  REDISMM_REQUIRES_RESULT(std::int64_t, zrevrank(key, member));
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, zrevrange(key, start, stop));
  REDISMM_REQUIRES_RESULT(std::string, zpopmin(key));
  REDISMM_REQUIRES_RESULT(std::string, zpopmax(key));
  REDISMM_REQUIRES_RESULT(std::vector<std::optional<double>>, zmscore(key, members));
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, zrangebylex(key, lexmin, lexmax));
  REDISMM_REQUIRES_RESULT(std::uint64_t, zlexcount(key, lexmin, lexmax));
  REDISMM_REQUIRES_RESULT(std::uint64_t, zremrangebyrank(key, start, stop));
  REDISMM_REQUIRES_RESULT(std::uint64_t, zremrangebyscore(key, min_score, max_score));
};

/** @brief Streams 系コマンドの契約 */
template <typename S>
concept RedisStreams = requires(
    S& s, std::string_view key, std::string_view id, std::string_view start,
    std::string_view end, std::uint64_t maxlen,
    std::vector<std::pair<std::string, std::string>> const& fields,
    std::vector<std::string_view> const& ids) {
  REDISMM_REQUIRES_RESULT(std::string, xadd(key, id, fields));
  REDISMM_REQUIRES_RESULT(std::uint64_t, xlen(key));
  REDISMM_REQUIRES_RESULT(std::uint64_t, xdel(key, ids));
  REDISMM_REQUIRES_RESULT(std::vector<StreamEntry>, xrange(key, start, end));
  REDISMM_REQUIRES_RESULT(std::vector<StreamEntry>, xrevrange(key, start, end));
  REDISMM_REQUIRES_RESULT(std::uint64_t, xtrim(key, maxlen));
};

/** @brief 汎用・キー操作・有効期限系コマンドの契約 */
template <typename S>
concept RedisKeyspace = requires(
    S& s, std::string_view key, std::string_view pattern, std::uint64_t unix_time,
    std::uint64_t seconds, std::uint64_t milliseconds) {
  REDISMM_REQUIRES_RESULT(bool, del(key));
  REDISMM_REQUIRES_RESULT(bool, exists(key));
  REDISMM_REQUIRES_RESULT(std::string, type(key));
  REDISMM_REQUIRES_RESULT(bool, expireat(key, unix_time));
  REDISMM_REQUIRES_RESULT(bool, pexpireat(key, unix_time));
  REDISMM_REQUIRES_RESULT(bool, touch(key));
  REDISMM_REQUIRES_RESULT(bool, expire(key, seconds));
  REDISMM_REQUIRES_RESULT(std::int64_t, ttl(key));
  REDISMM_REQUIRES_RESULT(bool, pexpire(key, milliseconds));
  REDISMM_REQUIRES_RESULT(std::int64_t, pttl(key));
  REDISMM_REQUIRES_RESULT(bool, persist(key));
  REDISMM_REQUIRES_RESULT(std::string, randomkey());
  REDISMM_REQUIRES_RESULT(std::vector<std::string>, keys(pattern));
};

/**
 * @brief Redis サブセットの全コマンドを提供するバックエンドの契約
 *
 * @details ライフサイクル（is_open）と Pipeline 生成も契約に含める。
 *   `pipeline()` は `Pipeline<S>`（バックエンド固有の特殊化）を返す。
 */
template <typename S>
concept RedisStore = requires(S& s) {
  { s.is_open() } -> std::same_as<bool>;
  { s.pipeline() } -> std::same_as<Pipeline<S>>;
  requires RedisStrings<S>;
  requires RedisHashes<S>;
  requires RedisLists<S>;
  requires RedisSets<S>;
  requires RedisSortedSets<S>;
  requires RedisStreams<S>;
  requires RedisKeyspace<S>;
};

#undef REDISMM_REQUIRES_RESULT

// clang-format on

// 組み込みバックエンドが契約を満たすことを保証する（満たさなければコンパイルエラー）
static_assert(RedisStore<EmbeddedRedis>, "EmbeddedRedis must satisfy RedisStore");
static_assert(!RedisStore<int>, "RedisStore must reject unrelated types");

/**
 * @brief 組み込みバックエンドの接続情報
 * @details 将来のネットワークバックエンドは RedisConfig { host; port; } を追加し、
 *   make_store(RedisConfig) オーバーロードを提供する。
 */
struct EmbeddedConfig {
  std::string path; ///< RocksDB データベースファイルのパス
};

/**
 * @brief 組み込みバックエンドを構築する
 *
 * @param cfg 接続情報
 * @return 構築済みの EmbeddedRedis（失敗時は is_open() が false）
 */
inline auto make_store(EmbeddedConfig const& cfg) -> EmbeddedRedis {
  return EmbeddedRedis(cfg.path);
}

} // namespace redismm
