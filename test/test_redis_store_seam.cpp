#include "catch2/catch_all.hpp"
#include "redismm/RedisStore.hpp"

#include <filesystem>

// RedisStore concept を満たすことをコンパイル時に保証する
static_assert(redismm::RedisStore<redismm::EmbeddedRedis>);
// 無関係な型が concept を満たさないことも確認する
static_assert(!redismm::RedisStore<int>);

/** @brief make_store 経由で組み込みバックエンドを構築するフィクスチャ */
struct SeamFixture {
  std::filesystem::path  path; ///< テスト用 DB パス
  redismm::EmbeddedRedis db;   ///< make_store() で生成したインスタンス

  SeamFixture() : path(make_path()), db(redismm::make_store(redismm::EmbeddedConfig{path.string()})) {}

  /** @brief フレッシュなテンポラリパスを返す */
  static std::filesystem::path make_path() {
    auto p = std::filesystem::temp_directory_path() / "redismm_seam_test";
    std::filesystem::remove_all(p);
    return p;
  }
};

/**
 * @brief バックエンド非依存の共通ワークロード
 *
 * @details ユーザーが書くコードの形そのもの。RedisStore concept を満たす
 *   任意のバックエンド（現状: EmbeddedRedis、将来: RedisClient）で動作する。
 *
 * @tparam S RedisStore を満たすバックエンド型
 * @param db バックエンドインスタンス
 */
template <typename S>
void exercise(S& db) {
  // Strings
  REQUIRE(db.set("name", "Alice").has_value());
  auto name = db.get("name");
  REQUIRE(name.has_value());
  REQUIRE(*name == "Alice");

  // Hashes
  std::ignore = db.hset("user:1", "name", "Bob");
  std::ignore = db.hset("user:1", "age", "30");
  auto all = db.hgetall("user:1");
  REQUIRE(all.has_value());
  REQUIRE(all->size() == 2);
  REQUIRE(all->at("age") == "30");

  // Lists
  std::ignore = db.rpush("queue", "task1");
  std::ignore = db.rpush("queue", "task2");
  auto list = db.lrange("queue", 0, -1);
  REQUIRE(list.has_value());
  REQUIRE(list->size() == 2);

  // Sets
  std::ignore = db.sadd("tags", "cpp");
  std::ignore = db.sadd("tags", "rust");
  auto tags = db.smembers("tags");
  REQUIRE(tags.has_value());
  REQUIRE(tags->size() == 2);

  // Sorted Sets
  std::ignore = db.zadd("scores", 100.0, "player1");
  std::ignore = db.zadd("scores", 85.5, "player2");
  auto top = db.zrangebyscore("scores", 80.0, 100.0);
  REQUIRE(top.has_value());
  REQUIRE(top->size() == 2);

  // Streams
  auto id = db.xadd("events", "*", {{"type", "login"}, {"user", "alice"}});
  REQUIRE(id.has_value());
  auto entries = db.xrange("events", "-", "+");
  REQUIRE(entries.has_value());
  REQUIRE(entries->size() == 1);

  // 有効期限
  REQUIRE(db.expire("name", 60).has_value());
  auto ttl = db.ttl("name");
  REQUIRE(ttl.has_value());
  REQUIRE(*ttl > 0);

  // 汎用操作
  REQUIRE(db.exists("name").has_value());
  REQUIRE(db.del("name").has_value());

  // WrongType: Set キーを String として読むと WrongType
  std::ignore = db.sadd("set_key", "a");
  auto wrong = db.get("set_key");
  REQUIRE_FALSE(wrong.has_value());
  REQUIRE(wrong.error() == redismm::ErrorCode::WrongType);

  // Pipeline
  auto pipe = db.pipeline();
  pipe.set("pkey", "pval")
      .hset("phash", "f", "v")
      .sadd("pset", "m");
  REQUIRE(pipe.exec().has_value());
  auto pv = db.get("pkey");
  REQUIRE(pv.has_value());
  REQUIRE(*pv == "pval");
}

TEST_CASE_METHOD(SeamFixture, "backend-agnostic workload runs on EmbeddedRedis") {
  exercise(db);
}
