#include "catch2/catch_all.hpp"
#include "redismm/EmbeddedRedis.hpp"

#include <chrono>
#include <filesystem>
#include <thread>

/** @brief テストごとに一時ディレクトリを払い出すフィクスチャ */
struct DbFixture {
  std::filesystem::path  path; ///< テスト用 DB パス
  redismm::EmbeddedRedis db;   ///< テスト対象インスタンス

  /**
   * @brief テンポラリパスを生成し、既存ディレクトリを削除する
   *
   * @return フレッシュなパス
   */
  static std::filesystem::path fresh_path() {
    auto p = std::filesystem::temp_directory_path() / "redismm_test";
    std::filesystem::remove_all(p);
    return p;
  }

  DbFixture() : path(fresh_path()), db(path.string()) {}
  ~DbFixture() { std::filesystem::remove_all(path); }
};

// ---- Strings ----

TEST_CASE_METHOD(DbFixture, "String set/get roundtrip") {
  REQUIRE(db.set("k", "hello").has_value());
  auto v = db.get("k");
  REQUIRE(v.has_value());
  REQUIRE(*v == "hello");
}

TEST_CASE_METHOD(DbFixture, "String overwrite") {
  std::ignore = db.set("k", "first");
  std::ignore = db.set("k", "second");
  auto v      = db.get("k");
  REQUIRE(v.has_value());
  REQUIRE(*v == "second");
}

TEST_CASE_METHOD(DbFixture, "String get missing") {
  auto v = db.get("no_such");
  REQUIRE_FALSE(v.has_value());
  REQUIRE(v.error() == redismm::ErrorCode::NotFound);
}

// ---- Hashes ----

TEST_CASE_METHOD(DbFixture, "Hash hset/hget") {
  auto r = db.hset("h", "f", "v");
  REQUIRE(r.has_value());
  REQUIRE(*r == true); // new field

  r = db.hset("h", "f", "v2");
  REQUIRE(r.has_value());
  REQUIRE(*r == false); // existing field

  auto g = db.hget("h", "f");
  REQUIRE(g.has_value());
  REQUIRE(*g == "v2");
}

TEST_CASE_METHOD(DbFixture, "Hash hgetall") {
  std::ignore = db.hset("h", "a", "1");
  std::ignore = db.hset("h", "b", "2");
  std::ignore = db.hset("h", "c", "3");

  auto all = db.hgetall("h");
  REQUIRE(all.has_value());
  REQUIRE(all->size() == 3);
  REQUIRE(all->at("a") == "1");
  REQUIRE(all->at("c") == "3");
}

TEST_CASE_METHOD(DbFixture, "Hash WrongType") {
  std::ignore = db.set("s", "val");
  auto r      = db.hget("s", "f");
  REQUIRE_FALSE(r.has_value());
  REQUIRE(r.error() == redismm::ErrorCode::WrongType);
}

// ---- Lists ----

TEST_CASE_METHOD(DbFixture, "List rpush/lpop FIFO") {
  std::ignore = db.rpush("q", "a");
  std::ignore = db.rpush("q", "b");
  std::ignore = db.rpush("q", "c");

  REQUIRE(*db.lpop("q") == "a");
  REQUIRE(*db.lpop("q") == "b");
  REQUIRE(*db.lpop("q") == "c");
  REQUIRE_FALSE(db.lpop("q").has_value());
}

TEST_CASE_METHOD(DbFixture, "List lpush/rpop FILO") {
  std::ignore = db.lpush("s", "a");
  std::ignore = db.lpush("s", "b");
  std::ignore = db.lpush("s", "c");

  REQUIRE(*db.rpop("s") == "a");
  REQUIRE(*db.rpop("s") == "b");
  REQUIRE(*db.rpop("s") == "c");
}

TEST_CASE_METHOD(DbFixture, "List count") {
  auto c1 = db.rpush("l", "x");
  auto c2 = db.lpush("l", "y");
  REQUIRE(*c1 == 1);
  REQUIRE(*c2 == 2);
}

// ---- Sets ----

TEST_CASE_METHOD(DbFixture, "Set sadd/smembers") {
  REQUIRE(*db.sadd("s", "a") == true);
  REQUIRE(*db.sadd("s", "b") == true);
  REQUIRE(*db.sadd("s", "a") == false); // dup

  auto m = db.smembers("s");
  REQUIRE(m.has_value());
  REQUIRE(m->size() == 2);
}

// ---- Sorted Sets ----

TEST_CASE_METHOD(DbFixture, "ZSet zadd/zrangebyscore") {
  std::ignore = db.zadd("z", 1.0, "one");
  std::ignore = db.zadd("z", 2.0, "two");
  std::ignore = db.zadd("z", 3.0, "three");

  auto r = db.zrangebyscore("z", 1.5, 2.5);
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 1);
  REQUIRE((*r)[0] == "two");
}

TEST_CASE_METHOD(DbFixture, "ZSet score update") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 5.0, "a"); // update

  auto full = db.zrangebyscore("z", 0.0, 2.0);
  REQUIRE(full.has_value());
  REQUIRE(full->empty());

  auto updated = db.zrangebyscore("z", 4.0, 6.0);
  REQUIRE(updated.has_value());
  REQUIRE(updated->size() == 1);
}

TEST_CASE_METHOD(DbFixture, "ZSet negative scores sorted") {
  std::ignore = db.zadd("z", -3.0, "neg3");
  std::ignore = db.zadd("z", -1.0, "neg1");
  std::ignore = db.zadd("z", 0.0, "zero");
  std::ignore = db.zadd("z", 2.0, "pos2");

  auto r = db.zrangebyscore("z", -10.0, 10.0);
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 4);
  REQUIRE((*r)[0] == "neg3");
  REQUIRE((*r)[1] == "neg1");
  REQUIRE((*r)[2] == "zero");
  REQUIRE((*r)[3] == "pos2");
}

// ---- Streams ----

TEST_CASE_METHOD(DbFixture, "Stream xadd auto-id") {
  auto id1 = db.xadd("ev", "*", {{"k", "v"}});
  auto id2 = db.xadd("ev", "*", {{"k", "v2"}});
  REQUIRE(id1.has_value());
  REQUIRE(id2.has_value());
  REQUIRE(*id1 != *id2);
}

TEST_CASE_METHOD(DbFixture, "Stream xadd explicit id monotone") {
  REQUIRE(db.xadd("s", "10-0", {}).has_value());
  REQUIRE(db.xadd("s", "10-1", {}).has_value());
  REQUIRE(db.xadd("s", "11-0", {}).has_value());

  // 単調増加違反
  auto bad = db.xadd("s", "10-0", {});
  REQUIRE_FALSE(bad.has_value());
  REQUIRE(bad.error() == redismm::ErrorCode::InvalidArgument);
}

// ---- Generic ----

TEST_CASE_METHOD(DbFixture, "Generic del/exists") {
  std::ignore = db.set("x", "1");
  REQUIRE(*db.exists("x") == true);
  REQUIRE(*db.del("x") == true);
  REQUIRE(*db.exists("x") == false);
  REQUIRE(*db.del("x") == false);
}

TEST_CASE_METHOD(DbFixture, "Generic del collection") {
  std::ignore = db.rpush("lst", "a");
  std::ignore = db.rpush("lst", "b");
  REQUIRE(*db.del("lst") == true);
  REQUIRE(*db.exists("lst") == false);
}

// ---- Expiration ----

TEST_CASE_METHOD(DbFixture, "Expire expire/ttl") {
  std::ignore = db.set("k", "v");

  auto ex = db.expire("k", 10);
  REQUIRE(ex.has_value());
  REQUIRE(*ex == true);

  auto t = db.ttl("k");
  REQUIRE(t.has_value());
  REQUIRE(*t > 0);
  REQUIRE(*t <= 10);
}

TEST_CASE_METHOD(DbFixture, "Expire pexpire/pttl") {
  std::ignore = db.set("k", "v");

  auto ex = db.pexpire("k", 10000);
  REQUIRE(ex.has_value());
  REQUIRE(*ex == true);

  auto pt = db.pttl("k");
  REQUIRE(pt.has_value());
  REQUIRE(*pt > 0);
  REQUIRE(*pt <= 10000);
}

TEST_CASE_METHOD(DbFixture, "Expire persist") {
  std::ignore = db.set("k", "v");
  std::ignore = db.expire("k", 10);

  auto ex = db.persist("k");
  REQUIRE(ex.has_value());
  REQUIRE(*ex == true);

  auto t = db.ttl("k");
  REQUIRE(t.has_value());
  REQUIRE(*t == -1);
}

TEST_CASE_METHOD(DbFixture, "Expire missing key") {
  auto ex = db.expire("no_such", 10);
  REQUIRE(ex.has_value());
  REQUIRE(*ex == false);

  auto t = db.ttl("no_such");
  REQUIRE(t.has_value());
  REQUIRE(*t == -1);

  auto pt = db.pttl("no_such");
  REQUIRE(pt.has_value());
  REQUIRE(*pt == -1);

  auto p = db.persist("no_such");
  REQUIRE(p.has_value());
  REQUIRE(*p == false);
}

TEST_CASE_METHOD(DbFixture, "Expire no TTL") {
  std::ignore = db.set("k", "v");

  auto t = db.ttl("k");
  REQUIRE(t.has_value());
  REQUIRE(*t == -1);

  auto pt = db.pttl("k");
  REQUIRE(pt.has_value());
  REQUIRE(*pt == -1);

  auto p = db.persist("k");
  REQUIRE(p.has_value());
  REQUIRE(*p == false);
}

TEST_CASE_METHOD(DbFixture, "Expire expired key") {
  std::ignore = db.set("k", "v", 1); // 1ms TTL

  // 少し待機して期限切れにする
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  auto t = db.ttl("k");
  REQUIRE(t.has_value());
  // get_meta で期限切れを検出すると遅延削除されるため -1 が返る
  REQUIRE(*t == -1);

  auto v = db.get("k");
  REQUIRE_FALSE(v.has_value());
}
