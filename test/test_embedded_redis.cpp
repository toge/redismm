#include "catch2/catch_all.hpp"
#include "redismm/EmbeddedRedis.hpp"

#include <chrono>
#include <filesystem>
#include <thread>
#include <cstdio>

static int startup_checker() {
  std::puts("ALIVE: static init done");
  std::fflush(stdout);
  return 0;
}
static int const g_checker = startup_checker();

struct DbFixture {
  std::filesystem::path  path;
  redismm::EmbeddedRedis db;

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
  std::puts("FIRST_TEST_BODY");
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
  REQUIRE(*r == true);

  r = db.hset("h", "f", "v2");
  REQUIRE(r.has_value());
  REQUIRE(*r == false);

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
  auto r = db.hset("s", "f", "v");
  REQUIRE_FALSE(r.has_value());
  REQUIRE(r.error() == redismm::ErrorCode::WrongType);
}

// ---- Lists ----

TEST_CASE_METHOD(DbFixture, "List lpush/lrange") {
  REQUIRE(db.lpush("lst", "c").has_value());
  REQUIRE(db.lpush("lst", "b").has_value());
  REQUIRE(db.lpush("lst", "a").has_value());

  auto r = db.lrange("lst", 0, -1);
  REQUIRE(r.has_value());
  REQUIRE(*r == std::vector<std::string>{"a", "b", "c"});
}

TEST_CASE_METHOD(DbFixture, "List rpush") {
  REQUIRE(db.rpush("lst", "a").has_value());
  REQUIRE(db.rpush("lst", "b").has_value());
  REQUIRE(db.rpush("lst", "c").has_value());

  auto r = db.lrange("lst", 0, -1);
  REQUIRE(r.has_value());
  REQUIRE(*r == std::vector<std::string>{"a", "b", "c"});
}

TEST_CASE_METHOD(DbFixture, "List lpop") {
  REQUIRE(db.rpush("lst", "a").has_value());
  REQUIRE(db.rpush("lst", "b").has_value());
  REQUIRE(db.rpush("lst", "c").has_value());

  auto v = db.lpop("lst");
  REQUIRE(v.has_value());
  REQUIRE(*v == "a");

  v = db.lpop("lst");
  REQUIRE(v.has_value());
  REQUIRE(*v == "b");
}

TEST_CASE_METHOD(DbFixture, "List llen") {
  REQUIRE(db.rpush("lst", "x").has_value());
  REQUIRE(db.rpush("lst", "y").has_value());
  REQUIRE(db.rpush("lst", "z").has_value());

  auto n = db.llen("lst");
  REQUIRE(n.has_value());
  REQUIRE(*n == 3);
}

TEST_CASE_METHOD(DbFixture, "List ltrim") {
  REQUIRE(db.rpush("lst", "a").has_value());
  REQUIRE(db.rpush("lst", "b").has_value());
  REQUIRE(db.rpush("lst", "c").has_value());
  REQUIRE(db.rpush("lst", "d").has_value());
  REQUIRE(db.rpush("lst", "e").has_value());

  REQUIRE(db.ltrim("lst", 1, 3).has_value());

  auto r = db.lrange("lst", 0, -1);
  REQUIRE(r.has_value());
  REQUIRE(*r == std::vector<std::string>{"b", "c", "d"});
}

TEST_CASE_METHOD(DbFixture, "List rpop") {
  REQUIRE(db.rpush("lst", "a").has_value());
  REQUIRE(db.rpush("lst", "b").has_value());
  REQUIRE(db.rpush("lst", "c").has_value());

  auto v = db.rpop("lst");
  REQUIRE(v.has_value());
  REQUIRE(*v == "c");

  v = db.rpop("lst");
  REQUIRE(v.has_value());
  REQUIRE(*v == "b");
}

// ---- Sets ----

TEST_CASE_METHOD(DbFixture, "Set sadd/smembers") {
  REQUIRE(db.sadd("s", "a").has_value());
  REQUIRE(db.sadd("s", "b").has_value());
  REQUIRE(db.sadd("s", "c").has_value());

  auto m = db.smembers("s");
  REQUIRE(m.has_value());
  REQUIRE(m->size() == 3);
}

TEST_CASE_METHOD(DbFixture, "Set srem") {
  REQUIRE(db.sadd("s", "a").has_value());
  REQUIRE(db.sadd("s", "b").has_value());

  REQUIRE(db.srem("s", "a").has_value());
  auto m = db.smembers("s");
  REQUIRE(m.has_value());
  REQUIRE(m->size() == 1);
}

TEST_CASE_METHOD(DbFixture, "Set sismember") {
  REQUIRE(db.sadd("s", "a").has_value());
  REQUIRE(db.sismember("s", "a").has_value());
  REQUIRE_FALSE(db.sismember("s", "z").has_value());
}

TEST_CASE_METHOD(DbFixture, "Set scard") {
  REQUIRE(db.sadd("s", "a").has_value());
  REQUIRE(db.sadd("s", "b").has_value());

  auto n = db.scard("s");
  REQUIRE(n.has_value());
  REQUIRE(*n == 2);
}

TEST_CASE_METHOD(DbFixture, "Set sunion") {
  REQUIRE(db.sadd("s1", "a").has_value());
  REQUIRE(db.sadd("s1", "b").has_value());
  REQUIRE(db.sadd("s2", "b").has_value());
  REQUIRE(db.sadd("s2", "c").has_value());

  auto union_set = db.sunion({"s1", "s2"});
  REQUIRE(union_set.has_value());
  REQUIRE(union_set->size() == 3);
  REQUIRE(union_set->count("a") == 1);
  REQUIRE(union_set->count("b") == 1);
  REQUIRE(union_set->count("c") == 1);
}

// ---- Sorted Sets ----

TEST_CASE_METHOD(DbFixture, "ZSet zadd/zrange") {
  REQUIRE(db.zadd("z", 1.0, "a").has_value());
  REQUIRE(db.zadd("z", 3.0, "c").has_value());
  REQUIRE(db.zadd("z", 2.0, "b").has_value());

  auto r = db.zrange("z", 0, -1);
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 3);
  REQUIRE(r->at(0).member == "a");
  REQUIRE(r->at(2).member == "c");
}

TEST_CASE_METHOD(DbFixture, "ZSet zadd update score") {
  REQUIRE(db.zadd("z", 1.0, "k").has_value());
  REQUIRE(db.zadd("z", 99.0, "k").has_value());

  auto r = db.zrange("z", 0, -1);
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 1);
  REQUIRE(r->at(0).member == "k");
  REQUIRE(r->at(0).score == 99.0);
}

TEST_CASE_METHOD(DbFixture, "ZSet zrange by score") {
  REQUIRE(db.zadd("z", 1.0, "a").has_value());
  REQUIRE(db.zadd("z", 2.0, "b").has_value());
  REQUIRE(db.zadd("z", 3.0, "c").has_value());
  REQUIRE(db.zadd("z", 4.0, "d").has_value());

  auto r = db.zrangebyscore("z", 2.0, 3.0);
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 2);
}

// ---- Expiry / TTL ----

TEST_CASE_METHOD(DbFixture, "Expire ttl") {
  auto constexpr ms = std::chrono::milliseconds(50);
  REQUIRE(db.set("k", "hello", ms).has_value());

  auto t = db.ttl("k");
  REQUIRE(t.has_value());
  REQUIRE(*t > 0);
  REQUIRE(*t <= 50);
}

TEST_CASE_METHOD(DbFixture, "Expire after ttl passes") {
  auto constexpr ms = std::chrono::milliseconds(10);
  REQUIRE(db.set("k", "hello", ms).has_value());

  std::this_thread::sleep_for(ms * 3);

  auto v = db.get("k");
  REQUIRE_FALSE(v.has_value());
  REQUIRE(v.error() == redismm::ErrorCode::NotFound);
}

TEST_CASE_METHOD(DbFixture, "Expire persist") {
  auto constexpr ms = std::chrono::milliseconds(10);
  REQUIRE(db.set("k", "hello", ms).has_value());

  REQUIRE(db.persist("k").has_value());
  REQUIRE(db.ttl("k").has_value());
  REQUIRE(*(db.ttl("k")) == -1);
}

TEST_CASE_METHOD(DbFixture, "Expire pexpire") {
  REQUIRE(db.set("k", "hello").has_value());
  REQUIRE(db.pexpire("k", 100).has_value());

  auto t = db.ttl("k");
  REQUIRE(t.has_value());
  REQUIRE(*t > 0);
  REQUIRE(*t <= 100);
}
