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

TEST_CASE_METHOD(DbFixture, "String append") {
  std::ignore = db.set("k", "hello");
  auto r = db.append("k", " world");
  REQUIRE(r.has_value());
  REQUIRE(*r == 11);

  auto v = db.get("k");
  REQUIRE(v.has_value());
  REQUIRE(*v == "hello world");
}

TEST_CASE_METHOD(DbFixture, "String append new key") {
  auto r = db.append("k", "hello");
  REQUIRE(r.has_value());
  REQUIRE(*r == 5);
}

TEST_CASE_METHOD(DbFixture, "String incr/decr") {
  REQUIRE(*db.incr("c") == 1);
  REQUIRE(*db.incr("c") == 2);
  REQUIRE(*db.decr("c") == 1);
  REQUIRE(*db.incrby("c", 10) == 11);
  REQUIRE(*db.decrby("c", 3) == 8);
}

TEST_CASE_METHOD(DbFixture, "String incr non-integer") {
  std::ignore = db.set("s", "abc");
  auto r = db.incr("s");
  REQUIRE_FALSE(r.has_value());
  REQUIRE(r.error() == redismm::ErrorCode::WrongType);
}

TEST_CASE_METHOD(DbFixture, "String strlen") {
  std::ignore = db.set("k", "hello");
  REQUIRE(*db.strlen("k") == 5);
  REQUIRE(*db.strlen("no_such") == 0);
}

// ---- Hashes ----

TEST_CASE_METHOD(DbFixture, "Hash hdel") {
  std::ignore = db.hset("h", "f1", "v1");
  std::ignore = db.hset("h", "f2", "v2");

  REQUIRE(*db.hdel("h", "f1") == true);
  REQUIRE(db.hget("h", "f1").has_value() == false);
  REQUIRE(*db.hdel("h", "f1") == false);
  REQUIRE(db.hget("h", "f2").has_value());
}

TEST_CASE_METHOD(DbFixture, "Hash hdel last field") {
  std::ignore = db.hset("h", "f", "v");
  REQUIRE(*db.hdel("h", "f") == true);
  REQUIRE(*db.exists("h") == false);
}

TEST_CASE_METHOD(DbFixture, "Hash hexists") {
  std::ignore = db.hset("h", "f", "v");
  REQUIRE(*db.hexists("h", "f") == true);
  REQUIRE(*db.hexists("h", "no") == false);
  REQUIRE(*db.hexists("no_such", "f") == false);
}

TEST_CASE_METHOD(DbFixture, "Hash hlen") {
  REQUIRE(*db.hlen("no_such") == 0);
  std::ignore = db.hset("h", "a", "1");
  REQUIRE(*db.hlen("h") == 1);
  std::ignore = db.hset("h", "b", "2");
  REQUIRE(*db.hlen("h") == 2);
}

TEST_CASE_METHOD(DbFixture, "Hash hkeys/hvals") {
  std::ignore = db.hset("h", "a", "1");
  std::ignore = db.hset("h", "b", "2");

  auto ks = db.hkeys("h");
  REQUIRE(ks.has_value());
  REQUIRE(ks->size() == 2);

  auto vs = db.hvals("h");
  REQUIRE(vs.has_value());
  REQUIRE(vs->size() == 2);
}

TEST_CASE_METHOD(DbFixture, "Hash hkeys empty") {
  auto ks = db.hkeys("no_such");
  REQUIRE(ks.has_value());
  REQUIRE(ks->empty());
}

TEST_CASE_METHOD(DbFixture, "Hash WrongType on hdel") {
  std::ignore = db.set("s", "v");
  auto r = db.hdel("s", "f");
  REQUIRE_FALSE(r.has_value());
  REQUIRE(r.error() == redismm::ErrorCode::WrongType);
}

// ---- Lists ----

TEST_CASE_METHOD(DbFixture, "List llen") {
  REQUIRE(*db.llen("no_such") == 0);
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  REQUIRE(*db.llen("l") == 2);
}

TEST_CASE_METHOD(DbFixture, "List lindex") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  std::ignore = db.rpush("l", "c");

  REQUIRE(*db.lindex("l", 0) == "a");
  REQUIRE(*db.lindex("l", 1) == "b");
  REQUIRE(*db.lindex("l", -1) == "c");
  REQUIRE_FALSE(db.lindex("l", 10).has_value());
  REQUIRE_FALSE(db.lindex("no_such", 0).has_value());
}

// ---- Sets ----

TEST_CASE_METHOD(DbFixture, "Set scard") {
  REQUIRE(*db.scard("no_such") == 0);
  std::ignore = db.sadd("s", "a");
  REQUIRE(*db.scard("s") == 1);
  std::ignore = db.sadd("s", "b");
  REQUIRE(*db.scard("s") == 2);
}

TEST_CASE_METHOD(DbFixture, "Set sismember") {
  std::ignore = db.sadd("s", "a");
  REQUIRE(*db.sismember("s", "a") == true);
  REQUIRE(*db.sismember("s", "b") == false);
  REQUIRE(*db.sismember("no_such", "a") == false);
}

TEST_CASE_METHOD(DbFixture, "Set spop") {
  std::ignore = db.sadd("s", "a");
  std::ignore = db.sadd("s", "b");

  auto r1 = db.spop("s");
  REQUIRE(r1.has_value());

  auto r2 = db.spop("s");
  REQUIRE(r2.has_value());
  REQUIRE(*r1 != *r2);

  REQUIRE_FALSE(db.spop("s").has_value());
  REQUIRE(*db.exists("s") == false);
}

TEST_CASE_METHOD(DbFixture, "Set spop empty") {
  REQUIRE_FALSE(db.spop("no_such").has_value());
}

// ---- Sorted Sets ----

TEST_CASE_METHOD(DbFixture, "ZSet zrem") {
  std::ignore = db.zadd("z", 1.0, "one");
  std::ignore = db.zadd("z", 2.0, "two");

  REQUIRE(*db.zrem("z", "one") == true);
  REQUIRE(*db.zrem("z", "one") == false);

  auto r = db.zrangebyscore("z", 0.0, 10.0);
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 1);
  REQUIRE((*r)[0] == "two");
}

TEST_CASE_METHOD(DbFixture, "ZSet zrem last member") {
  std::ignore = db.zadd("z", 1.0, "one");
  REQUIRE(*db.zrem("z", "one") == true);
  REQUIRE(*db.exists("z") == false);
}

TEST_CASE_METHOD(DbFixture, "ZSet zcard") {
  REQUIRE(*db.zcard("no_such") == 0);
  std::ignore = db.zadd("z", 1.0, "one");
  REQUIRE(*db.zcard("z") == 1);
  std::ignore = db.zadd("z", 2.0, "two");
  REQUIRE(*db.zcard("z") == 2);
}

TEST_CASE_METHOD(DbFixture, "ZSet zcount") {
  std::ignore = db.zadd("z", 1.0, "one");
  std::ignore = db.zadd("z", 2.0, "two");
  std::ignore = db.zadd("z", 3.0, "three");

  REQUIRE(*db.zcount("z", 1.5, 2.5) == 1);
  REQUIRE(*db.zcount("z", -10.0, 10.0) == 3);
  REQUIRE(*db.zcount("no_such", 0.0, 1.0) == 0);
}

TEST_CASE_METHOD(DbFixture, "ZSet zscore") {
  std::ignore = db.zadd("z", 2.5, "m");
  auto s = db.zscore("z", "m");
  REQUIRE(s.has_value());
  REQUIRE(*s == 2.5);

  REQUIRE_FALSE(db.zscore("z", "no_such").has_value());
  REQUIRE_FALSE(db.zscore("no_such", "m").has_value());
}

TEST_CASE_METHOD(DbFixture, "ZSet zrank") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 3.0, "c");
  std::ignore = db.zadd("z", 2.0, "b");

  REQUIRE(*db.zrank("z", "a") == 0);
  REQUIRE(*db.zrank("z", "b") == 1);
  REQUIRE(*db.zrank("z", "c") == 2);
  REQUIRE_FALSE(db.zrank("z", "no_such").has_value());
  REQUIRE_FALSE(db.zrank("no_such", "a").has_value());
}

TEST_CASE_METHOD(DbFixture, "ZSet zrange") {
  std::ignore = db.zadd("z", 3.0, "c");
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 2.0, "b");

  auto r1 = db.zrange("z", 0, -1);
  REQUIRE(r1.has_value());
  REQUIRE(r1->size() == 3);
  REQUIRE((*r1)[0] == "a");
  REQUIRE((*r1)[2] == "c");

  auto r2 = db.zrange("z", 0, 1);
  REQUIRE(r2.has_value());
  REQUIRE(r2->size() == 2);

  auto r3 = db.zrange("no_such", 0, -1);
  REQUIRE(r3.has_value());
  REQUIRE(r3->empty());
}

// ---- Generic ----

TEST_CASE_METHOD(DbFixture, "Generic type") {
  REQUIRE(*db.type("no_such") == "none");

  std::ignore = db.set("s", "v");
  REQUIRE(*db.type("s") == "string");

  std::ignore = db.hset("h", "f", "v");
  REQUIRE(*db.type("h") == "hash");

  std::ignore = db.rpush("l", "a");
  REQUIRE(*db.type("l") == "list");

  std::ignore = db.sadd("se", "a");
  REQUIRE(*db.type("se") == "set");

  std::ignore = db.zadd("z", 1.0, "m");
  REQUIRE(*db.type("z") == "zset");
}

TEST_CASE_METHOD(DbFixture, "String getset") {
  std::ignore = db.set("k", "old");
  auto r = db.getset("k", "new");
  REQUIRE(r.has_value());
  REQUIRE(*r == "old");
  REQUIRE(*db.get("k") == "new");
}

TEST_CASE_METHOD(DbFixture, "String getset missing") {
  auto r = db.getset("no_such", "v");
  REQUIRE_FALSE(r.has_value());
}

TEST_CASE_METHOD(DbFixture, "String setnx") {
  REQUIRE(*db.setnx("k", "v") == true);
  REQUIRE(*db.setnx("k", "v2") == false);
  REQUIRE(*db.get("k") == "v");
}

TEST_CASE_METHOD(DbFixture, "String incrbyfloat") {
  REQUIRE(*db.incrbyfloat("f", 1.5) == 1.5);
  REQUIRE(*db.incrbyfloat("f", 2.5) == 4.0);
}

TEST_CASE_METHOD(DbFixture, "String getrange") {
  std::ignore = db.set("k", "hello world");
  REQUIRE(*db.getrange("k", 0, 4) == "hello");
  REQUIRE(*db.getrange("k", -5, -1) == "world");
  REQUIRE(*db.getrange("no_such", 0, -1) == "");
}

TEST_CASE_METHOD(DbFixture, "String setrange") {
  std::ignore = db.set("k", "hello world");
  REQUIRE(*db.setrange("k", 6, "there") == 11);
  REQUIRE(*db.get("k") == "hello there");
}

TEST_CASE_METHOD(DbFixture, "Hash hsetnx") {
  REQUIRE(*db.hsetnx("h", "f", "v") == true);
  REQUIRE(*db.hsetnx("h", "f", "v2") == false);
  REQUIRE(*db.hget("h", "f") == "v");
}

TEST_CASE_METHOD(DbFixture, "Hash hincrby") {
  REQUIRE(*db.hincrby("h", "c", 10) == 10);
  REQUIRE(*db.hincrby("h", "c", 5) == 15);
}

TEST_CASE_METHOD(DbFixture, "Hash hmget") {
  std::ignore = db.hset("h", "a", "1");
  std::ignore = db.hset("h", "b", "2");
  auto r = db.hmget("h", {"a", "b", "c"});
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 3);
  REQUIRE(r->at(0) == "1");
  REQUIRE(r->at(1) == "2");
  REQUIRE(r->at(2) == std::nullopt);
}

TEST_CASE_METHOD(DbFixture, "Hash hstrlen") {
  std::ignore = db.hset("h", "f", "hello");
  REQUIRE(*db.hstrlen("h", "f") == 5);
  REQUIRE(*db.hstrlen("h", "no") == 0);
}

TEST_CASE_METHOD(DbFixture, "Hash hrandfield") {
  std::ignore = db.hset("h", "a", "1");
  std::ignore = db.hset("h", "b", "2");
  auto r = db.hrandfield("h");
  REQUIRE(r.has_value());
  REQUIRE((*r == "a" || *r == "b"));
}

TEST_CASE_METHOD(DbFixture, "Hash hrandfield empty") {
  REQUIRE_FALSE(db.hrandfield("no_such").has_value());
}

TEST_CASE_METHOD(DbFixture, "List lset") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  REQUIRE(db.lset("l", 0, "x").has_value());
  REQUIRE(*db.lindex("l", 0) == "x");
}

TEST_CASE_METHOD(DbFixture, "List lset out of range") {
  std::ignore = db.rpush("l", "a");
  REQUIRE_FALSE(db.lset("l", 5, "x").has_value());
}

TEST_CASE_METHOD(DbFixture, "List lpos") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  std::ignore = db.rpush("l", "a");
  REQUIRE(*db.lpos("l", "a") == 0);
  REQUIRE(*db.lpos("l", "b") == 1);
  REQUIRE_FALSE(db.lpos("l", "c").has_value());
}

TEST_CASE_METHOD(DbFixture, "List lpushx/rpushx") {
  REQUIRE(*db.lpushx("no_such", "a") == 0);
  REQUIRE(*db.rpushx("no_such", "a") == 0);

  std::ignore = db.rpush("l", "b");
  REQUIRE(*db.lpushx("l", "a") == 2);
  REQUIRE(*db.rpushx("l", "c") == 3);
  REQUIRE(*db.lindex("l", 0) == "a");
  REQUIRE(*db.lindex("l", 2) == "c");
}

TEST_CASE_METHOD(DbFixture, "Set srandmember") {
  std::ignore = db.sadd("s", "a");
  std::ignore = db.sadd("s", "b");
  auto r = db.srandmember("s");
  REQUIRE(r.has_value());
  REQUIRE((*r == "a" || *r == "b"));
}

TEST_CASE_METHOD(DbFixture, "Set srandmember empty") {
  REQUIRE_FALSE(db.srandmember("no_such").has_value());
}

TEST_CASE_METHOD(DbFixture, "ZSet zincrby") {
  REQUIRE(*db.zincrby("z", "m", 1.5) == 1.5);
  REQUIRE(*db.zincrby("z", "m", 2.5) == 4.0);
  REQUIRE(*db.zscore("z", "m") == 4.0);
}

TEST_CASE_METHOD(DbFixture, "ZSet zrevrank") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 3.0, "c");
  std::ignore = db.zadd("z", 2.0, "b");

  REQUIRE(*db.zrevrank("z", "a") == 2);
  REQUIRE(*db.zrevrank("z", "b") == 1);
  REQUIRE(*db.zrevrank("z", "c") == 0);
  REQUIRE_FALSE(db.zrevrank("z", "no_such").has_value());
}

TEST_CASE_METHOD(DbFixture, "ZSet zrevrange") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 2.0, "b");
  std::ignore = db.zadd("z", 3.0, "c");

  auto r = db.zrevrange("z", 0, -1);
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 3);
  REQUIRE((*r)[0] == "c");
  REQUIRE((*r)[2] == "a");
}

TEST_CASE_METHOD(DbFixture, "ZSet zpopmin") {
  std::ignore = db.zadd("z", 2.0, "b");
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 3.0, "c");
  REQUIRE(*db.zpopmin("z") == "a");
  REQUIRE(*db.zcard("z") == 2);
}

TEST_CASE_METHOD(DbFixture, "ZSet zpopmax") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 3.0, "c");
  std::ignore = db.zadd("z", 2.0, "b");
  REQUIRE(*db.zpopmax("z") == "c");
  REQUIRE(*db.zcard("z") == 2);
}

TEST_CASE_METHOD(DbFixture, "ZSet zmscore") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 2.0, "b");
  auto r = db.zmscore("z", {"a", "b", "c"});
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 3);
  REQUIRE(r->at(0) == 1.0);
  REQUIRE(r->at(1) == 2.0);
  REQUIRE(r->at(2) == std::nullopt);
}

TEST_CASE_METHOD(DbFixture, "Stream xlen") {
  REQUIRE(*db.xlen("no_such") == 0);
  std::ignore = db.xadd("s", "*", {{"k", "v"}});
  REQUIRE(*db.xlen("s") == 1);
}

TEST_CASE_METHOD(DbFixture, "Stream xdel") {
  auto id = db.xadd("s", "*", {{"k", "v"}});
  REQUIRE(id.has_value());
  REQUIRE(*db.xlen("s") == 1);
  REQUIRE(*db.xdel("s", {*id}) == 1);
  REQUIRE(*db.xlen("s") == 0);
}

TEST_CASE_METHOD(DbFixture, "Generic expireat/pexpireat") {
  std::ignore = db.set("k", "v");
  REQUIRE(*db.expireat("k", 9999999999) == true);
  REQUIRE(*db.pexpireat("no_such", 9999999999) == false);
}

TEST_CASE_METHOD(DbFixture, "Generic touch") {
  std::ignore = db.set("k", "v");
  REQUIRE(*db.touch("k") == true);
  REQUIRE(*db.touch("no_such") == false);
}

// ---- Pipeline ----

TEST_CASE_METHOD(DbFixture, "Pipeline append/hdel/zrem") {
  std::ignore = db.set("k", "hello");
  std::ignore = db.hset("h", "f", "v");
  std::ignore = db.zadd("z", 1.0, "one");

  auto pipe = db.pipeline();
  pipe.append("k", " world")
      .hdel("h", "f")
      .zrem("z", "one");
  REQUIRE(pipe.exec().has_value());

  REQUIRE(*db.get("k") == "hello world");
  REQUIRE_FALSE(db.hget("h", "f").has_value());
  REQUIRE_FALSE(db.zscore("z", "one").has_value());
}

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

// ---- List Operations ----

TEST_CASE_METHOD(DbFixture, "List lrange") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  std::ignore = db.rpush("l", "c");
  std::ignore = db.rpush("l", "d");
  std::ignore = db.rpush("l", "e");

  auto r1 = db.lrange("l", 0, -1);
  REQUIRE(r1.has_value());
  REQUIRE(r1->size() == 5);

  auto r2 = db.lrange("l", 1, 3);
  REQUIRE(r2.has_value());
  REQUIRE(r2->size() == 3);
  REQUIRE((*r2)[0] == "b");
  REQUIRE((*r2)[2] == "d");

  auto r3 = db.lrange("l", -2, -1);
  REQUIRE(r3.has_value());
  REQUIRE(r3->size() == 2);
  REQUIRE((*r3)[0] == "d");
  REQUIRE((*r3)[1] == "e");

  auto r4 = db.lrange("l", 0, 0);
  REQUIRE(r4.has_value());
  REQUIRE(r4->size() == 1);
  REQUIRE((*r4)[0] == "a");

  auto r5 = db.lrange("l", 3, 1);
  REQUIRE(r5.has_value());
  REQUIRE(r5->empty());
}

TEST_CASE_METHOD(DbFixture, "List lrem") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "c");
  std::ignore = db.rpush("l", "a");

  // 先頭から1個削除
  auto r1 = db.lrem("l", 1, "a");
  REQUIRE(r1.has_value());
  REQUIRE(*r1 == 1);

  auto v1 = db.lrange("l", 0, -1);
  REQUIRE(v1.has_value());
  REQUIRE(v1->size() == 4);
  REQUIRE((*v1)[0] == "b");
  REQUIRE((*v1)[1] == "a");

  // 末尾から1個削除
  auto r2 = db.lrem("l", -1, "a");
  REQUIRE(r2.has_value());
  REQUIRE(*r2 == 1);

  auto v2 = db.lrange("l", 0, -1);
  REQUIRE(v2.has_value());
  REQUIRE(v2->size() == 3);
  REQUIRE((*v2)[2] == "c");

  // すべて削除
  auto r3 = db.lrem("l", 0, "a");
  REQUIRE(r3.has_value());
  REQUIRE(*r3 == 1);

  auto v3 = db.lrange("l", 0, -1);
  REQUIRE(v3.has_value());
  REQUIRE(v3->size() == 2);
  REQUIRE((*v3)[0] == "b");
  REQUIRE((*v3)[1] == "c");
}

TEST_CASE_METHOD(DbFixture, "List ltrim") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  std::ignore = db.rpush("l", "c");
  std::ignore = db.rpush("l", "d");
  std::ignore = db.rpush("l", "e");

  auto r = db.ltrim("l", 1, 3);
  REQUIRE(r.has_value());

  auto v = db.lrange("l", 0, -1);
  REQUIRE(v.has_value());
  REQUIRE(v->size() == 3);
  REQUIRE((*v)[0] == "b");
  REQUIRE((*v)[1] == "c");
  REQUIRE((*v)[2] == "d");
}

TEST_CASE_METHOD(DbFixture, "List ltrim all") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");

  auto r = db.ltrim("l", 5, 10);
  REQUIRE(r.has_value());

  auto v = db.lrange("l", 0, -1);
  REQUIRE(v.has_value());
  REQUIRE(v->empty());

  auto e = db.exists("l");
  REQUIRE(e.has_value());
  REQUIRE(*e == false);
}

// ---- Set Operations ----

TEST_CASE_METHOD(DbFixture, "Set srem") {
  std::ignore = db.sadd("s", "a");
  std::ignore = db.sadd("s", "b");
  std::ignore = db.sadd("s", "c");

  auto r1 = db.srem("s", "b");
  REQUIRE(r1.has_value());
  REQUIRE(*r1 == true);

  auto r2 = db.srem("s", "b");
  REQUIRE(r2.has_value());
  REQUIRE(*r2 == false);

  auto r3 = db.smembers("s");
  REQUIRE(r3.has_value());
  REQUIRE(r3->size() == 2);
}

TEST_CASE_METHOD(DbFixture, "Set srem last member") {
  std::ignore = db.sadd("s", "a");

  auto r = db.srem("s", "a");
  REQUIRE(r.has_value());
  REQUIRE(*r == true);

  auto e = db.exists("s");
  REQUIRE(e.has_value());
  REQUIRE(*e == false);
}

TEST_CASE_METHOD(DbFixture, "Set smove") {
  std::ignore = db.sadd("src", "a");
  std::ignore = db.sadd("src", "b");
  std::ignore = db.sadd("dst", "c");

  auto r = db.smove("src", "dst", "a");
  REQUIRE(r.has_value());
  REQUIRE(*r == true);

  auto src = db.smembers("src");
  REQUIRE(src.has_value());
  REQUIRE(src->size() == 1);

  auto dst = db.smembers("dst");
  REQUIRE(dst.has_value());
  REQUIRE(dst->size() == 2);
}

TEST_CASE_METHOD(DbFixture, "Set smove nonexistent member") {
  std::ignore = db.sadd("src", "a");
  auto r = db.smove("src", "dst", "x");
  REQUIRE(r.has_value());
  REQUIRE(*r == false);
}

TEST_CASE_METHOD(DbFixture, "Set smove creates dst") {
  std::ignore = db.sadd("src", "a");
  auto r = db.smove("src", "dst", "a");
  REQUIRE(r.has_value());
  REQUIRE(*r == true);
  REQUIRE(*db.smembers("dst") == std::vector<std::string>{"a"});
}

// ---- New List Operations (Medium) ----

TEST_CASE_METHOD(DbFixture, "List linsert before") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  std::ignore = db.rpush("l", "c");

  auto r = db.linsert("l", redismm::InsertPosition::Before, "b", "x");
  REQUIRE(r.has_value());
  REQUIRE(*r == 4);

  auto v = db.lrange("l", 0, -1);
  REQUIRE(v.has_value());
  REQUIRE((*v)[0] == "a");
  REQUIRE((*v)[1] == "x");
  REQUIRE((*v)[2] == "b");
  REQUIRE((*v)[3] == "c");
}

TEST_CASE_METHOD(DbFixture, "List linsert after") {
  std::ignore = db.rpush("l", "a");
  std::ignore = db.rpush("l", "b");
  std::ignore = db.rpush("l", "c");

  auto r = db.linsert("l", redismm::InsertPosition::After, "a", "x");
  REQUIRE(r.has_value());
  REQUIRE(*r == 4);

  auto v = db.lrange("l", 0, -1);
  REQUIRE(v.has_value());
  REQUIRE((*v)[0] == "a");
  REQUIRE((*v)[1] == "x");
  REQUIRE((*v)[2] == "b");
  REQUIRE((*v)[3] == "c");
}

TEST_CASE_METHOD(DbFixture, "List linsert pivot not found") {
  std::ignore = db.rpush("l", "a");
  auto r = db.linsert("l", redismm::InsertPosition::Before, "x", "v");
  REQUIRE_FALSE(r.has_value());
  REQUIRE(r.error() == redismm::ErrorCode::NotFound);
}

TEST_CASE_METHOD(DbFixture, "List lmove left to right") {
  std::ignore = db.rpush("src", "a");
  std::ignore = db.rpush("src", "b");
  std::ignore = db.rpush("src", "c");

  auto r = db.lmove("src", "dst", redismm::ListSide::Left, redismm::ListSide::Right);
  REQUIRE(r.has_value());
  REQUIRE(*r == "a");

  auto src = db.lrange("src", 0, -1);
  REQUIRE(src.has_value());
  REQUIRE(src->size() == 2);
  REQUIRE((*src)[0] == "b");

  auto dst = db.lrange("dst", 0, -1);
  REQUIRE(dst.has_value());
  REQUIRE(dst->size() == 1);
  REQUIRE((*dst)[0] == "a");
}

TEST_CASE_METHOD(DbFixture, "List lmove right to left") {
  std::ignore = db.rpush("src", "a");
  std::ignore = db.rpush("src", "b");

  auto r = db.lmove("src", "dst", redismm::ListSide::Right, redismm::ListSide::Left);
  REQUIRE(r.has_value());
  REQUIRE(*r == "b");

  auto dst = db.lrange("dst", 0, -1);
  REQUIRE(dst.has_value());
  REQUIRE(dst->size() == 1);
  REQUIRE((*dst)[0] == "b");
}

TEST_CASE_METHOD(DbFixture, "List rpoplpush") {
  std::ignore = db.rpush("src", "a");
  std::ignore = db.rpush("src", "b");

  auto r = db.rpoplpush("src", "dst");
  REQUIRE(r.has_value());
  REQUIRE(*r == "b");

  auto dst = db.lrange("dst", 0, -1);
  REQUIRE(dst.has_value());
  REQUIRE(dst->size() == 1);
  REQUIRE((*dst)[0] == "b");
}

TEST_CASE_METHOD(DbFixture, "List lmove empty source") {
  auto r = db.lmove("src", "dst", redismm::ListSide::Left, redismm::ListSide::Right);
  REQUIRE_FALSE(r.has_value());
}

// ---- Sorted Sets (Medium) ----

TEST_CASE_METHOD(DbFixture, "ZSet zrangebylex") {
  std::ignore = db.zadd("z", 1.0, "apple");
  std::ignore = db.zadd("z", 2.0, "banana");
  std::ignore = db.zadd("z", 3.0, "cherry");
  std::ignore = db.zadd("z", 4.0, "date");

  auto r = db.zrangebylex("z", "b", "d");
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 2);
  REQUIRE((*r)[0] == "banana");
  REQUIRE((*r)[1] == "cherry");
}

TEST_CASE_METHOD(DbFixture, "ZSet zrangebylex full range") {
  std::ignore = db.zadd("z", 1.0, "beta");
  std::ignore = db.zadd("z", 2.0, "alpha");
  std::ignore = db.zadd("z", 3.0, "gamma");

  auto r = db.zrangebylex("z", "-", "+");
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 3);
  REQUIRE((*r)[0] == "alpha");
  REQUIRE((*r)[1] == "beta");
  REQUIRE((*r)[2] == "gamma");
}

TEST_CASE_METHOD(DbFixture, "ZSet zlexcount") {
  std::ignore = db.zadd("z", 1.0, "apple");
  std::ignore = db.zadd("z", 2.0, "banana");
  std::ignore = db.zadd("z", 3.0, "cherry");

  auto r = db.zlexcount("z", "b", "d");
  REQUIRE(r.has_value());
  REQUIRE(*r == 2);
}

TEST_CASE_METHOD(DbFixture, "ZSet zremrangebyrank") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 2.0, "b");
  std::ignore = db.zadd("z", 3.0, "c");
  std::ignore = db.zadd("z", 4.0, "d");
  std::ignore = db.zadd("z", 5.0, "e");

  auto r = db.zremrangebyrank("z", 1, 3);
  REQUIRE(r.has_value());
  REQUIRE(*r == 3);

  auto remaining = db.zrange("z", 0, -1);
  REQUIRE(remaining.has_value());
  REQUIRE(remaining->size() == 2);
  REQUIRE((*remaining)[0] == "a");
  REQUIRE((*remaining)[1] == "e");
}

TEST_CASE_METHOD(DbFixture, "ZSet zremrangebyrank all") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 2.0, "b");

  auto r = db.zremrangebyrank("z", 0, -1);
  REQUIRE(r.has_value());
  REQUIRE(*r == 2);
  REQUIRE(*db.exists("z") == false);
}

TEST_CASE_METHOD(DbFixture, "ZSet zremrangebyscore") {
  std::ignore = db.zadd("z", 1.0, "a");
  std::ignore = db.zadd("z", 2.0, "b");
  std::ignore = db.zadd("z", 3.0, "c");
  std::ignore = db.zadd("z", 4.0, "d");

  auto r = db.zremrangebyscore("z", 2.0, 3.0);
  REQUIRE(r.has_value());
  REQUIRE(*r == 2);

  auto remaining = db.zrangebyscore("z", 0.0, 10.0);
  REQUIRE(remaining.has_value());
  REQUIRE(remaining->size() == 2);
  REQUIRE((*remaining)[0] == "a");
  REQUIRE((*remaining)[1] == "d");
}

TEST_CASE_METHOD(DbFixture, "ZSet zremrangebyscore all") {
  std::ignore = db.zadd("z", 1.0, "a");
  auto r = db.zremrangebyscore("z", 0.0, 10.0);
  REQUIRE(r.has_value());
  REQUIRE(*r == 1);
  REQUIRE(*db.exists("z") == false);
}

// ---- Streams (Medium) ----

TEST_CASE_METHOD(DbFixture, "Stream xrange") {
  REQUIRE(db.xadd("s", "1-0", {{"k1", "v1"}}).has_value());
  REQUIRE(db.xadd("s", "2-0", {{"k2", "v2"}}).has_value());
  REQUIRE(db.xadd("s", "3-0", {{"k3", "v3"}}).has_value());

  auto r = db.xrange("s", "-", "+");
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 3);
  REQUIRE((*r)[0].id == "1-0");
  REQUIRE((*r)[1].id == "2-0");
  REQUIRE((*r)[2].id == "3-0");
  REQUIRE((*r)[0].fields.size() == 1);
  REQUIRE((*r)[0].fields[0].first == "k1");
}

TEST_CASE_METHOD(DbFixture, "Stream xrange partial") {
  REQUIRE(db.xadd("s", "1-0", {{"k", "v"}}).has_value());
  REQUIRE(db.xadd("s", "2-0", {{"k", "v"}}).has_value());
  REQUIRE(db.xadd("s", "3-0", {{"k", "v"}}).has_value());

  auto r = db.xrange("s", "2-0", "2-0");
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 1);
  REQUIRE((*r)[0].id == "2-0");
}

TEST_CASE_METHOD(DbFixture, "Stream xrevrange") {
  REQUIRE(db.xadd("s", "1-0", {{"k", "v"}}).has_value());
  REQUIRE(db.xadd("s", "2-0", {{"k", "v"}}).has_value());
  REQUIRE(db.xadd("s", "3-0", {{"k", "v"}}).has_value());

  auto r = db.xrevrange("s", "+", "-");
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 3);
  REQUIRE((*r)[0].id == "3-0");
  REQUIRE((*r)[2].id == "1-0");
}

TEST_CASE_METHOD(DbFixture, "Stream xtrim") {
  REQUIRE(db.xadd("s", "1-0", {{"k", "v"}}).has_value());
  REQUIRE(db.xadd("s", "2-0", {{"k", "v"}}).has_value());
  REQUIRE(db.xadd("s", "3-0", {{"k", "v"}}).has_value());

  auto r = db.xtrim("s", 2);
  REQUIRE(r.has_value());
  REQUIRE(*r == 1);
  REQUIRE(*db.xlen("s") == 2);
}

TEST_CASE_METHOD(DbFixture, "Stream xtrim below maxlen") {
  REQUIRE(db.xadd("s", "1-0", {{"k", "v"}}).has_value());
  auto r = db.xtrim("s", 10);
  REQUIRE(r.has_value());
  REQUIRE(*r == 0);
}

// ---- Generic (Medium) ----

TEST_CASE_METHOD(DbFixture, "Generic keys pattern match") {
  std::ignore = db.set("foo", "1");
  std::ignore = db.set("bar", "2");
  std::ignore = db.set("baz", "3");
  std::ignore = db.set("qux", "4");

  auto r = db.keys("b*");
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 2);
  std::sort(r->begin(), r->end());
  REQUIRE((*r)[0] == "bar");
  REQUIRE((*r)[1] == "baz");
}

TEST_CASE_METHOD(DbFixture, "Generic keys empty db") {
  auto r = db.keys("*");
  REQUIRE(r.has_value());
  REQUIRE(r->empty());
}

TEST_CASE_METHOD(DbFixture, "Generic keys wildcard") {
  std::ignore = db.set("alpha", "1");
  std::ignore = db.set("beta", "2");
  auto r = db.keys("*");
  REQUIRE(r.has_value());
  REQUIRE(r->size() == 2);
}

TEST_CASE_METHOD(DbFixture, "Generic randomkey") {
  std::ignore = db.set("k1", "v1");
  std::ignore = db.set("k2", "v2");
  std::ignore = db.set("k3", "v3");

  auto r = db.randomkey();
  REQUIRE(r.has_value());
  REQUIRE(!r->empty());
}

TEST_CASE_METHOD(DbFixture, "Generic randomkey empty") {
  auto r = db.randomkey();
  REQUIRE_FALSE(r.has_value());
  REQUIRE(r.error() == redismm::ErrorCode::NotFound);
}

// ---- Pipeline ----

TEST_CASE_METHOD(DbFixture, "Pipeline basic") {
  auto pipe = db.pipeline();

  pipe.set("k1", "v1")
      .set("k2", "v2")
      .hset("h", "f", "v")
      .sadd("s", "m")
      .rpush("l", "i1")
      .rpush("l", "i2")
      .zadd("z", 1.0, "zm");

  auto r = pipe.exec();
  REQUIRE(r.has_value());

  auto v1 = db.get("k1");
  REQUIRE(v1.has_value());
  REQUIRE(*v1 == "v1");

  auto v2 = db.hget("h", "f");
  REQUIRE(v2.has_value());
  REQUIRE(*v2 == "v");

  auto v3 = db.smembers("s");
  REQUIRE(v3.has_value());
  REQUIRE(v3->size() == 1);

  auto v4 = db.lrange("l", 0, -1);
  REQUIRE(v4.has_value());
  REQUIRE(v4->size() == 2);
}

TEST_CASE_METHOD(DbFixture, "Pipeline same key") {
  auto pipe = db.pipeline();

  pipe.set("counter", "1")
      .expire("counter", 10);

  auto r = pipe.exec();
  REQUIRE(r.has_value());

  auto t = db.ttl("counter");
  REQUIRE(t.has_value());
  REQUIRE(*t > 0);
}

TEST_CASE_METHOD(DbFixture, "Pipeline del") {
  std::ignore = db.set("k", "v");

  auto pipe = db.pipeline();
  pipe.set("k2", "v2")
      .del("k");

  auto r = pipe.exec();
  REQUIRE(r.has_value());

  auto e1 = db.exists("k");
  REQUIRE(e1.has_value());
  REQUIRE(*e1 == false);

  auto e2 = db.exists("k2");
  REQUIRE(e2.has_value());
  REQUIRE(*e2 == true);
}

