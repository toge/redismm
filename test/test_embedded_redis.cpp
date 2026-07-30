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

  // デストラクタで remove_all してはいけない。dtor 本体は db の破棄より先に走るため、
  // Windows では RocksDB が開いたままのファイル削除に失敗し、filesystem_error が
  // noexcept デストラクタを脱出して terminate する (MSVC CI の 0xC0000409)。
  // 各テストのクリーン状態は次テストの fresh_path() が保証する。
  DbFixture() : path(fresh_path()), db(path.string()) {}
};

// ---- Strings ----

TEST_CASE("trivial: no fixture") {
  REQUIRE(1 + 1 == 2);
}

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

TEST_CASE_METHOD(DbFixture, "String getset when missing returns NotFound") {
  auto res = db.getset("missing_k", "val");
  REQUIRE_FALSE(res.has_value());
  REQUIRE(res.error() == redismm::ErrorCode::NotFound);
}

TEST_CASE_METHOD(DbFixture, "String getrange basics and negative indices") {
  std::ignore = db.set("gk", "hello");
  auto r1 = db.getrange("gk", 0, 1);
  REQUIRE(r1.has_value());
  REQUIRE(*r1 == "he");

  auto r2 = db.getrange("gk", 1, -1);
  REQUIRE(r2.has_value());
  REQUIRE(*r2 == "ello");

  auto r3 = db.getrange("gk", -2, -1);
  REQUIRE(r3.has_value());
  REQUIRE(*r3 == "lo");

  auto r4 = db.getrange("gk", 2, 1);
  REQUIRE(r4.has_value());
  REQUIRE(*r4 == "");

  auto r5 = db.getrange("gk", 0, 100);
  REQUIRE(r5.has_value());
  REQUIRE(*r5 == "hello");

  // missing key returns empty string
  auto rm = db.getrange("no_such", 0, 10);
  REQUIRE(rm.has_value());
  REQUIRE(*rm == "");
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

TEST_CASE_METHOD(DbFixture, "Stream xdel deletes existing and skips missing") {
  // prepare stream with two entries
  auto id1r = db.xadd("st", "*", {{"k", "v1"}});
  auto id2r = db.xadd("st", "*", {{"k", "v2"}});
  REQUIRE(id1r.has_value());
  REQUIRE(id2r.has_value());
  std::string id1 = *id1r;
  std::string id2 = *id2r;

  // delete existing id1
  auto r1 = db.xdel("st", std::vector<std::string_view>{std::string_view(id1)});
  REQUIRE(r1.has_value());
  REQUIRE(*r1 == 1);
  auto len_after1 = db.xlen("st");
  REQUIRE(len_after1.has_value());
  REQUIRE(*len_after1 == 1);

  // deleting a non-existent id should return 0 and not change size
  auto r2 = db.xdel("st", std::vector<std::string_view>{"999999-0"});
  REQUIRE(r2.has_value());
  REQUIRE(*r2 == 0);
  auto len_after2 = db.xlen("st");
  REQUIRE(len_after2.has_value());
  REQUIRE(*len_after2 == 1);

  // delete existing mixed with missing
  auto id3r = db.xadd("st", "*", {{"k", "v3"}});
  REQUIRE(id3r.has_value());
  std::string id3 = *id3r;
  // current ids: id2, id3
  auto r3 = db.xdel("st", std::vector<std::string_view>{std::string_view(id2), "nope-0"});
  REQUIRE(r3.has_value());
  REQUIRE(*r3 == 1);
  auto len_after3 = db.xlen("st");
  REQUIRE(len_after3.has_value());
  REQUIRE(*len_after3 == 1);
}

TEST_CASE_METHOD(DbFixture, "Stream xdel id double-delete does not underflow") {
  auto idr = db.xadd("sx", "*", {{"k", "v"}});
  REQUIRE(idr.has_value());
  std::string id = *idr;

  // first delete succeeds
  auto r1 = db.xdel("sx", std::vector<std::string_view>{std::string_view(id)});
  REQUIRE(r1.has_value());
  REQUIRE(*r1 == 1);
  auto len1 = db.xlen("sx");
  REQUIRE(len1.has_value());
  REQUIRE(*len1 == 0);

  // second delete of same id should be a no-op and not make size wrap
  auto r2 = db.xdel("sx", std::vector<std::string_view>{std::string_view(id)});
  REQUIRE(r2.has_value());
  REQUIRE(*r2 == 0);
  auto len2 = db.xlen("sx");
  REQUIRE(len2.has_value());
  REQUIRE(*len2 == 0);
}

// ---- Generic ----

TEST_CASE_METHOD(DbFixture, "Pipeline records WrongType and prevents exec") {
  // prepare a string key
  std::ignore = db.set("pk", "val");

  auto pipe = db.pipeline();
  pipe.hset("pk", "f", "v");
  auto res = pipe.exec();
  REQUIRE_FALSE(res.has_value());
  REQUIRE(res.error() == redismm::ErrorCode::WrongType);

  // ensure original key remains a string and unchanged
  auto gv = db.get("pk");
  REQUIRE(gv.has_value());
  REQUIRE(*gv == "val");
}

TEST_CASE_METHOD(DbFixture, "Pipeline records InvalidArgument for bad xadd id and prevents exec") {
  auto pipe = db.pipeline();
  pipe.xadd("ps", "bad-id", {});
  auto res = pipe.exec();
  REQUIRE_FALSE(res.has_value());
  REQUIRE(res.error() == redismm::ErrorCode::InvalidArgument);

  // no stream should be created
  auto len = db.xlen("ps");
  REQUIRE(len.has_value());
  REQUIRE(*len == 0);
}

TEST_CASE_METHOD(DbFixture, "Pipeline keeps first error when earlier op fails") {
  // prepare a string key
  std::ignore = db.set("k", "v");

  auto pipe = db.pipeline();
  // first operation errors (WrongType)
  pipe.hset("k", "f", "val");
  // second operation would error with InvalidArgument if evaluated
  pipe.xadd("s", "bad-id", {});

  auto res = pipe.exec();
  REQUIRE_FALSE(res.has_value());
  REQUIRE(res.error() == redismm::ErrorCode::WrongType);

  // ensure no changes were applied
  auto gv = db.get("k");
  REQUIRE(gv.has_value());
  REQUIRE(*gv == "v");
  auto sl = db.xlen("s");
  REQUIRE(sl.has_value());
  REQUIRE(*sl == 0);
}

TEST_CASE_METHOD(DbFixture, "Pipeline keeps first error when a later op errors before another") {
  // prepare a string key for WrongType check later
  std::ignore = db.set("k", "orig");

  auto pipe = db.pipeline();
  // first op succeeds (creates hash 'h')
  pipe.hset("h", "f", "1");
  // second op errors (InvalidArgument)
  pipe.xadd("s2", "bad-id", {});
  // third op would error (WrongType) but must not overwrite earlier error
  pipe.hset("k", "f2", "2");

  auto res = pipe.exec();
  REQUIRE_FALSE(res.has_value());
  REQUIRE(res.error() == redismm::ErrorCode::InvalidArgument);

  // ensure no changes were applied: stream not created and hash not created
  auto sl = db.xlen("s2");
  REQUIRE(sl.has_value());
  REQUIRE(*sl == 0);
  auto hget = db.hget("h", "f");
  REQUIRE_FALSE(hget.has_value());
  REQUIRE(hget.error() == redismm::ErrorCode::NotFound);
  auto gv = db.get("k");
  REQUIRE(gv.has_value());
  REQUIRE(*gv == "orig");
}

TEST_CASE_METHOD(DbFixture, "Pipeline exec clears pending ops on error so pipeline can be reused") {
  // prepare a string key that will trigger WrongType
  std::ignore = db.set("r", "v");

  auto pipe = db.pipeline();
  pipe.hset("r", "f", "x"); // will set WrongType in pipeline

  auto res = pipe.exec();
  REQUIRE_FALSE(res.has_value());
  REQUIRE(res.error() == redismm::ErrorCode::WrongType);

  // after failing exec, pipeline should be cleared and reusable
  auto pipe2 = db.pipeline();
  pipe2.set("ok", "1");
  auto r2 = pipe2.exec();
  REQUIRE(r2.has_value());
  auto got = db.get("ok");
  REQUIRE(got.has_value());
  REQUIRE(*got == "1");
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
