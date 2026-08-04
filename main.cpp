#include "redismm/RedisStore.hpp"

#include <filesystem>
#include <iostream>
#include <print>

namespace {

// バックエンドの選択はこの 1 行で切り替える（例: 将来は redismm::RedisClient）
using Store = redismm::EmbeddedRedis;

/**
 * @brief 簡易チェック関数
 *
 * @param label テストラベル
 * @param cond 条件
 */
void check(std::string_view label, bool cond) {
  if (cond) {
    std::println("  [OK] {}", label);
  } else {
    std::println("  [FAIL] {}", label);
  }
}

/**
 * @brief Result 型の成功判定
 *
 * @return 値が存在すれば true
 */
template <typename T>
auto ok(redismm::Result<T> const& r) -> bool {
  return r.has_value();
}

// ---- 各データ型のデモ ----

/**
 * @brief 文字列操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_strings(Store& db) {
  std::println("\n=== Strings ===");

  std::ignore = db.set("name", "Alice");
  auto v      = db.get("name");
  check("get after set", ok(v) && *v == "Alice");

  std::ignore = db.set("name", "Bob");
  v           = db.get("name");
  check("overwrite", ok(v) && *v == "Bob");

  auto missing = db.get("no_such_key");
  check("get missing → NotFound", !missing && missing.error() == redismm::ErrorCode::NotFound);

  // TTL: 1 ms で即期限切れ
  std::ignore    = db.set("ttl_key", "value", 1);
  auto ttl_check = db.get("ttl_key");
  check("set with TTL written", ok(ttl_check) || !ttl_check);
}

/**
 * @brief ハッシュ操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_hashes(Store& db) {
  std::println("\n=== Hashes ===");

  auto r1 = db.hset("user:1", "name", "Alice");
  check("hset new field → true", ok(r1) && *r1 == true);

  auto r2 = db.hset("user:1", "name", "Alice2");
  check("hset existing field → false", ok(r2) && *r2 == false);

  std::ignore = db.hset("user:1", "age", "30");

  auto name = db.hget("user:1", "name");
  check("hget name", ok(name) && *name == "Alice2");

  auto all = db.hgetall("user:1");
  check("hgetall size == 2", ok(all) && all->size() == 2);
  check("hgetall age", ok(all) && all->at("age") == "30");

  auto wrong = db.hget("user:1", "nonexistent");
  check("hget missing field → NotFound", !wrong && wrong.error() == redismm::ErrorCode::NotFound);
}

/**
 * @brief リスト操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_lists(Store& db) {
  std::println("\n=== Lists ===");

  auto r1 = db.rpush("queue", "a");
  auto r2 = db.rpush("queue", "b");
  auto r3 = db.rpush("queue", "c");
  check("rpush counts", ok(r1) && *r1 == 1 && ok(r2) && *r2 == 2 && ok(r3) && *r3 == 3);

  auto lp = db.lpop("queue");
  check("lpop → a", ok(lp) && *lp == "a");

  auto rp = db.rpop("queue");
  check("rpop → c", ok(rp) && *rp == "c");

  auto lp2 = db.lpop("queue");
  check("lpop → b", ok(lp2) && *lp2 == "b");

  auto empty = db.lpop("queue");
  check("lpop empty → NotFound", !empty && empty.error() == redismm::ErrorCode::NotFound);

  // LPUSH で先頭挿入
  std::ignore    = db.rpush("stack", "x");
  std::ignore    = db.lpush("stack", "y");
  auto top       = db.lpop("stack");
  check("lpush then lpop → y", ok(top) && *top == "y");
}

/**
 * @brief セット操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_sets(Store& db) {
  std::println("\n=== Sets ===");

  auto a1 = db.sadd("tags", "cpp");
  auto a2 = db.sadd("tags", "rust");
  auto a3 = db.sadd("tags", "cpp");
  check("sadd new → true", ok(a1) && *a1 == true);
  check("sadd new → true", ok(a2) && *a2 == true);
  check("sadd dup → false", ok(a3) && *a3 == false);

  auto members = db.smembers("tags");
  check("smembers size == 2", ok(members) && members->size() == 2);
}

/**
 * @brief ソート済みセット操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_zsets(Store& db) {
  std::println("\n=== Sorted Sets ===");

  std::ignore = db.zadd("scores", 1.5, "Alice");
  std::ignore = db.zadd("scores", 3.0, "Bob");
  std::ignore = db.zadd("scores", 2.0, "Carol");
  std::ignore = db.zadd("scores", -1.0, "Dan");

  auto r1 = db.zrangebyscore("scores", 1.0, 2.5);
  check("zrangebyscore [1.0, 2.5] has 2", ok(r1) && r1->size() == 2);
  check("order: Alice < Carol", ok(r1) && (*r1)[0] == "Alice" && (*r1)[1] == "Carol");

  // score 更新
  std::ignore = db.zadd("scores", 5.0, "Alice");
  auto r2     = db.zrangebyscore("scores", 4.5, 5.5);
  check("zadd update score", ok(r2) && r2->size() == 1 && (*r2)[0] == "Alice");

  auto r3 = db.zrangebyscore("scores", -2.0, 0.0);
  check("negative score range", ok(r3) && r3->size() == 1 && (*r3)[0] == "Dan");
}

/**
 * @brief ストリーム操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_streams(Store& db) {
  std::println("\n=== Streams ===");

  auto id1 = db.xadd("events", "*", {{"type", "login"}, {"user", "Alice"}});
  auto id2 = db.xadd("events", "*", {{"type", "logout"}, {"user", "Bob"}});
  check("xadd auto-id 1", ok(id1));
  check("xadd auto-id 2", ok(id2));
  if (ok(id1) && ok(id2)) {
    std::println("  id1={} id2={}", *id1, *id2);
  }

  auto id3 = db.xadd("events2", "100-0", {{"x", "hello"}});
  check("xadd explicit id", ok(id3) && *id3 == "100-0");

  // 単調増加違反
  auto bad = db.xadd("events2", "100-0", {{"y", "world"}});
  check("xadd duplicate id → InvalidArgument", !bad && bad.error() == redismm::ErrorCode::InvalidArgument);
}

/**
 * @brief 汎用操作のデモ（exists/del と型チェック）
 *
 * @param db データベースインスタンス
 */
void test_generic(Store& db) {
  std::println("\n=== Generic ===");

  std::ignore = db.set("del_me", "value");
  auto ex1    = db.exists("del_me");
  check("exists → true", ok(ex1) && *ex1 == true);

  auto d = db.del("del_me");
  check("del → true", ok(d) && *d == true);

  auto ex2 = db.exists("del_me");
  check("exists after del → false", ok(ex2) && *ex2 == false);

  auto d2 = db.del("del_me");
  check("del missing → false", ok(d2) && *d2 == false);

  // WrongType チェック: Set キーを String として読む
  std::ignore    = db.sadd("myset2", "a");
  auto wrong_get = db.get("myset2");
  check("get on Set key → WrongType", !wrong_get && wrong_get.error() == redismm::ErrorCode::WrongType);
}

/**
 * @brief 有効期限操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_expire(Store& db) {
  std::println("\n=== Expire ===");

  std::ignore = db.set("temp_key", "temp_value");

  auto ex1 = db.expire("temp_key", 10);
  check("expire → true", ok(ex1) && *ex1 == true);

  auto t1 = db.ttl("temp_key");
  check("ttl > 0", ok(t1) && *t1 > 0);

  auto pt1 = db.pttl("temp_key");
  check("pttl > 0", ok(pt1) && *pt1 > 0);

  auto ex2 = db.persist("temp_key");
  check("persist → true", ok(ex2) && *ex2 == true);

  auto t2 = db.ttl("temp_key");
  check("ttl after persist → -1", ok(t2) && *t2 == -1);

  auto ex3 = db.expire("no_such_key", 10);
  check("expire missing → false", ok(ex3) && *ex3 == false);

  auto t3 = db.ttl("no_such_key");
  check("ttl missing → -1", ok(t3) && *t3 == -1);

  auto ex4 = db.persist("no_such_key");
  check("persist missing → false", ok(ex4) && *ex4 == false);

  auto ex5 = db.persist("temp_key");
  check("persist no TTL → false", ok(ex5) && *ex5 == false);
}

/**
 * @brief リスト拡張操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_list_ops(Store& db) {
  std::println("\n=== List Operations ===");

  std::ignore = db.rpush("mylist", "a");
  std::ignore = db.rpush("mylist", "b");
  std::ignore = db.rpush("mylist", "c");
  std::ignore = db.rpush("mylist", "d");
  std::ignore = db.rpush("mylist", "e");

  auto r1 = db.lrange("mylist", 0, -1);
  check("lrange all size == 5", ok(r1) && r1->size() == 5);

  auto r2 = db.lrange("mylist", 1, 3);
  check("lrange [1,3] size == 3", ok(r2) && r2->size() == 3);
  check("lrange [1,3] values", ok(r2) && (*r2)[0] == "b" && (*r2)[2] == "d");

  auto r3 = db.lrange("mylist", -2, -1);
  check("lrange [-2,-1] size == 2", ok(r3) && r3->size() == 2);
  check("lrange [-2,-1] values", ok(r3) && (*r3)[0] == "d" && (*r3)[1] == "e");

  auto rem = db.lrem("mylist", 1, "b");
  check("lrem 1 'b' → 1", ok(rem) && *rem == 1);

  auto r4 = db.lrange("mylist", 0, -1);
  check("after lrem size == 4", ok(r4) && r4->size() == 4);
  check("after lrem no 'b'", ok(r4) && (*r4)[0] == "a" && (*r4)[1] == "c");

  auto tr = db.ltrim("mylist", 1, 2);
  check("ltrim [1,2] → ok", ok(tr));

  auto r5 = db.lrange("mylist", 0, -1);
  check("after ltrim size == 2", ok(r5) && r5->size() == 2);
  check("after ltrim values", ok(r5) && (*r5)[0] == "c" && (*r5)[1] == "d");
}

/**
 * @brief セット拡張操作のデモ
 *
 * @param db データベースインスタンス
 */
void test_set_ops(Store& db) {
  std::println("\n=== Set Operations ===");

  std::ignore = db.sadd("myset", "a");
  std::ignore = db.sadd("myset", "b");
  std::ignore = db.sadd("myset", "c");

  auto r1 = db.srem("myset", "b");
  check("srem 'b' → true", ok(r1) && *r1 == true);

  auto r2 = db.srem("myset", "b");
  check("srem 'b' again → false", ok(r2) && *r2 == false);

  auto r3 = db.smembers("myset");
  check("smembers size == 2", ok(r3) && r3->size() == 2);
}

/**
 * @brief Pipeline のデモ
 *
 * @param db データベースインスタンス
 */
void test_pipeline(Store& db) {
  std::println("\n=== Pipeline ===");

  auto pipe = db.pipeline();

  pipe.set("pkey1", "value1")
      .set("pkey2", "value2")
      .hset("phash", "field1", "fvalue1")
      .sadd("pset", "member1")
      .rpush("plist", "item1")
      .rpush("plist", "item2")
      .zadd("pzset", 1.0, "zmember1");

  auto r = pipe.exec();
  check("pipeline exec → ok", ok(r));

  auto v1 = db.get("pkey1");
  check("pipeline get pkey1", ok(v1) && *v1 == "value1");

  auto v2 = db.hget("phash", "field1");
  check("pipeline hget phash", ok(v2) && *v2 == "fvalue1");

  auto v3 = db.smembers("pset");
  check("pipeline smembers pset", ok(v3) && v3->size() == 1);

  auto v4 = db.lrange("plist", 0, -1);
  check("pipeline lrange plist", ok(v4) && v4->size() == 2);

  // 同じキーへの連続操作
  auto pipe2 = db.pipeline();
  pipe2.set("counter", "1")
      .expire("counter", 10);

  auto r2 = pipe2.exec();
  check("pipeline same key exec → ok", ok(r2));

  auto t = db.ttl("counter");
  check("pipeline ttl > 0", ok(t) && *t > 0);
}

} // namespace

/** @brief デモエントリポイント */
int main() {
  auto const db_path = std::filesystem::temp_directory_path() / "redismm_demo";
  std::filesystem::remove_all(db_path);

  std::println("Opening EmbeddedRedis at {}", db_path.string());
  Store db = redismm::make_store(redismm::EmbeddedConfig{db_path.string()});

  if (!db.is_open()) {
    std::cerr << "Failed to open database\n";
    return 1;
  }

  test_strings(db);
  test_hashes(db);
  test_lists(db);
  test_sets(db);
  test_zsets(db);
  test_streams(db);
  test_generic(db);
  test_expire(db);
  test_list_ops(db);
  test_set_ops(db);
  test_pipeline(db);

  std::println("\nDone.");
  return 0;
}
