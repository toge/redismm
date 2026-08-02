// レビューで検出した不具合の回帰テスト。
// 各 TEST_CASE のコメントは「修正前に何が起きていたか」を記録している。
#include "catch2/catch_all.hpp"
#include "redismm/EmbeddedRedis.hpp"

#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace {

/** @brief 回帰テスト用に専用ディレクトリを払い出すフィクスチャ */
struct RegressionFixture {
  std::filesystem::path  path;
  redismm::EmbeddedRedis db;

  static std::filesystem::path fresh_path() {
    auto p = std::filesystem::temp_directory_path() / "redismm_regression";
    std::filesystem::remove_all(p);
    return p;
  }

  // デストラクタで remove_all しない理由は test_embedded_redis.cpp の DbFixture と同じ
  RegressionFixture() : path(fresh_path()), db(path.string()) {
    REQUIRE(db.is_open());
  }
};

/** @brief NUL を含み、他方のエンコード済みプレフィックスと衝突しうるキー */
std::string const kBinaryKeyA = "a";
std::string const kBinaryKeyB = std::string("a\0\0\0\0\0\0\0\x01", 9) + "x";

} // namespace

// ============================================================
// #1 lrem がシーケンス空間に開けた穴とインデックス計算
// ============================================================

TEST_CASE_METHOD(RegressionFixture, "lrange indexes survive holes left by lrem") {
  // 修正前: lrange は seek 先を head_seq + index で計算していたため、
  //   lrem が空けた穴の分だけずれて別の要素を返していた（lrange 2 2 が "c"）。
  for (auto const* v : {"a", "b", "c", "d"}) {
    REQUIRE(db.rpush("L", v).has_value());
  }
  REQUIRE(db.lrem("L", 1, "b").value() == 1);
  REQUIRE(db.llen("L").value() == 3);

  REQUIRE(db.lrange("L", 0, -1).value() == std::vector<std::string>{"a", "c", "d"});
  REQUIRE(db.lrange("L", 2, 2).value() == std::vector<std::string>{"d"});
  REQUIRE(db.lrange("L", 1, 2).value() == std::vector<std::string>{"c", "d"});
  REQUIRE(db.lrange("L", 0, 0).value() == std::vector<std::string>{"a"});

  // 位置ベースの lindex と一致すること
  for (int64_t i = 0; i < 3; ++i) {
    REQUIRE(db.lindex("L", i).value() == db.lrange("L", i, i).value().front());
  }
}

TEST_CASE_METHOD(RegressionFixture, "ltrim keeps the right elements after lrem") {
  // 修正前: ltrim も head_seq + index で境界を決めていたため、
  //   穴のあるリストでは残すべき要素まで削除され llen と実データが食い違った。
  for (auto const* v : {"a", "b", "c", "d", "e"}) {
    REQUIRE(db.rpush("T", v).has_value());
  }
  REQUIRE(db.lrem("T", 1, "b").value() == 1);
  REQUIRE(db.lrange("T", 0, -1).value() == std::vector<std::string>{"a", "c", "d", "e"});

  REQUIRE(db.ltrim("T", 0, 1).has_value());
  REQUIRE(db.llen("T").value() == 2);
  REQUIRE(db.lrange("T", 0, -1).value() == std::vector<std::string>{"a", "c"});
}

TEST_CASE_METHOD(RegressionFixture, "ltrim from the middle after lrem") {
  for (auto const* v : {"a", "b", "c", "d", "e"}) {
    REQUIRE(db.rpush("T", v).has_value());
  }
  REQUIRE(db.lrem("T", 1, "c").value() == 1); // [a, b, d, e]
  REQUIRE(db.ltrim("T", 1, -1).has_value());
  REQUIRE(db.lrange("T", 0, -1).value() == std::vector<std::string>{"b", "d", "e"});
  REQUIRE(db.llen("T").value() == 3);

  // トリム後も push/pop が破綻しないこと
  REQUIRE(db.rpush("T", "f").value() == 4);
  REQUIRE(db.lpush("T", "z").value() == 5);
  REQUIRE(db.lrange("T", 0, -1).value() == std::vector<std::string>{"z", "b", "d", "e", "f"});
}

// ============================================================
// #2 lmove の宛先リスト初期化
// ============================================================

TEST_CASE_METHOD(RegressionFixture, "lmove into a fresh destination allows later lpush") {
  // 修正前: 新規宛先の head_seq を 0 で初期化していたため、直後の lpush で
  //   head_seq が uint64_t の下限を一周し、lrange が空を返していた（llen は 2）。
  REQUIRE(db.rpush("src", "x").has_value());
  REQUIRE(db.lmove("src", "dst", redismm::ListSide::Left, redismm::ListSide::Right).value() == "x");

  REQUIRE(db.lpush("dst", "front").value() == 2);
  REQUIRE(db.llen("dst").value() == 2);
  REQUIRE(db.lrange("dst", 0, -1).value() == std::vector<std::string>{"front", "x"});
  REQUIRE(db.lindex("dst", 0).value() == "front");
  REQUIRE(db.lindex("dst", 1).value() == "x");
}

TEST_CASE_METHOD(RegressionFixture, "rpoplpush into a fresh destination allows later rpush") {
  REQUIRE(db.rpush("src", "x").has_value());
  REQUIRE(db.rpoplpush("src", "dst").value() == "x");

  REQUIRE(db.rpush("dst", "back").value() == 2);
  REQUIRE(db.lrange("dst", 0, -1).value() == std::vector<std::string>{"x", "back"});
}

// ============================================================
// #5 バイナリキーのプレフィックス衝突
// ============================================================

TEST_CASE_METHOD(RegressionFixture, "keys with embedded NUL do not share data space") {
  // 修正前: データキーが [prefix] + key + version の並びでキー長を持たなかったため、
  //   "a" のプレフィックスが "a\0...\x01x" のデータキーに一致し、
  //   hgetall が他キーのフィールドを返し、del が他キーのデータを消していた。
  REQUIRE(db.hset(kBinaryKeyA, "f1", "v1").has_value());
  REQUIRE(db.hset(kBinaryKeyB, "f2", "v2").has_value());

  REQUIRE(db.hlen(kBinaryKeyA).value() == 1);
  REQUIRE(db.hlen(kBinaryKeyB).value() == 1);

  auto const all_a = db.hgetall(kBinaryKeyA).value();
  REQUIRE(all_a.size() == 1);
  REQUIRE(all_a.at("f1") == "v1");

  auto const all_b = db.hgetall(kBinaryKeyB).value();
  REQUIRE(all_b.size() == 1);
  REQUIRE(all_b.at("f2") == "v2");

  // 片方を消してももう片方は残る
  REQUIRE(db.del(kBinaryKeyA).value());
  REQUIRE(db.hget(kBinaryKeyB, "f2").value() == "v2");
}

TEST_CASE_METHOD(RegressionFixture, "collections with colliding binary keys stay isolated") {
  REQUIRE(db.sadd(kBinaryKeyA, "m1").has_value());
  REQUIRE(db.sadd(kBinaryKeyB, "m2").has_value());
  REQUIRE(db.smembers(kBinaryKeyA).value() == std::vector<std::string>{"m1"});
  REQUIRE(db.smembers(kBinaryKeyB).value() == std::vector<std::string>{"m2"});

  std::string const zkey_a = kBinaryKeyA + "z";
  std::string const zkey_b = kBinaryKeyB + "z";
  REQUIRE(db.zadd(zkey_a, 1.0, "za").has_value());
  REQUIRE(db.zadd(zkey_b, 2.0, "zb").has_value());
  REQUIRE(db.zcard(zkey_a).value() == 1);
  REQUIRE(db.zrange(zkey_a, 0, -1).value() == std::vector<std::string>{"za"});
  REQUIRE(db.zrange(zkey_b, 0, -1).value() == std::vector<std::string>{"zb"});

  std::string const lkey_a = kBinaryKeyA + "l";
  std::string const lkey_b = kBinaryKeyB + "l";
  REQUIRE(db.rpush(lkey_a, "la").has_value());
  REQUIRE(db.rpush(lkey_b, "lb").has_value());
  REQUIRE(db.lrange(lkey_a, 0, -1).value() == std::vector<std::string>{"la"});
  REQUIRE(db.del(lkey_a).value());
  REQUIRE(db.lrange(lkey_b, 0, -1).value() == std::vector<std::string>{"lb"});
}

// ============================================================
// #6 TTL の保持と破棄
// ============================================================

TEST_CASE_METHOD(RegressionFixture, "value-mutating string ops preserve TTL") {
  // 修正前: append / incr 系 / setrange は MetaValue を作り直していたため
  //   expiration_ms が 0 に戻り、TTL が黙って消えていた。
  auto ttl_of = [&](std::string_view key) { return db.ttl(key).value(); };

  REQUIRE(db.set("s", "hello").has_value());
  REQUIRE(db.expire("s", 100).value());
  REQUIRE(db.append("s", "!").has_value());
  REQUIRE(ttl_of("s") > 0);
  REQUIRE(db.get("s").value() == "hello!");

  REQUIRE(db.set("n", "1").has_value());
  REQUIRE(db.expire("n", 100).value());
  REQUIRE(db.incr("n").value() == 2);
  REQUIRE(ttl_of("n") > 0);
  REQUIRE(db.incrby("n", 5).value() == 7);
  REQUIRE(ttl_of("n") > 0);
  REQUIRE(db.decr("n").value() == 6);
  REQUIRE(ttl_of("n") > 0);

  REQUIRE(db.set("f", "1.5").has_value());
  REQUIRE(db.expire("f", 100).value());
  REQUIRE(db.incrbyfloat("f", 0.5).value() == Catch::Approx(2.0));
  REQUIRE(ttl_of("f") > 0);

  REQUIRE(db.set("r", "hello").has_value());
  REQUIRE(db.expire("r", 100).value());
  REQUIRE(db.setrange("r", 1, "E").has_value());
  REQUIRE(ttl_of("r") > 0);
  REQUIRE(db.get("r").value() == "hEllo");
}

TEST_CASE_METHOD(RegressionFixture, "set and getset drop the TTL like Redis") {
  REQUIRE(db.set("s", "a").has_value());
  REQUIRE(db.expire("s", 100).value());
  REQUIRE(db.set("s", "b").has_value()); // TTL なしの SET は TTL を消す
  REQUIRE(db.ttl("s").value() == -1);

  REQUIRE(db.expire("s", 100).value());
  REQUIRE(db.getset("s", "c").value() == "b");
  REQUIRE(db.ttl("s").value() == -1);
}

// ============================================================
// #7 zrevrange の範囲クランプ
// ============================================================

TEST_CASE_METHOD(RegressionFixture, "zrevrange clamps ranges the same way zrange does") {
  // 修正前: start を size-1 に丸めていたため、範囲外指定でも 1 件返っていた。
  REQUIRE(db.zadd("Z", 1.0, "a").has_value());
  REQUIRE(db.zadd("Z", 2.0, "b").has_value());
  REQUIRE(db.zadd("Z", 3.0, "c").has_value());

  REQUIRE(db.zrange("Z", 5, 10).value().empty());
  REQUIRE(db.zrevrange("Z", 5, 10).value().empty());

  REQUIRE(db.zrevrange("Z", 0, -1).value() == std::vector<std::string>{"c", "b", "a"});
  REQUIRE(db.zrevrange("Z", 0, 0).value() == std::vector<std::string>{"c"});
  REQUIRE(db.zrevrange("Z", 1, 5).value() == std::vector<std::string>{"b", "a"});
  REQUIRE(db.zrevrange("Z", -2, -1).value() == std::vector<std::string>{"b", "a"});
}

// ============================================================
// #8 存在しないキーに対する戻り値の統一
// ============================================================

TEST_CASE_METHOD(RegressionFixture, "missing keys return Redis-compatible empty values") {
  // 修正前: 同じ「存在しないキー」でも型ごとに NotFound と空が混在していた。
  REQUIRE(db.hgetall("missing").value().empty());
  REQUIRE(db.hkeys("missing").value().empty());
  REQUIRE(db.hvals("missing").value().empty());
  REQUIRE(db.smembers("missing").value().empty());
  REQUIRE(db.srem("missing", "m").value() == false);
  REQUIRE(db.zrangebyscore("missing", 0.0, 10.0).value().empty());
  REQUIRE(db.zrange("missing", 0, -1).value().empty());
  REQUIRE(db.zrevrange("missing", 0, -1).value().empty());
  REQUIRE(db.lrange("missing", 0, -1).value().empty());
  REQUIRE(db.lrem("missing", 0, "v").value() == 0);
  REQUIRE(db.linsert("missing", redismm::InsertPosition::Before, "p", "v").value() == 0);
  REQUIRE(db.ltrim("missing", 0, -1).has_value());
  REQUIRE(db.zrem("missing", "m").value() == false);
  REQUIRE(db.hdel("missing", "f").value() == false);
}

TEST_CASE_METHOD(RegressionFixture, "wrong type still reports WrongType") {
  // 空を返すようにしても、型不一致の検出は失わないこと
  REQUIRE(db.set("str", "v").has_value());
  REQUIRE(db.hgetall("str").error() == redismm::ErrorCode::WrongType);
  REQUIRE(db.smembers("str").error() == redismm::ErrorCode::WrongType);
  REQUIRE(db.srem("str", "m").error() == redismm::ErrorCode::WrongType);
  REQUIRE(db.zrangebyscore("str", 0.0, 1.0).error() == redismm::ErrorCode::WrongType);
  REQUIRE(db.lrem("str", 0, "v").error() == redismm::ErrorCode::WrongType);
  REQUIRE(db.ltrim("str", 0, -1).error() == redismm::ErrorCode::WrongType);
  REQUIRE(db.linsert("str", redismm::InsertPosition::Before, "p", "v").error() == redismm::ErrorCode::WrongType);
}

TEST_CASE_METHOD(RegressionFixture, "linsert still reports NotFound for a missing pivot") {
  REQUIRE(db.rpush("L", "a").has_value());
  REQUIRE(db.linsert("L", redismm::InsertPosition::Before, "nope", "v").error() == redismm::ErrorCode::NotFound);
  REQUIRE(db.linsert("L", redismm::InsertPosition::Before, "a", "v").value() == 2);
  REQUIRE(db.lrange("L", 0, -1).value() == std::vector<std::string>{"v", "a"});
}

// ============================================================
// #3 スレッド安全性
// ============================================================

TEST_CASE_METHOD(RegressionFixture, "concurrent incr does not lose updates") {
  // 修正前: get -> parse -> put が非アトミックだったため、
  //   4 スレッド x 500 回で 2000 のはずが 501 にしかならなかった。
  constexpr int kThreads = 4;
  constexpr int kIters   = 250;

  REQUIRE(db.set("ctr", "0").has_value());

  std::vector<std::thread> ts;
  ts.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&] {
      for (int i = 0; i < kIters; ++i) {
        REQUIRE(db.incr("ctr").has_value());
      }
    });
  }
  for (auto& t : ts) {
    t.join();
  }

  REQUIRE(db.get("ctr").value() == std::to_string(kThreads * kIters));
}

TEST_CASE_METHOD(RegressionFixture, "concurrent sadd keeps scard consistent with the data") {
  // 修正前: メタの size 更新が競合し、scard が実メンバー数と恒久的にずれていた。
  constexpr int kThreads = 4;
  constexpr int kIters   = 250;

  std::vector<std::thread> ts;
  ts.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&, t] {
      for (int i = 0; i < kIters; ++i) {
        REQUIRE(db.sadd("s", std::to_string(t * kIters + i)).has_value());
      }
    });
  }
  for (auto& t : ts) {
    t.join();
  }

  REQUIRE(db.smembers("s").value().size() == kThreads * kIters);
  REQUIRE(db.scard("s").value() == kThreads * kIters);
}

TEST_CASE_METHOD(RegressionFixture, "concurrent rpush keeps every element reachable") {
  constexpr int kThreads = 4;
  constexpr int kIters   = 100;

  std::vector<std::thread> ts;
  ts.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&, t] {
      for (int i = 0; i < kIters; ++i) {
        REQUIRE(db.rpush("q", std::to_string(t * kIters + i)).has_value());
      }
    });
  }
  for (auto& t : ts) {
    t.join();
  }

  REQUIRE(db.llen("q").value() == kThreads * kIters);
  REQUIRE(db.lrange("q", 0, -1).value().size() == kThreads * kIters);
}

// ============================================================
// #4 Pipeline
// ============================================================

TEST_CASE("Pipeline on a database that failed to open does not crash") {
  // 修正前: Pipeline は is_open() を見ずに rocksdb::DB* を参照していたため、
  //   オープンに失敗した DB に対して操作を積むと segfault していた。
  auto const bad_path = std::filesystem::temp_directory_path() / "redismm_not_a_db";
  std::filesystem::remove_all(bad_path);
  {
    std::ofstream f(bad_path); // ディレクトリではなく通常ファイルを置く
    f << "not a database";
  }

  redismm::EmbeddedRedis db(bad_path.string());
  REQUIRE_FALSE(db.is_open());

  auto pipe = db.pipeline();
  pipe.set("k", "v").hset("h", "f", "v").rpush("l", "v").del("k").lrem("l", 0, "v").ltrim("l", 0, 1);
  auto const r = pipe.exec();
  REQUIRE_FALSE(r.has_value());
  REQUIRE(r.error() == redismm::ErrorCode::RocksDBError);

  std::filesystem::remove_all(bad_path);
}

TEST_CASE_METHOD(RegressionFixture, "Pipeline sees its own uncommitted writes") {
  // 修正前: Pipeline::lrem は DB を直接走査しており、同じ Pipeline 内で
  //   push した要素が見えず、head/tail の再計算も行っていなかった。
  auto pipe = db.pipeline();
  pipe.rpush("L", "a").rpush("L", "b").rpush("L", "a").lrem("L", 0, "a");
  REQUIRE(pipe.exec().has_value());

  REQUIRE(db.llen("L").value() == 1);
  REQUIRE(db.lrange("L", 0, -1).value() == std::vector<std::string>{"b"});
  REQUIRE(db.lindex("L", 0).value() == "b");
}

TEST_CASE_METHOD(RegressionFixture, "Pipeline ltrim matches the non-pipelined behaviour") {
  for (auto const* v : {"a", "b", "c", "d", "e"}) {
    REQUIRE(db.rpush("L", v).has_value());
  }
  REQUIRE(db.lrem("L", 1, "b").value() == 1); // [a, c, d, e]

  auto pipe = db.pipeline();
  pipe.ltrim("L", 1, 2);
  REQUIRE(pipe.exec().has_value());

  REQUIRE(db.llen("L").value() == 2);
  REQUIRE(db.lrange("L", 0, -1).value() == std::vector<std::string>{"c", "d"});
}

TEST_CASE_METHOD(RegressionFixture, "Pipeline preserves TTL on append") {
  REQUIRE(db.set("s", "hello").has_value());
  REQUIRE(db.expire("s", 100).value());

  auto pipe = db.pipeline();
  pipe.append("s", "!");
  REQUIRE(pipe.exec().has_value());

  REQUIRE(db.get("s").value() == "hello!");
  REQUIRE(db.ttl("s").value() > 0);
}

TEST_CASE_METHOD(RegressionFixture, "Pipeline is reusable after a successful exec") {
  auto pipe = db.pipeline();
  pipe.set("a", "1");
  REQUIRE(pipe.exec().has_value());

  pipe.set("b", "2");
  REQUIRE(pipe.exec().has_value());

  REQUIRE(db.get("a").value() == "1");
  REQUIRE(db.get("b").value() == "2");
}

TEST_CASE_METHOD(RegressionFixture, "Pipeline changes are invisible until exec") {
  auto pipe = db.pipeline();
  pipe.set("pending", "v");
  REQUIRE(db.exists("pending").value() == false);
  REQUIRE(pipe.exec().has_value());
  REQUIRE(db.exists("pending").value() == true);
}
