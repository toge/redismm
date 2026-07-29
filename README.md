# redismm

RocksDB をストレージエンジンとして使用する、Redisライクな組み込み Key-Value ストアライブラリです。

## 特徴

- **RedisサブセットのAPI**: Strings, Hashes, Lists, Sets, Sorted Sets, Streams の 6 データ型をサポート
- **永続化**: RocksDB による高信頼性ストレージ
- **有効期限管理**: EXPIRE, TTL, PEXPIRE, PTTL, PERSIST コマンドをサポート
- **型安全**: `std::expected` を使用したエラーハンドリング
- **C++23**: モダン C++ 機能を活用

## 対応データ型

| データ型    | 説明                                       |
| ----------- | ------------------------------------------ |
| Strings     | 文字列の set/get                           |
| Hashes      | フィールド-値のマップ                      |
| Lists       | 両端キュー（lpush/rpush/lpop/rpop）        |
| Sets        | 一意なメンバーのコレクション               |
| Sorted Sets | スコア付きメンバーのソート済みコレクション |
| Streams     | ログ構造データ（xadd）                     |

## 有効期限管理

| コマンド                     | 説明                       |
| ---------------------------- | -------------------------- |
| `expire(key, seconds)`       | 有効期限を秒で設定         |
| `ttl(key)`                   | 残り有効期限を秒で取得     |
| `pexpire(key, milliseconds)` | 有効期限をミリ秒で設定     |
| `pttl(key)`                  | 残り有効期限をミリ秒で取得 |
| `persist(key)`               | 有効期限を削除             |

## ビルド要件

- C++23 対応コンパイラ（GCC 13+, Clang 16+, MSVC 2022+）
- CMake 3.25+
- vcpkg（依存関係管理）

## 依存関係

- [RocksDB](https://github.com/facebook/rocksdb) - ストレージエンジン
- [Catch2](https://github.com/catchorg/Catch2) - テストフレームワーク（テストビルド時のみ）

## ビルド方法

### 1. vcpkg のセットアップ

```bash
export VCPKG_ROOT=/path/to/vcpkg
```

### 2. ビルドスクリプトの実行

```bash
cmake -B build \
  -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -S .

cmake --build build --parallel
```

## テスト実行

```bash
./test.sh
```

または:

```bash
cd build
ctest -V
```

## 使用例

```cpp
#include "redismm/EmbeddedRedis.hpp"
#include <iostream>

int main() {
  // データベースを開く
  redismm::EmbeddedRedis db("/path/to/db");
  if (!db.is_open()) {
    std::cerr << "Failed to open database\n";
    return 1;
  }

  // Strings
  db.set("name", "Alice");
  auto name = db.get("name");
  if (name) {
    std::cout << *name << "\n";  // Alice
  }

  // 有効期限設定
  db.expire("name", 60);  // 60秒後に期限切れ
  auto ttl = db.ttl("name");
  if (ttl) {
    std::cout << "TTL: " << *ttl << "秒\n";
  }

  // Hashes
  db.hset("user:1", "name", "Bob");
  db.hset("user:1", "age", "30");
  auto user = db.hgetall("user:1");

  // Lists
  db.rpush("queue", "task1");
  db.rpush("queue", "task2");
  auto task = db.lpop("queue");

  // Sets
  db.sadd("tags", "cpp");
  db.sadd("tags", "rust");
  auto tags = db.smembers("tags");

  // Sorted Sets
  db.zadd("scores", 100.0, "player1");
  db.zadd("scores", 85.5, "player2");
  auto top = db.zrangebyscore("scores", 80.0, 100.0);

  // Streams
  auto id = db.xadd("events", "*", {{"type", "login"}, {"user", "alice"}});

  // 汎用操作
  db.exists("name");
  db.del("name");

  return 0;
}
```

## エラーハンドリング

すべての操作は `Result<T>` 型（`std::expected<T, ErrorCode>`）を返します。

```cpp
auto result = db.get("key");
if (result) {
  // 成功: *result で値にアクセス
  std::cout << *result << "\n";
} else {
  // エラー: result.error() でエラーコードを取得
  switch (result.error()) {
  case redismm::ErrorCode::NotFound:
    std::cerr << "Key not found\n";
    break;
  case redismm::ErrorCode::WrongType:
    std::cerr << "Wrong type\n";
    break;
  case redismm::ErrorCode::RocksDBError:
    std::cerr << "Storage error\n";
    break;
  case redismm::ErrorCode::InvalidArgument:
    std::cerr << "Invalid argument\n";
    break;
  }
}
```

## プロジェクト構成

```
redismm/
├── include/redismm/
│   ├── EmbeddedRedis.hpp  # メイン API
│   └── Encoder.hpp        # 内部エンコーダ
├── src/
│   └── EmbeddedRedis.cpp  # 実装
├── test/
│   └── test_embedded_redis.cpp  # テスト
├── main.cpp               # デモプログラム
├── CMakeLists.txt         # ビルド設定
├── vcpkg.json             # 依存関係定義
├── build.sh               # ビルドスクリプト
└── test.sh                # テストスクリプト
```

## ライセンス

[LICENSE](LICENSE) を参照してください。
