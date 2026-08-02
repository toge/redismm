#pragma once

#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

namespace redismm {

/** @brief データ型を表す列挙型 */
enum class DataType : uint8_t {
  String = 0x01, ///< 文字列
  Hash   = 0x02, ///< ハッシュ
  List   = 0x03, ///< リスト
  Set    = 0x04, ///< セット
  ZSet   = 0x05, ///< ソート済みセット
  Stream = 0x06, ///< ストリーム
};

/** @brief RocksDB キーの名前空間プレフィックス（1 バイト） */
namespace prefix {
  inline constexpr uint8_t Meta       = 0x00; ///< メタデータ
  inline constexpr char    String     = 'S';  ///< 文字列データ
  inline constexpr char    Hash       = 'H';  ///< ハッシュフィールド
  inline constexpr char    List       = 'L';  ///< リスト要素
  inline constexpr char    Set        = 'E';  ///< セットメンバー（'S' と衝突回避）
  inline constexpr char    ZSetMember = 'Z';  ///< ソート済みセットメンバー→スコア
  inline constexpr char    ZSetScore  = 'W';  ///< ソート済みセットスコア→メンバー
  inline constexpr char    Stream     = 'R';  ///< ストリームエントリ
} // namespace prefix

/**
 * @brief データキーのヘッダ長（ユーザーキー本体を除く）
 * @details レイアウトは ['prefix'(1)] + [keylen(4BE)] + [key] + [version(8BE)] + [suffix]。
 *   keylen を前置することでユーザーキーの境界が一意に定まり、NUL を含むバイナリキーでも
 *   「あるキーのプレフィックスが別のキーのプレフィックスに含まれる」曖昧さが発生しない。
 *   これを省くと例えば "a" と "a\0\0\0\0\0\0\0\x01x" のデータ領域が重なり、
 *   走査・範囲削除が別キーのデータを巻き込む。
 */
inline constexpr std::size_t kKeyLenSize  = 4; ///< ユーザーキー長フィールドのバイト数
inline constexpr std::size_t kVersionSize = 8; ///< バージョンフィールドのバイト数

/**
 * @brief メタデータのバイナリレイアウト（固定長）
 * @details
 *   [0]      Type     (1 バイト)
 *   [1..8]   Expiration ms (8 バイト big-endian, 0 = TTL なし)
 *   [9..16]  Version        (8 バイト big-endian)
 *   [17..24] Size           (8 バイト big-endian)
 *   --- List のみ ---
 *   [25..32] HeadSeq        (8 バイト big-endian)
 *   [33..40] TailSeq        (8 バイト big-endian)
 *   --- Stream のみ ---
 *   [25..32] LastMS         (8 バイト big-endian)
 *   [33..40] LastSeq        (8 バイト big-endian)
 */
inline constexpr std::size_t kMetaBaseSize   = 25;                 ///< 基本メタデータサイズ
inline constexpr std::size_t kMetaExtSize    = kMetaBaseSize + 16; ///< 拡張メタデータサイズ
inline constexpr std::size_t kListMetaSize   = kMetaExtSize;       ///< リスト用メタデータサイズ
inline constexpr std::size_t kStreamMetaSize = kMetaExtSize;       ///< ストリーム用メタデータサイズ

/** @brief メタデータ構造体 */
struct MetaValue {
  DataType type;               ///< データ型
  uint64_t expiration_ms = 0;  ///< 有効期限（ミリ秒）。0 は無期限
  uint64_t version       = 1;  ///< データバージョン
  uint64_t size          = 0;  ///< 要素数
  // List
  uint64_t head_seq = 0;       ///< リスト先頭シーケンス番号
  uint64_t tail_seq = 0;       ///< リスト末尾シーケンス番号
  // Stream
  uint64_t last_ms  = 0;       ///< ストリーム最終エントリのミリ秒部分
  uint64_t last_seq = 0;       ///< ストリーム最終エントリのシーケンス番号
};

// ---- 移植性のある big-endian 入出力補助関数 ----

/**
 * @brief 64 ビット整数を big-endian でバッファに書き込む
 *
 * @param buf 出力先
 * @param v 書き込む値
 */
inline void write_u64be(uint8_t* buf, uint64_t v) {
  buf[0] = static_cast<uint8_t>(v >> 56);
  buf[1] = static_cast<uint8_t>(v >> 48);
  buf[2] = static_cast<uint8_t>(v >> 40);
  buf[3] = static_cast<uint8_t>(v >> 32);
  buf[4] = static_cast<uint8_t>(v >> 24);
  buf[5] = static_cast<uint8_t>(v >> 16);
  buf[6] = static_cast<uint8_t>(v >> 8);
  buf[7] = static_cast<uint8_t>(v);
}

/**
 * @brief big-endian バッファから 64 ビット整数を読み込む
 *
 * @param buf 入力
 * @return 読み込んだ値
 */
inline uint64_t read_u64be(uint8_t const* buf) {
  return (uint64_t{buf[0]} << 56) | (uint64_t{buf[1]} << 48) | (uint64_t{buf[2]} << 40) | (uint64_t{buf[3]} << 32) |
         (uint64_t{buf[4]} << 24) | (uint64_t{buf[5]} << 16) | (uint64_t{buf[6]} << 8) | uint64_t{buf[7]};
}

/**
 * @brief 32 ビット整数を big-endian でバッファに書き込む
 *
 * @param buf 出力先
 * @param v 書き込む値
 */
inline void write_u32be(uint8_t* buf, uint32_t v) {
  buf[0] = static_cast<uint8_t>(v >> 24);
  buf[1] = static_cast<uint8_t>(v >> 16);
  buf[2] = static_cast<uint8_t>(v >> 8);
  buf[3] = static_cast<uint8_t>(v);
}

/**
 * @brief big-endian バッファから 32 ビット整数を読み込む
 *
 * @param buf 入力
 * @return 読み込んだ値
 */
inline uint32_t read_u32be(uint8_t const* buf) {
  return (uint32_t{buf[0]} << 24) | (uint32_t{buf[1]} << 16) | (uint32_t{buf[2]} << 8) | uint32_t{buf[3]};
}

// ---- ZSet 用スコアエンコード ----
/**
 * @brief IEEE 754 double をバイトソート可能なビット列に変換する
 * @details
 *   正数 / +0: 符号ビット(bit63) を反転
 *   負数:      全ビットを反転
 *   結果を Big-Endian で格納すると、RocksDB のレキシコグラフィカル順が数値順と一致する。
 * @param score 変換するスコア
 * @return 8 バイトのエンコード済みバイト列
 */
inline auto encode_score(double score) -> std::array<uint8_t, 8> {
  auto bits = std::bit_cast<uint64_t>(score);
  bits      = (bits >> 63) ? ~bits : bits ^ (uint64_t{1} << 63);
  std::array<uint8_t, 8> out{};
  write_u64be(out.data(), bits);
  return out;
}

/**
 * @brief エンコード済みバイト列から元のスコアを復元する
 *
 * @see encode_score
 * @param buf エンコード済み 8 バイト
 * @return 復元されたスコア
 */
inline double decode_score(uint8_t const* buf) {
  auto const bits_be = read_u64be(buf);
  auto const bits    = (bits_be >> 63) ? bits_be ^ (uint64_t{1} << 63) : ~bits_be;
  return std::bit_cast<double>(bits);
}

// ---- 時間補助関数 ----

/**
 * @brief 現在時刻をエポックからの経過ミリ秒で取得する
 *
 * @return ミリ秒単位の現在時刻
 */
inline uint64_t now_ms() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

/**
 * @brief メタデータの有効期限が切れているか判定する
 *
 * @param m メタデータ
 * @return 期限切れなら true
 */
inline bool is_expired(MetaValue const& m) {
  return m.expiration_ms != 0 && now_ms() >= m.expiration_ms;
}

// ---- メタキー/メタ値のエンコード/デコード ----

/** @brief メタキーを生成する @param key ユーザーキー @return プレフィックス付きメタキー */
inline std::string encode_meta_key(std::string_view key) {
  std::string out;
  out.reserve(1 + key.size());
  out.push_back(static_cast<char>(prefix::Meta));
  out.append(key);
  return out;
}

/** @brief メタデータ構造体をバイナリ値にシリアライズする @param m メタデータ @return シリアライズ済みバイナリ */
inline std::string encode_meta_value(MetaValue const& m) {
  bool const  ext   = (m.type == DataType::List || m.type == DataType::Stream);
  auto const  total = ext ? kMetaExtSize : kMetaBaseSize;
  std::string out(total, '\0');
  auto*       b = reinterpret_cast<uint8_t*>(out.data());
  b[0]          = static_cast<uint8_t>(m.type);
  write_u64be(b + 1, m.expiration_ms);
  write_u64be(b + 9, m.version);
  write_u64be(b + 17, m.size);
  if (m.type == DataType::List) {
    write_u64be(b + 25, m.head_seq);
    write_u64be(b + 33, m.tail_seq);
  } else if (m.type == DataType::Stream) {
    write_u64be(b + 25, m.last_ms);
    write_u64be(b + 33, m.last_seq);
  }
  return out;
}

/**
 * @brief バイナリ値をメタデータ構造体にデコードする
 *
 * @param data シリアライズ済みバイナリ
 * @return メタデータ。サイズ不足時は nullopt
 */
inline std::optional<MetaValue> decode_meta_value(std::string_view data) {
  if (data.size() < kMetaBaseSize) {
    return std::nullopt;
  }
  auto const* b = reinterpret_cast<uint8_t const*>(data.data());

  MetaValue m;
  m.type          = static_cast<DataType>(b[0]);
  m.expiration_ms = read_u64be(b + 1);
  m.version       = read_u64be(b + 9);
  m.size          = read_u64be(b + 17);
  if (m.type == DataType::List) {
    if (data.size() < kListMetaSize) {
      return std::nullopt;
    }
    m.head_seq = read_u64be(b + 25);
    m.tail_seq = read_u64be(b + 33);
  } else if (m.type == DataType::Stream) {
    if (data.size() < kStreamMetaSize) {
      return std::nullopt;
    }
    m.last_ms  = read_u64be(b + 25);
    m.last_seq = read_u64be(b + 33);
  }
  return m;
}

// ---- データキーエンコーダ ----

/**
 * @brief データキー共通ヘッダを書き込む: [prefix] + keylen(4BE) + key + version(8BE)
 *
 * @param out     出力先（末尾に追記する）
 * @param pfx     名前空間プレフィックス
 * @param key     ユーザーキー
 * @param version メタデータバージョン
 */
inline void append_key_head(std::string& out, char pfx, std::string_view key, uint64_t version) {
  auto const base = out.size();
  out.resize(base + 1 + kKeyLenSize);
  out[base] = pfx;
  write_u32be(reinterpret_cast<uint8_t*>(out.data() + base + 1), static_cast<uint32_t>(key.size()));
  out.append(key);
  auto const vpos = out.size();
  out.resize(vpos + kVersionSize);
  write_u64be(reinterpret_cast<uint8_t*>(out.data() + vpos), version);
}

/** @brief データキー共通ヘッダの長さ @param key ユーザーキー @return ヘッダ長 */
inline std::size_t key_head_size(std::string_view key) {
  return 1 + kKeyLenSize + key.size() + kVersionSize;
}

/**
 * @brief 文字列データキーを生成: ['S'] + key
 * @note 文字列は完全一致でしか引かないため、プレフィックス走査の曖昧さは生じない
 * @param key ユーザーキー
 * @return エンコード済みキー
 */
inline std::string encode_string_key(std::string_view key) {
  std::string out;
  out.reserve(1 + key.size());
  out.push_back(prefix::String);
  out.append(key);
  return out;
}

/**
 * @brief ハッシュフィールドキーを生成: ['H'] + keylen(4BE) + key + version(8BE) + field
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @param field フィールド名
 * @return エンコード済みキー
 */
inline std::string encode_hash_key(std::string_view key, uint64_t version, std::string_view field) {
  std::string out;
  out.reserve(key_head_size(key) + field.size());
  append_key_head(out, prefix::Hash, key, version);
  out.append(field);
  return out;
}

/**
 * @brief ハッシュ走査用プレフィックスキー: ['H'] + keylen(4BE) + key + version(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @return プレフィックスキー
 */
inline std::string encode_hash_prefix(std::string_view key, uint64_t version) {
  std::string out;
  out.reserve(key_head_size(key));
  append_key_head(out, prefix::Hash, key, version);
  return out;
}

/**
 * @brief リスト要素キーを生成: ['L'] + keylen(4BE) + key + version(8BE) + index(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @param index シーケンス番号
 * @return エンコード済みキー
 */
inline std::string encode_list_key(std::string_view key, uint64_t version, uint64_t index) {
  std::string out;
  out.reserve(key_head_size(key) + 8);
  append_key_head(out, prefix::List, key, version);
  auto const pos = out.size();
  out.resize(pos + 8);
  write_u64be(reinterpret_cast<uint8_t*>(out.data() + pos), index);
  return out;
}

/**
 * @brief リスト走査用プレフィックスキー: ['L'] + keylen(4BE) + key + version(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @return プレフィックスキー
 */
inline std::string encode_list_prefix(std::string_view key, uint64_t version) {
  std::string out;
  out.reserve(key_head_size(key));
  append_key_head(out, prefix::List, key, version);
  return out;
}

/**
 * @brief セットメンバーキーを生成: ['E'] + keylen(4BE) + key + version(8BE) + member
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @param member メンバー
 * @return エンコード済みキー
 */
inline std::string encode_set_key(std::string_view key, uint64_t version, std::string_view member) {
  std::string out;
  out.reserve(key_head_size(key) + member.size());
  append_key_head(out, prefix::Set, key, version);
  out.append(member);
  return out;
}

/**
 * @brief セット走査用プレフィックスキー: ['E'] + keylen(4BE) + key + version(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @return プレフィックスキー
 */
inline std::string encode_set_prefix(std::string_view key, uint64_t version) {
  std::string out;
  out.reserve(key_head_size(key));
  append_key_head(out, prefix::Set, key, version);
  return out;
}

/**
 * @brief ZSet メンバー→スコアキーを生成: ['Z'] + keylen(4BE) + key + version(8BE) + member → value: score(8BE エンコード)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @param member メンバー
 * @return エンコード済みキー
 */
inline std::string encode_zset_member_key(std::string_view key, uint64_t version, std::string_view member) {
  std::string out;
  out.reserve(key_head_size(key) + member.size());
  append_key_head(out, prefix::ZSetMember, key, version);
  out.append(member);
  return out;
}

/**
 * @brief ZSet メンバー走査用プレフィックス: ['Z'] + keylen(4BE) + key + version(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @return プレフィックスキー
 */
inline std::string encode_zset_member_prefix(std::string_view key, uint64_t version) {
  std::string out;
  out.reserve(key_head_size(key));
  append_key_head(out, prefix::ZSetMember, key, version);
  return out;
}

/**
 * @brief ZSet スコア→メンバー逆引きキーを生成: ['W'] + keylen(4BE) + key + version(8BE) + score(8BE) + member
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @param score スコア
 * @param member メンバー
 * @return エンコード済みキー
 */
inline std::string encode_zset_score_key(std::string_view key, uint64_t version, double score, std::string_view member) {
  auto const  sb = encode_score(score);
  std::string out;
  out.reserve(key_head_size(key) + 8 + member.size());
  append_key_head(out, prefix::ZSetScore, key, version);
  out.append(reinterpret_cast<char const*>(sb.data()), 8);
  out.append(member);
  return out;
}

/**
 * @brief ZSet スコア範囲走査用プレフィックス: ['W'] + keylen(4BE) + key + version(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @return プレフィックスキー
 */
inline std::string encode_zset_score_range_prefix(std::string_view key, uint64_t version) {
  std::string out;
  out.reserve(key_head_size(key));
  append_key_head(out, prefix::ZSetScore, key, version);
  return out;
}

/**
 * @brief スコア範囲シーク用キーを生成: ['W'] + keylen(4BE) + key + version(8BE) + score(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @param score シーク開始スコア
 * @return シークキー
 */
inline std::string encode_zset_score_seek_key(std::string_view key, uint64_t version, double score) {
  auto const  sb = encode_score(score);
  std::string out;
  out.reserve(key_head_size(key) + 8);
  append_key_head(out, prefix::ZSetScore, key, version);
  out.append(reinterpret_cast<char const*>(sb.data()), 8);
  return out;
}

/**
 * @brief ストリームエントリキーを生成: ['R'] + keylen(4BE) + key + version(8BE) + ms(8BE) + seq(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @param ms エントリのミリ秒
 * @param seq エントリのシーケンス番号
 * @return エンコード済みキー
 */
inline std::string encode_stream_key(std::string_view key, uint64_t version, uint64_t ms, uint64_t seq) {
  std::string out;
  out.reserve(key_head_size(key) + 16);
  append_key_head(out, prefix::Stream, key, version);
  auto const pos = out.size();
  out.resize(pos + 16);
  auto* b = reinterpret_cast<uint8_t*>(out.data() + pos);
  write_u64be(b, ms);
  write_u64be(b + 8, seq);
  return out;
}

/**
 * @brief ストリーム走査用プレフィックスキー: ['R'] + keylen(4BE) + key + version(8BE)
 *
 * @param key ユーザーキー
 * @param version メタデータバージョン
 * @return プレフィックスキー
 */
inline std::string encode_stream_prefix(std::string_view key, uint64_t version) {
  std::string out;
  out.reserve(key_head_size(key));
  append_key_head(out, prefix::Stream, key, version);
  return out;
}

// ---- フィールド/メンバー抽出 ----

/**
 * @brief エンコード済みキーのうち、フィールド／メンバー／シーケンスが始まるオフセット
 *
 * @param user_key   ユーザーキー
 * @param prefix_len プレフィックス長（1 バイト）
 * @return サフィックス開始オフセット
 */
inline std::size_t suffix_offset(std::string_view user_key, std::size_t prefix_len = 1) {
  return prefix_len + kKeyLenSize + user_key.size() + kVersionSize;
}

/**
 * @brief エンコード済みキーからサフィックス（フィールド/メンバー）を切り出す
 *
 * @param encoded_key エンコード済みキー全体
 * @param prefix_len  プレフィックス長（1 バイト）
 * @param user_key    ユーザーキー
 * @return サフィックス部分。範囲外の場合は空文字列
 */
inline std::string_view extract_suffix(std::string_view encoded_key, std::size_t prefix_len, std::string_view user_key) {
  auto const offset = suffix_offset(user_key, prefix_len);
  if (encoded_key.size() <= offset) {
    return {};
  }
  return encoded_key.substr(offset);
}

} // namespace redismm
