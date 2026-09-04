// Copyright 2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/gcs/store_client/redis_context.h"

extern "C" {
#include "hiredis/hiredis.h"
}

namespace ray::gcs {
TEST(TestCallbackReply, TestParseAsStringArray) {
  {
    redisReply redis_reply_string1;
    redis_reply_string1.type = REDIS_REPLY_STRING;
    std::string string1 = "string1";
    redis_reply_string1.str = string1.data();
    redis_reply_string1.len = 7;

    redisReply redis_reply_string2;
    redis_reply_string2.type = REDIS_REPLY_STRING;
    std::string string2 = "string2";
    redis_reply_string2.str = string2.data();
    redis_reply_string2.len = 7;

    redisReply redis_reply_array;
    redis_reply_array.type = REDIS_REPLY_ARRAY;
    redis_reply_array.elements = 2;
    redisReply *redis_reply_array_elements[2];
    redis_reply_array_elements[0] = &redis_reply_string1;
    redis_reply_array_elements[1] = &redis_reply_string2;
    redis_reply_array.element = redis_reply_array_elements;
    CallbackReply callback_reply(redis_reply_array);
    ASSERT_EQ(
        callback_reply.ReadAsStringArray(),
        (std::vector<std::optional<std::string>>{std::optional<std::string>(string1),
                                                 std::optional<std::string>(string2)}));
  }

  {
    redisReply redis_reply_string1;
    redis_reply_string1.type = REDIS_REPLY_STRING;
    std::string string1 = "string1";
    redis_reply_string1.str = string1.data();
    redis_reply_string1.len = 7;

    redisReply redis_reply_nil1;
    redis_reply_nil1.type = REDIS_REPLY_NIL;
    redisReply redis_reply_nil2;
    redis_reply_nil2.type = REDIS_REPLY_NIL;

    redisReply redis_reply_array;
    redis_reply_array.type = REDIS_REPLY_ARRAY;
    redis_reply_array.elements = 3;
    redisReply *redis_reply_array_elements[3];
    redis_reply_array_elements[0] = &redis_reply_nil1;
    redis_reply_array_elements[1] = &redis_reply_string1;
    redis_reply_array_elements[2] = &redis_reply_nil2;
    redis_reply_array.element = redis_reply_array_elements;
    CallbackReply callback_reply(redis_reply_array);
    ASSERT_EQ(
        callback_reply.ReadAsStringArray(),
        (std::vector<std::optional<std::string>>{std::optional<std::string>(),
                                                 std::optional<std::string>(string1),
                                                 std::optional<std::string>()}));
  }

  {
    redisReply redis_reply_cursor;
    redis_reply_cursor.type = REDIS_REPLY_STRING;
    std::string num_str = "18446744073709551614";
    redis_reply_cursor.str = num_str.data();
    redis_reply_cursor.len = num_str.size();

    redisReply redis_reply_array;
    redis_reply_array.type = REDIS_REPLY_ARRAY;
    redis_reply_array.elements = 0;
    redis_reply_array.element = NULL;

    redisReply redis_reply_test;
    redis_reply_test.type = REDIS_REPLY_ARRAY;
    redis_reply_test.elements = 2;
    redisReply *redis_reply_test_elements[2];
    redis_reply_test_elements[0] = &redis_reply_cursor;
    redis_reply_test_elements[1] = &redis_reply_array;
    redis_reply_test.element = redis_reply_test_elements;
    CallbackReply callback_reply(redis_reply_test);
    std::vector<std::string> scan_array;
    ASSERT_EQ(callback_reply.ReadAsScanArray(&scan_array), 18446744073709551614u);
  }
}

TEST(TestCallbackReply, TestErrorReplyDoesNotCrash) {
  // A REDIS_REPLY_ERROR reply must not abort the process (it previously
  // RAY_LOG(FATAL)'d). It should be flagged as an error instead, so callers can
  // return a Status, e.g. ValidateRedisDB during a non-fatal Connect.
  redisReply redis_reply_error;
  redis_reply_error.type = REDIS_REPLY_ERROR;
  std::string error = "ERR This instance has cluster support disabled";
  redis_reply_error.str = error.data();
  redis_reply_error.len = error.size();

  CallbackReply callback_reply(redis_reply_error);
  ASSERT_TRUE(callback_reply.IsError());
}

namespace {

redisReply MakeStringReply(int type, std::string &backing) {
  redisReply reply{};
  reply.type = type;
  reply.str = backing.data();
  reply.len = static_cast<int>(backing.size());
  return reply;
}

redisReply MakeArrayReply(redisReply **elements, size_t count) {
  redisReply reply{};
  reply.type = REDIS_REPLY_ARRAY;
  reply.elements = count;
  reply.element = elements;
  return reply;
}

}  // namespace

// ResponsePayloadBytes is the normative definition of
// gcs_redis_response_payload_bytes: application bytes carried by every reply
// node, recursing into arrays and excluding RESP framing.
// These cases pin that definition; changing any expectation here changes what
// the exported metric means.
TEST(TestResponsePayloadBytes, ScalarReplies) {
  {
    redisReply nil{};
    nil.type = REDIS_REPLY_NIL;
    ASSERT_EQ(ResponsePayloadBytes(nil), 0u);
  }
  {
    // RESP integers carry their ASCII decimal form between ':' and CRLF.
    redisReply integer{};
    integer.type = REDIS_REPLY_INTEGER;
    integer.integer = 0;
    ASSERT_EQ(ResponsePayloadBytes(integer), 1u);
    integer.integer = 1;
    ASSERT_EQ(ResponsePayloadBytes(integer), 1u);
    integer.integer = -12345;
    ASSERT_EQ(ResponsePayloadBytes(integer), 6u);
    integer.integer = std::numeric_limits<long long>::max();
    ASSERT_EQ(ResponsePayloadBytes(integer), 19u);
    integer.integer = std::numeric_limits<long long>::min();
    ASSERT_EQ(ResponsePayloadBytes(integer), 20u);
  }
  {
    std::string decimal = "-12.5";
    redisReply number = MakeStringReply(REDIS_REPLY_DOUBLE, decimal);
    ASSERT_EQ(ResponsePayloadBytes(number), decimal.size());
  }
  {
    redisReply boolean{};
    boolean.type = REDIS_REPLY_BOOL;
    boolean.integer = 1;
    ASSERT_EQ(ResponsePayloadBytes(boolean), 1u);
  }
  {
    // PING replies "+PONG\r\n"; `len` is the decoded 4, because the leading '+'
    // and the trailing CRLF are framing.
    std::string pong = "PONG";
    redisReply status = MakeStringReply(REDIS_REPLY_STATUS, pong);
    ASSERT_EQ(ResponsePayloadBytes(status), 4u);
  }
  {
    std::string value(1024, 'x');
    redisReply str = MakeStringReply(REDIS_REPLY_STRING, value);
    ASSERT_EQ(ResponsePayloadBytes(str), 1024u);
  }
  {
    // Measured like any other string, but this shape only arises for an error
    // nested inside an aggregate reply: RedisResponseFn retries a top-level
    // error instead of delivering it, so gcs_redis_response_payload_bytes
    // documents error replies as contributing nothing.
    std::string error = "ERR unknown command";
    redisReply err = MakeStringReply(REDIS_REPLY_ERROR, error);
    ASSERT_EQ(ResponsePayloadBytes(err), error.size());
  }
}

TEST(TestResponsePayloadBytes, EmptyArrayDoesNotDereferenceElements) {
  // hiredis leaves `element` uninitialized when `elements` is 0. Point it at
  // garbage to prove the loop bound, not the pointer, is what stops us.
  redisReply empty{};
  empty.type = REDIS_REPLY_ARRAY;
  empty.elements = 0;
  empty.element = reinterpret_cast<redisReply **>(0xdeadbeef);
  ASSERT_EQ(ResponsePayloadBytes(empty), 0u);
}

TEST(TestResponsePayloadBytes, HmgetArrayCountsOnlyPresentValues) {
  // A partially-missed HMGET: absent fields come back as nil and contribute
  // nothing, so the counter measures data actually returned, not requested.
  std::string present1(100, 'a');
  std::string present2(250, 'b');
  redisReply value1 = MakeStringReply(REDIS_REPLY_STRING, present1);
  redisReply value2 = MakeStringReply(REDIS_REPLY_STRING, present2);
  redisReply missing{};
  missing.type = REDIS_REPLY_NIL;

  redisReply *elements[3] = {&value1, &missing, &value2};
  redisReply array = MakeArrayReply(elements, 3);
  ASSERT_EQ(ResponsePayloadBytes(array), 350u);
}

TEST(TestResponsePayloadBytes, ScanArrayCountsCursorAndFieldNames) {
  // HSCAN replies [cursor, [field, value, field, value]]. The cursor is a bulk
  // string in RESP2 so its bytes count, and the returned field names count too
  // -- both facts are stated in the metric description.
  std::string cursor = "1234";
  std::string field1 = "field-one";
  std::string value1(64, 'v');
  std::string field2 = "field-two";
  std::string value2(128, 'w');

  redisReply cursor_reply = MakeStringReply(REDIS_REPLY_STRING, cursor);
  redisReply field1_reply = MakeStringReply(REDIS_REPLY_STRING, field1);
  redisReply value1_reply = MakeStringReply(REDIS_REPLY_STRING, value1);
  redisReply field2_reply = MakeStringReply(REDIS_REPLY_STRING, field2);
  redisReply value2_reply = MakeStringReply(REDIS_REPLY_STRING, value2);

  redisReply *pairs[4] = {&field1_reply, &value1_reply, &field2_reply, &value2_reply};
  redisReply pairs_reply = MakeArrayReply(pairs, 4);
  redisReply *outer[2] = {&cursor_reply, &pairs_reply};
  redisReply scan_reply = MakeArrayReply(outer, 2);

  const size_t expected =
      cursor.size() + field1.size() + value1.size() + field2.size() + value2.size();
  ASSERT_EQ(ResponsePayloadBytes(scan_reply), expected);
  ASSERT_EQ(expected, 4u + 9u + 64u + 9u + 128u);
}

TEST(TestNormalizeRedisCommandLabel, PreservesKnownCommands) {
  const std::vector<std::string> known_commands{"HSET",
                                                "HSETNX",
                                                "HGET",
                                                "HMGET",
                                                "HDEL",
                                                "HEXISTS",
                                                "HSCAN",
                                                "SCAN",
                                                "INCRBY",
                                                "DEL",
                                                "UNLINK",
                                                "PING",
                                                "INFO"};
  for (const auto &command : known_commands) {
    EXPECT_EQ(NormalizeRedisCommandLabel(command), command);
  }
}

TEST(TestNormalizeRedisCommandLabel, UppercasesAsciiVerb) {
  EXPECT_EQ(NormalizeRedisCommandLabel("hSeTnX"), "HSETNX");
}

TEST(TestNormalizeRedisCommandLabel, UnknownCommandsMapToOther) {
  EXPECT_EQ(NormalizeRedisCommandLabel(""), kOtherRedisCommandLabel);
  EXPECT_EQ(NormalizeRedisCommandLabel("HGETALL"), kOtherRedisCommandLabel);
  EXPECT_EQ(NormalizeRedisCommandLabel(std::string(128, 'x')), kOtherRedisCommandLabel);
}
}  // namespace ray::gcs
