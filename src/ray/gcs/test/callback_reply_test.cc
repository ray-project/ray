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

#include "gtest/gtest.h"
#include "ray/gcs/redis_context.h"

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
    CallbackReply callback_reply(&redis_reply_array);
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
    CallbackReply callback_reply(&redis_reply_array);
    ASSERT_EQ(
        callback_reply.ReadAsStringArray(),
        (std::vector<std::optional<std::string>>{std::optional<std::string>(),
                                                 std::optional<std::string>(string1),
                                                 std::optional<std::string>()}));
  }
}
}  // namespace ray::gcs

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}