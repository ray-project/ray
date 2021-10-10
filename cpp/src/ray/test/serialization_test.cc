// Copyright 2020-2021 The Ray Authors.
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

#include <gtest/gtest.h>
#include <ray/api.h>

TEST(SerializationTest, TypeHybridTest) {
  uint32_t in_arg1 = 123456789, out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;

  // 1 arg
  // marshall
  msgpack::sbuffer buffer1 = ray::internal::Serializer::Serialize(in_arg1);
  // unmarshall
  out_arg1 =
      ray::internal::Serializer::Deserialize<uint32_t>(buffer1.data(), buffer1.size());

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2 =
      ray::internal::Serializer::Serialize(std::make_tuple(in_arg1, in_arg2));

  // unmarshall
  std::tie(out_arg1, out_arg2) =
      ray::internal::Serializer::Deserialize<std::tuple<uint32_t, std::string>>(
          buffer2.data(), buffer2.size());

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}