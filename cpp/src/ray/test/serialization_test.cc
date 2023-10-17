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

bool IsAnyEqual(const std::any &lhs, const std::any &rhs) {
  if (lhs.type() != rhs.type()) {
    return false;
  }
  if (lhs.type() == typeid(uint64_t)) {
    return std::any_cast<uint64_t>(lhs) == std::any_cast<uint64_t>(rhs);
  } else if (lhs.type() == typeid(int64_t)) {
    return std::any_cast<int64_t>(lhs) == std::any_cast<int64_t>(rhs);
  } else if (lhs.type() == typeid(bool)) {
    return std::any_cast<bool>(lhs) == std::any_cast<bool>(rhs);
  } else if (lhs.type() == typeid(float)) {
    return std::any_cast<float>(lhs) == std::any_cast<float>(rhs);
  } else if (lhs.type() == typeid(double)) {
    return std::any_cast<double>(lhs) == std::any_cast<double>(rhs);
  } else if (lhs.type() == typeid(std::string)) {
    return std::any_cast<std::string>(lhs) == std::any_cast<std::string>(rhs);
  } else if (lhs.type() == typeid(std::vector<char>)) {
    return std::any_cast<std::vector<char>>(lhs) == std::any_cast<std::vector<char>>(rhs);
  } else {
    return false;
  }
}

TEST(SerializationTest, TypeHybridTest) {
  uint32_t in_arg1 = 123456789, out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;
  double in_arg3 = 1.1;
  float in_arg4 = 1.2;
  uint64_t in_arg5 = 100;
  int64_t in_arg6 = -100;
  bool in_arg7 = false;
  std::vector<char> in_arg8 = {'a', 'b'};

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

// TODO(Larry Lian) Adapt on windows
#ifndef _WIN32
  // Test std::any
  msgpack::sbuffer buffer3 = ray::internal::Serializer::Serialize(std::make_tuple(
      in_arg1, in_arg2, in_arg3, in_arg4, in_arg5, in_arg6, in_arg7, in_arg8));

  std::vector<std::any> result =
      ray::internal::Serializer::Deserialize<std::vector<std::any>>(buffer3.data(),
                                                                    buffer3.size());
  EXPECT_TRUE(result[0].type() == typeid(uint64_t));
  EXPECT_EQ(std::any_cast<uint64_t>(result[0]), in_arg1);
  EXPECT_TRUE(result[1].type() == typeid(std::string));
  EXPECT_EQ(std::any_cast<std::string>(result[1]), in_arg2);
  EXPECT_TRUE(result[2].type() == typeid(double));
  EXPECT_EQ(std::any_cast<double>(result[2]), in_arg3);
  EXPECT_TRUE(result[3].type() == typeid(double));
  EXPECT_EQ(std::any_cast<double>(result[3]), in_arg4);
  EXPECT_TRUE(result[4].type() == typeid(uint64_t));
  EXPECT_EQ(std::any_cast<uint64_t>(result[4]), in_arg5);
  EXPECT_TRUE(result[5].type() == typeid(int64_t));
  EXPECT_EQ(std::any_cast<int64_t>(result[5]), in_arg6);
  EXPECT_TRUE(result[6].type() == typeid(bool));
  EXPECT_EQ(std::any_cast<bool>(result[6]), in_arg7);
  EXPECT_TRUE(result[7].type() == typeid(std::vector<char>));
  EXPECT_EQ(std::any_cast<std::vector<char>>(result[7]), in_arg8);

  msgpack::sbuffer buffer4 = ray::internal::Serializer::Serialize(result);

  std::vector<std::any> result_2 =
      ray::internal::Serializer::Deserialize<std::vector<std::any>>(buffer4.data(),
                                                                    buffer4.size());
  EXPECT_TRUE(std::equal(result.begin(), result.end(), result_2.begin(), IsAnyEqual));
#endif
}

TEST(SerializationTest, BoundaryValueTest) {
  std::string in_arg1 = "", out_arg1;
  msgpack::sbuffer buffer1 = ray::internal::Serializer::Serialize(in_arg1);
  out_arg1 =
      ray::internal::Serializer::Deserialize<std::string>(buffer1.data(), buffer1.size());
  EXPECT_EQ(in_arg1, out_arg1);

  std::vector<std::byte> in_arg2, out_arg2;
  msgpack::sbuffer buffer2 = ray::internal::Serializer::Serialize(in_arg2);
  out_arg2 = ray::internal::Serializer::Deserialize<std::vector<std::byte>>(
      buffer1.data(), buffer1.size());
  EXPECT_EQ(in_arg2, out_arg2);

  char *in_arg3 = nullptr;
  msgpack::sbuffer buffer3 = ray::internal::Serializer::Serialize(in_arg3, 0);
  auto out_arg3 = ray::internal::Serializer::Deserialize<std::vector<std::byte>>(
      buffer1.data(), buffer1.size());
  EXPECT_EQ(std::vector<std::byte>(), out_arg3);
}