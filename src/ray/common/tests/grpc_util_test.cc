// Copyright 2017 The Ray Authors.
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

#include "ray/common/grpc_util.h"

#include <string>

#include "gtest/gtest.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
class GrpcUtilTest : public ::testing::Test {};

TEST_F(GrpcUtilTest, TestMapEqualMapSizeNotEqual) {
  google::protobuf::Map<std::string, double> map1;
  google::protobuf::Map<std::string, double> map2;
  map1["key1"] = 1.0;
  ASSERT_FALSE(MapEqual(map1, map2));
}

TEST_F(GrpcUtilTest, TestMapEqualMissingKey) {
  google::protobuf::Map<std::string, double> map1;
  google::protobuf::Map<std::string, double> map2;
  map1["key1"] = 1.0;
  map2["key2"] = 1.0;
  ASSERT_FALSE(MapEqual(map1, map2));
}

TEST_F(GrpcUtilTest, TestMapEqualSimpleTypeValueNotEqual) {
  google::protobuf::Map<std::string, double> map1;
  google::protobuf::Map<std::string, double> map2;
  map1["key1"] = 1.0;
  map2["key1"] = 2.0;
  ASSERT_FALSE(MapEqual(map1, map2));
}

TEST_F(GrpcUtilTest, TestMapEqualSimpleTypeEuqal) {
  google::protobuf::Map<std::string, double> map1;
  google::protobuf::Map<std::string, double> map2;
  map1["key1"] = 1.0;
  map2["key1"] = 1.0;
  ASSERT_TRUE(MapEqual(map1, map2));
}

TEST_F(GrpcUtilTest, TestMapEqualProtoMessageTypeNotEqual) {
  google::protobuf::Map<std::string, ray::rpc::LabelIn> map1;
  google::protobuf::Map<std::string, ray::rpc::LabelIn> map2;
  ray::rpc::LabelIn label_in_1;
  ray::rpc::LabelIn label_in_2;
  label_in_1.add_values("value1");
  label_in_2.add_values("value2");
  map1["key1"] = label_in_1;
  map2["key1"] = label_in_2;
  ASSERT_FALSE(MapEqual(map1, map2));
}

TEST_F(GrpcUtilTest, TestMapEqualProtoMessageTypeEqual) {
  google::protobuf::Map<std::string, ray::rpc::LabelIn> map1;
  google::protobuf::Map<std::string, ray::rpc::LabelIn> map2;
  ray::rpc::LabelIn label_in;
  label_in.add_values("value1");
  map1["key1"] = label_in;
  map2["key1"] = label_in;
  ASSERT_TRUE(MapEqual(map1, map2));
}

}  // namespace ray
