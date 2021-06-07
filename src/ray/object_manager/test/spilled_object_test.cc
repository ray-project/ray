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

#include "ray/object_manager/spilled_object.h"

#include <boost/endian/conversion.hpp>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

TEST(SpilledObjectTest, ParseObjectURL) {
  auto assert_parse_success =
      [](const std::string &object_url, const std::string &expected_file_path,
         uint64_t expected_object_offset, uint64_t expected_total_size) {
        std::string actual_file_path;
        uint64_t actual_offset = 0;
        uint64_t actual_size = 0;
        ASSERT_TRUE(SpilledObject::ParseObjectURL(object_url, actual_file_path,
                                                  actual_offset, actual_size));
        ASSERT_EQ(expected_file_path, actual_file_path);
        ASSERT_EQ(expected_object_offset, actual_offset);
        ASSERT_EQ(expected_total_size, actual_size);
      };

  auto assert_parse_fail = [](const std::string &object_url) {
    std::string actual_file_path;
    uint64_t actual_offset = 0;
    uint64_t actual_size = 0;
    ASSERT_FALSE(SpilledObject::ParseObjectURL(object_url, actual_file_path,
                                               actual_offset, actual_size));
  };

  assert_parse_success("file://path/to/file?offset=123&size=456", "file://path/to/file",
                       123, 456);
  assert_parse_success("http://123?offset=123&size=456", "http://123", 123, 456);
  assert_parse_success("file:///C:/Users/file.txt?offset=123&size=456",
                       "file:///C:/Users/file.txt", 123, 456);
  assert_parse_success("/tmp/file.txt?offset=123&size=456", "/tmp/file.txt", 123, 456);
  assert_parse_success("C:\\file.txt?offset=123&size=456", "C:\\file.txt", 123, 456);

  assert_parse_fail("file://path/to/file?offset=a&size=456");
  assert_parse_fail("file://path/to/file?offset=0&size=bb");
  assert_parse_fail("file://path/to/file?offset=123");
  assert_parse_fail("file://path/to/file?offset=a&size=456&extra");
}

TEST(SpilledObjectTest, ToUINT64) {
  ASSERT_EQ(0, SpilledObject::ToUINT64(
                   {'\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}));
  ASSERT_EQ(1, SpilledObject::ToUINT64(
                   {'\x01', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}));
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
            SpilledObject::ToUINT64(
                {'\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}));
}

TEST(SpilledObjectTest, ReadUINT64) {
  std::istringstream s1(std::string{
      '\x00', '\x00', '\x00', '\x00', '\x00', '\x00',
      '\x00', '\x00',  // little endian of 0
      '\x01', '\x00', '\x00', '\x00', '\x00', '\x00',
      '\x00', '\x00',  // little endian of 1
      '\xff', '\xff', '\xff', '\xff', '\xff', '\xff',
      '\xff', '\xff',                                 // little endian of 2^64 - 1
      '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'  // ill format
  });
  uint64_t output{100};
  ASSERT_TRUE(SpilledObject::ReadUINT64(s1, output));
  ASSERT_EQ(0, output);
  ASSERT_TRUE(SpilledObject::ReadUINT64(s1, output));
  ASSERT_EQ(1, output);
  ASSERT_TRUE(SpilledObject::ReadUINT64(s1, output));
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), output);
  ASSERT_FALSE(SpilledObject::ReadUINT64(s1, output));
}

namespace {
std::string ContructObjectString(uint64_t object_offset, std::string data,
                                 std::string metadata, rpc::Address owner_address) {
  std::string result(object_offset, '\0');
  std::string address_str;
  owner_address.SerializeToString(&address_str);
  uint64_t address_size = boost::endian::native_to_little(address_str.size());
  uint64_t data_size = boost::endian::native_to_little(data.size());
  uint64_t metadata_size = boost::endian::native_to_little(metadata.size());

  result.append((char *)(&address_size), 8);
  result.append((char *)(&metadata_size), 8);
  result.append((char *)(&data_size), 8);
  result.append(address_str);
  result.append(metadata);
  result.append(data);
  return result;
}
}  // namespace

TEST(SpilledObjectTest, ParseObjectHeader) {
  auto assert_parse_success = [](uint64_t object_offset, std::string data,
                                 std::string metadata, std::string raylet_id) {
    rpc::Address owner_address;
    owner_address.set_raylet_id(raylet_id);
    auto str = ContructObjectString(object_offset, data, metadata, owner_address);
    uint64_t actual_data_offset = 0;
    uint64_t actual_data_size = 0;
    uint64_t actual_metadata_offset = 0;
    uint64_t actual_metadata_size = 0;
    rpc::Address actual_owner_address;
    std::istringstream is(str);
    SpilledObject::ParseObjectHeader(is, object_offset, actual_data_offset,
                                     actual_data_size, actual_metadata_offset,
                                     actual_metadata_size, actual_owner_address);
    std::string address_str;
    owner_address.SerializeToString(&address_str);
    ASSERT_EQ(object_offset + 24 + address_str.size(), actual_metadata_offset);
    ASSERT_EQ(object_offset + 24 + address_str.size() + metadata.size(),
              actual_data_offset);
    ASSERT_EQ(data.size(), actual_data_size);
    ASSERT_EQ(metadata.size(), actual_metadata_size);
    ASSERT_EQ(owner_address.raylet_id(), actual_owner_address.raylet_id());
    ASSERT_EQ(data, str.substr(actual_data_offset, actual_data_size));
    ASSERT_EQ(metadata, str.substr(actual_metadata_offset, actual_metadata_size));
  };
  std::vector<uint64_t> offsets{0, 1, 100};
  std::string large_data(10000, 'c');
  std::vector<std::string> data_list{"", "somedata", large_data};
  std::string large_metadata(10000, 'm');
  std::vector<std::string> metadata_list{"", "somemetadata", large_metadata};
  std::vector<std::string> raylet_ids{"", "yes", "laaaaaaaarrrrrggge"};

  for (auto offset : offsets) {
    for (auto &data : data_list) {
      for (auto &metadata : metadata_list) {
        for (auto &raylet_id : raylet_ids) {
          assert_parse_success(offset, data, metadata, raylet_id);
        }
      }
    }
  }
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
