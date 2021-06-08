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
#include <fstream>

#include "absl/strings/str_format.h"
#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/util/filesystem.h"

namespace ray {

TEST(SpilledObjectTest, ParseObjectURL) {
  auto assert_parse_success =
      [](const std::string &object_url, const std::string &expected_file_path,
         uint64_t expected_object_offset, uint64_t expected_object_size) {
        std::string actual_file_path;
        uint64_t actual_offset = 0;
        uint64_t actual_size = 0;
        ASSERT_TRUE(SpilledObject::ParseObjectURL(object_url, actual_file_path,
                                                  actual_offset, actual_size));
        ASSERT_EQ(expected_file_path, actual_file_path);
        ASSERT_EQ(expected_object_offset, actual_offset);
        ASSERT_EQ(expected_object_size, actual_size);
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
      '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'  // malformed
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
    ASSERT_TRUE(SpilledObject::ParseObjectHeader(
        is, object_offset, actual_data_offset, actual_data_size, actual_metadata_offset,
        actual_metadata_size, actual_owner_address));
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

  auto assert_parse_failure = [](uint64_t object_offset, uint64_t truncate_size) {
    std::string data("data");
    std::string metadata("metadata");
    rpc::Address owner_address;
    auto str = ContructObjectString(object_offset, data, metadata, owner_address);
    str = str.substr(0, truncate_size);
    uint64_t actual_data_offset = 0;
    uint64_t actual_data_size = 0;
    uint64_t actual_metadata_offset = 0;
    uint64_t actual_metadata_size = 0;
    rpc::Address actual_owner_address;
    std::istringstream is(str);
    ASSERT_FALSE(SpilledObject::ParseObjectHeader(
        is, object_offset, actual_data_offset, actual_data_size, actual_metadata_offset,
        actual_metadata_size, actual_owner_address));
  };

  std::string address_str;
  rpc::Address owner_address;
  owner_address.SerializeToString(&address_str);
  for (uint64_t truncate_len = 98; truncate_len < 124 + address_str.size();
       truncate_len++) {
    assert_parse_failure(100, truncate_len);
  }
}

TEST(SpilledObjectTest, Getters) {
  rpc::Address owner_address;
  owner_address.set_raylet_id("nonsense");
  SpilledObject obj("path", 8 /* object_size */, 2 /* data_offset */, 3 /* data_size */,
                    4 /* metadata_offset */, 5 /* metadata_size */, owner_address,
                    6 /* chunk_size */);
  ASSERT_EQ(3, obj.GetDataSize());
  ASSERT_EQ(5, obj.GetMetadataSize());
  ASSERT_EQ(owner_address.raylet_id(), obj.GetOwnerAddress().raylet_id());
  ASSERT_EQ(2, obj.GetNumChunks());
}

TEST(SpilledObjectTest, GetNumChunks) {
  auto assert_get_num_chunks = [](uint64_t data_size, uint64_t chunk_size,
                                  uint64_t expected_num_chunks) {
    rpc::Address owner_address;
    owner_address.set_raylet_id("nonsense");
    SpilledObject obj("path", 100 /* object_size */, 2 /* data_offset */,
                      data_size /* data_size */, 4 /* metadata_offset */,
                      0 /* metadata_size */, owner_address, chunk_size /* chunk_size */);

    ASSERT_EQ(expected_num_chunks, obj.GetNumChunks());
  };

  assert_get_num_chunks(11 /* data_size */, 1 /* chunk_size */,
                        11 /* expected_num_chunks */);
  assert_get_num_chunks(1 /* data_size */, 11 /* chunk_size */,
                        1 /* expected_num_chunks */);
  assert_get_num_chunks(0 /* data_size */, 11 /* chunk_size */,
                        0 /* expected_num_chunks */);
  assert_get_num_chunks(9 /* data_size */, 2 /* chunk_size */,
                        5 /* expected_num_chunks */);
  assert_get_num_chunks(10 /* data_size */, 2 /* chunk_size */,
                        5 /* expected_num_chunks */);
  assert_get_num_chunks(11 /* data_size */, 2 /* chunk_size */,
                        6 /* expected_num_chunks */);
}

namespace {
std::string CreateSpilledObjectOnTmp(uint64_t object_offset, std::string data,
                                     std::string metadata, rpc::Address owner_address,
                                     bool skip_write = false) {
  auto str = ContructObjectString(object_offset, data, metadata, owner_address);
  std::string tmp_file = ray::JoinPaths(
      ray::GetUserTempDir(), "spilled_object_test" + ObjectID::FromRandom().Hex());

  std::ofstream f(tmp_file, std::ios::binary);
  if (!skip_write) {
    RAY_CHECK(f.write(str.c_str(), str.size()));
  }
  f.close();
  return absl::StrFormat("%s?offset=%d&size=%d", tmp_file, object_offset,
                         str.size() - object_offset);
}
}  // namespace

TEST(SpilledObjectTest, CreateSpilledObject) {
  auto object_url = CreateSpilledObjectOnTmp(10 /* object_offset */, "data", "metadata",
                                             ray::rpc::Address());
  // 0 chunk_size.
  ASSERT_FALSE(
      SpilledObject::CreateSpilledObject(object_url, 0 /* chunk_size */).has_value());
  ASSERT_FALSE(SpilledObject::CreateSpilledObject("malformatted_url", 1 /* chunk_size */)
                   .has_value());
  auto optional_object =
      SpilledObject::CreateSpilledObject(object_url, 2 /* chunk_size */);
  ASSERT_TRUE(optional_object.has_value());

  auto object_url1 = CreateSpilledObjectOnTmp(10 /* object_offset */, "data", "metadata",
                                              ray::rpc::Address(), true /* skip_write */);
  // file corrupted.
  ASSERT_FALSE(
      SpilledObject::CreateSpilledObject(object_url1, 2 /* chunk_size */).has_value());
}

namespace {
void AssertGetChunkWorks(std::string metadata, std::string data,
                         std::vector<uint64_t> chunk_sizes) {
  std::string expected_output = data + metadata;
  chunk_sizes.push_back(expected_output.size());
  auto object_url = CreateSpilledObjectOnTmp(10 /* object_offset */, data, metadata,
                                             ray::rpc::Address());

  // check that we can reconstruct the output by concatinating chunks with different
  // chunk_size, and the size of chunk is expected.
  for (auto chunk_size : chunk_sizes) {
    auto optional_object = SpilledObject::CreateSpilledObject(object_url, chunk_size);
    ASSERT_TRUE(optional_object.has_value());
    std::string actual_output_by_chunks;
    for (uint64_t i = 0; i < optional_object->GetNumChunks(); i++) {
      auto chunk = optional_object->GetChunk(i);
      ASSERT_TRUE(chunk.has_value());
      ASSERT_GE(chunk_size, chunk->size());
      if (i + 1 != optional_object->GetNumChunks()) {
        ASSERT_EQ(chunk_size, chunk->size());
      }
      actual_output_by_chunks.append(chunk.value());
    }
    ASSERT_EQ(expected_output, actual_output_by_chunks);
  }
}
}  // namespace

TEST(SpilledObjectTest, GetChunk) {
  AssertGetChunkWorks("meta", "alotofdata", {1, 2, 3, 5, 100});
  AssertGetChunkWorks("alotofactualmeta", "meh", {1, 2, 3, 5, 100});
  AssertGetChunkWorks("", "weonlyhavedata", {1, 2, 3, 5, 100});
  AssertGetChunkWorks("weonlyhavemetadata", "", {1, 2, 3, 5, 100});
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
