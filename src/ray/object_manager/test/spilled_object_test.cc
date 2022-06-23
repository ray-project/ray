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

#include <boost/endian/conversion.hpp>
#include <fstream>

#include "absl/strings/str_format.h"
#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/object_manager/chunk_object_reader.h"
#include "ray/object_manager/memory_object_reader.h"
#include "ray/object_manager/spilled_object_reader.h"
#include "ray/util/filesystem.h"

namespace ray {

TEST(SpilledObjectReaderTest, ParseObjectURL) {
  auto assert_parse_success = [](const std::string &object_url,
                                 const std::string &expected_file_path,
                                 uint64_t expected_object_offset,
                                 uint64_t expected_object_size) {
    std::string actual_file_path;
    uint64_t actual_offset = 0;
    uint64_t actual_size = 0;
    ASSERT_TRUE(SpilledObjectReader::ParseObjectURL(
        object_url, actual_file_path, actual_offset, actual_size));
    ASSERT_EQ(expected_file_path, actual_file_path);
    ASSERT_EQ(expected_object_offset, actual_offset);
    ASSERT_EQ(expected_object_size, actual_size);
  };

  auto assert_parse_fail = [](const std::string &object_url) {
    std::string actual_file_path;
    uint64_t actual_offset = 0;
    uint64_t actual_size = 0;
    ASSERT_FALSE(SpilledObjectReader::ParseObjectURL(
        object_url, actual_file_path, actual_offset, actual_size));
  };

  assert_parse_success(
      "file://path/to/file?offset=123&size=456", "file://path/to/file", 123, 456);
  assert_parse_success("http://123?offset=123&size=456", "http://123", 123, 456);
  assert_parse_success("file:///C:/Users/file.txt?offset=123&size=456",
                       "file:///C:/Users/file.txt",
                       123,
                       456);
  assert_parse_success("/tmp/file.txt?offset=123&size=456", "/tmp/file.txt", 123, 456);
  assert_parse_success("C:\\file.txt?offset=123&size=456", "C:\\file.txt", 123, 456);
  assert_parse_success(
      "/tmp/ray/session_2021-07-19_09-50-58_115365_119/ray_spillled_objects/"
      "2f81e7cfcc578f4effffffffffffffffffffffff0200000001000000-multi-1?offset=0&size="
      "2199437144",
      "/tmp/ray/session_2021-07-19_09-50-58_115365_119/ray_spillled_objects/"
      "2f81e7cfcc578f4effffffffffffffffffffffff0200000001000000-multi-1",
      0,
      2199437144);
  assert_parse_success(
      "/tmp/123?offset=0&size=9223372036854775807", "/tmp/123", 0, 9223372036854775807);

  assert_parse_fail("/tmp/123?offset=-1&size=1");
  assert_parse_fail("/tmp/123?offset=0&size=9223372036854775808");
  assert_parse_fail("file://path/to/file?offset=a&size=456");
  assert_parse_fail("file://path/to/file?offset=0&size=bb");
  assert_parse_fail("file://path/to/file?offset=123");
  assert_parse_fail("file://path/to/file?offset=a&size=456&extra");
}

TEST(SpilledObjectReaderTest, ToUINT64) {
  ASSERT_EQ(0,
            SpilledObjectReader::ToUINT64(
                {'\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}));
  ASSERT_EQ(1,
            SpilledObjectReader::ToUINT64(
                {'\x01', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}));
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
            SpilledObjectReader::ToUINT64(
                {'\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}));
}

TEST(SpilledObjectReaderTest, ReadUINT64) {
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
  ASSERT_TRUE(SpilledObjectReader::ReadUINT64(s1, output));
  ASSERT_EQ(0, output);
  ASSERT_TRUE(SpilledObjectReader::ReadUINT64(s1, output));
  ASSERT_EQ(1, output);
  ASSERT_TRUE(SpilledObjectReader::ReadUINT64(s1, output));
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), output);
  ASSERT_FALSE(SpilledObjectReader::ReadUINT64(s1, output));
}

namespace {
std::string ContructObjectString(uint64_t object_offset,
                                 std::string data,
                                 std::string metadata,
                                 rpc::Address owner_address) {
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

TEST(SpilledObjectReaderTest, ParseObjectHeader) {
  auto assert_parse_success = [](uint64_t object_offset,
                                 std::string data,
                                 std::string metadata,
                                 std::string raylet_id) {
    rpc::Address owner_address;
    owner_address.set_raylet_id(raylet_id);
    auto str = ContructObjectString(object_offset, data, metadata, owner_address);
    uint64_t actual_data_offset = 0;
    uint64_t actual_data_size = 0;
    uint64_t actual_metadata_offset = 0;
    uint64_t actual_metadata_size = 0;
    rpc::Address actual_owner_address;
    std::istringstream is(str);
    ASSERT_TRUE(SpilledObjectReader::ParseObjectHeader(is,
                                                       object_offset,
                                                       actual_data_offset,
                                                       actual_data_size,
                                                       actual_metadata_offset,
                                                       actual_metadata_size,
                                                       actual_owner_address));
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
    ASSERT_FALSE(SpilledObjectReader::ParseObjectHeader(is,
                                                        object_offset,
                                                        actual_data_offset,
                                                        actual_data_size,
                                                        actual_metadata_offset,
                                                        actual_metadata_size,
                                                        actual_owner_address));
  };

  std::string address_str;
  rpc::Address owner_address;
  owner_address.SerializeToString(&address_str);
  for (uint64_t truncate_len = 98; truncate_len < 124 + address_str.size();
       truncate_len++) {
    assert_parse_failure(100, truncate_len);
  }
}

namespace {
std::string CreateSpilledObjectReaderOnTmp(uint64_t object_offset,
                                           std::string data,
                                           std::string metadata,
                                           rpc::Address owner_address,
                                           bool skip_write = false) {
  auto str = ContructObjectString(object_offset, data, metadata, owner_address);
  std::string tmp_file = ray::JoinPaths(
      ray::GetUserTempDir(), "spilled_object_test" + ObjectID::FromRandom().Hex());

  std::ofstream f(tmp_file, std::ios::binary);
  if (!skip_write) {
    RAY_CHECK(f.write(str.c_str(), str.size()));
  }
  f.close();
  return absl::StrFormat(
      "%s?offset=%d&size=%d", tmp_file, object_offset, str.size() - object_offset);
}

MemoryObjectReader CreateMemoryObjectReader(std::string &data,
                                            std::string &metadata,
                                            rpc::Address owner_address) {
  plasma::ObjectBuffer object_buffer;
  object_buffer.data =
      std::make_shared<SharedMemoryBuffer>((uint8_t *)data.c_str(), data.size());
  object_buffer.metadata =
      std::make_shared<SharedMemoryBuffer>((uint8_t *)metadata.c_str(), metadata.size());
  object_buffer.device_num = 0;
  return MemoryObjectReader(std::move(object_buffer), owner_address);
}
}  // namespace

TEST(ChunkObjectReaderTest, GetNumChunks) {
  auto assert_get_num_chunks =
      [](uint64_t data_size, uint64_t chunk_size, uint64_t expected_num_chunks) {
        rpc::Address owner_address;
        owner_address.set_raylet_id("nonsense");
        ChunkObjectReader reader(std::make_shared<SpilledObjectReader>(
                                     SpilledObjectReader("path",
                                                         100 /* object_size */,
                                                         2 /* data_offset */,
                                                         data_size /* data_size */,
                                                         4 /* metadata_offset */,
                                                         0 /* metadata_size */,
                                                         owner_address)),
                                 chunk_size /* chunk_size */);

        ASSERT_EQ(expected_num_chunks, reader.GetNumChunks());
        ASSERT_EQ(expected_num_chunks, reader.GetNumChunks());
      };

  assert_get_num_chunks(
      11 /* data_size */, 1 /* chunk_size */, 11 /* expected_num_chunks */);
  assert_get_num_chunks(
      1 /* data_size */, 11 /* chunk_size */, 1 /* expected_num_chunks */);
  assert_get_num_chunks(
      0 /* data_size */, 11 /* chunk_size */, 0 /* expected_num_chunks */);
  assert_get_num_chunks(
      9 /* data_size */, 2 /* chunk_size */, 5 /* expected_num_chunks */);
  assert_get_num_chunks(
      10 /* data_size */, 2 /* chunk_size */, 5 /* expected_num_chunks */);
  assert_get_num_chunks(
      11 /* data_size */, 2 /* chunk_size */, 6 /* expected_num_chunks */);
}

TEST(SpilledObjectReaderTest, CreateSpilledObjectReader) {
  auto object_url = CreateSpilledObjectReaderOnTmp(
      10 /* object_offset */, "data", "metadata", ray::rpc::Address());
  ASSERT_TRUE(SpilledObjectReader::CreateSpilledObjectReader(object_url).has_value());
  ASSERT_FALSE(
      SpilledObjectReader::CreateSpilledObjectReader("malformatted_url").has_value());
  auto optional_object = SpilledObjectReader::CreateSpilledObjectReader(object_url);
  ASSERT_TRUE(optional_object.has_value());

  auto object_url1 = CreateSpilledObjectReaderOnTmp(10 /* object_offset */,
                                                    "data",
                                                    "metadata",
                                                    ray::rpc::Address(),
                                                    true /* skip_write */);
  // file corrupted.
  ASSERT_FALSE(SpilledObjectReader::CreateSpilledObjectReader(object_url1).has_value());
}

template <class T>
std::shared_ptr<T> CreateObjectReader(std::string &data,
                                      std::string &metadata,
                                      rpc::Address owner_address);

template <>
std::shared_ptr<SpilledObjectReader> CreateObjectReader<SpilledObjectReader>(
    std::string &data, std::string &metadata, rpc::Address owner_address) {
  auto object_url = CreateSpilledObjectReaderOnTmp(
      0 /* object_offset */, data, metadata, owner_address);
  auto optional_object = SpilledObjectReader::CreateSpilledObjectReader(object_url);
  return std::make_shared<SpilledObjectReader>(std::move(optional_object.value()));
}

template <>
std::shared_ptr<MemoryObjectReader> CreateObjectReader<MemoryObjectReader>(
    std::string &data, std::string &metadata, rpc::Address owner_address) {
  return std::make_shared<MemoryObjectReader>(
      CreateMemoryObjectReader(data, metadata, owner_address));
}

template <typename T>
struct ObjectReaderTest : public ::testing::Test {
  static std::shared_ptr<T> CreateObjectReader_(std::string &data,
                                                std::string &metadata,
                                                rpc::Address owner_address) {
    return CreateObjectReader<T>(data, metadata, owner_address);
  }
};

typedef ::testing::Types<SpilledObjectReader, MemoryObjectReader> Implementations;

TYPED_TEST_SUITE(ObjectReaderTest, Implementations);

TYPED_TEST(ObjectReaderTest, Getters) {
  std::string data("data");
  std::string metadata("metadata");
  rpc::Address owner_address;
  owner_address.set_raylet_id("nonsense");
  auto obj_reader = this->CreateObjectReader_(data, metadata, owner_address);
  ASSERT_EQ(data.size(), obj_reader->GetDataSize());
  ASSERT_EQ(metadata.size(), obj_reader->GetMetadataSize());
  ASSERT_EQ(data.size() + metadata.size(), obj_reader->GetObjectSize());
  ASSERT_EQ(owner_address.raylet_id(), obj_reader->GetOwnerAddress().raylet_id());
}

TYPED_TEST(ObjectReaderTest, GetDataAndMetadata) {
  std::vector<std::string> list_data{"", "alotofdata", "da", "data"};
  std::vector<std::string> list_metadata{"", "meta", "metadata", "alotofmetadata"};
  for (auto &data : list_data) {
    for (auto &metadata : list_metadata) {
      std::vector<uint64_t> chunk_sizes{1, 2, 3, 5, 100};
      rpc::Address owner_address;

      auto reader = TestFixture::CreateObjectReader_(data, metadata, owner_address);

      for (size_t offset = 0; offset <= data.size(); offset++) {
        for (size_t size = offset; size <= data.size() - offset; size++) {
          std::string result(size, '\0');
          if (offset + size <= data.size()) {
            ASSERT_TRUE(reader->ReadFromDataSection(offset, size, &result[0]));
            ASSERT_EQ(data.substr(offset, size), result);
          } else {
            ASSERT_FALSE(reader->ReadFromDataSection(offset, size, &result[0]));
          }
        }
      }

      for (size_t offset = 0; offset <= metadata.size(); offset++) {
        for (size_t size = offset; size <= metadata.size() - offset; size++) {
          std::string result(size, '\0');
          if (offset + size <= metadata.size()) {
            ASSERT_TRUE(reader->ReadFromMetadataSection(offset, size, &result[0]));
            ASSERT_EQ(metadata.substr(offset, size), result);
          } else {
            ASSERT_FALSE(reader->ReadFromMetadataSection(offset, size, &result[0]));
          }
        }
      }
    }
  }
}

TYPED_TEST(ObjectReaderTest, GetChunk) {
  std::vector<std::string> list_data{"", "alotofdata", "da", "data"};
  std::vector<std::string> list_metadata{"", "meta", "metadata", "alotofmetadata"};
  for (auto &data : list_data) {
    for (auto &metadata : list_metadata) {
      std::vector<uint64_t> chunk_sizes{1, 2, 3, 5, 100};
      rpc::Address owner_address;
      owner_address.set_raylet_id("nonsense");

      std::string expected_output = data + metadata;
      if (expected_output.size() != 0) {
        chunk_sizes.push_back(expected_output.size());
      }

      // check that we can reconstruct the output by concatinating chunks with different
      // chunk_size, and the size of chunk is expected.
      for (auto chunk_size : chunk_sizes) {
        auto reader = ChunkObjectReader(
            TestFixture::CreateObjectReader_(data, metadata, owner_address), chunk_size);

        std::string actual_output_by_chunks;
        for (uint64_t i = 0; i < reader.GetNumChunks(); i++) {
          auto chunk = reader.GetChunk(i);
          ASSERT_TRUE(chunk.has_value());
          ASSERT_GE(chunk_size, chunk->size());
          if (i + 1 != reader.GetNumChunks()) {
            ASSERT_EQ(chunk_size, chunk->size());
          }
          actual_output_by_chunks.append(chunk.value());
        }
        ASSERT_EQ(expected_output, actual_output_by_chunks);
      }
    }
  }
}

TEST(StringAllocationTest, TestNoCopyWhenStringMoved) {
  // Since protobuf always allocate string on heap,
  // move assign a string field doesn't copy the data.
  std::string s(1000, '\0');
  auto allocation_address = s.c_str();
  rpc::Address address;
  address.set_raylet_id(std::move(s));
  EXPECT_EQ(allocation_address, address.raylet_id().c_str());
}

TEST(StringAllocationTest, TestCopyWhenPassByPointer) {
  // Construct a string field by pointer and length
  // always copy the data.
  char arr[1000];
  auto allocation_address = &arr[0];
  rpc::Address address;
  address.set_raylet_id(allocation_address, 1000);
  EXPECT_NE(allocation_address, address.raylet_id().c_str());
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
