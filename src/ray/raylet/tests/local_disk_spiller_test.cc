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

#include "ray/raylet/local_disk_spiller.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/object_manager/spilled_object_reader.h"

namespace ray {
namespace raylet {

class LocalDiskSpillerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a temporary directory for testing.
    test_dir_ = std::filesystem::temp_directory_path() / "ray_spiller_test";
    std::filesystem::create_directories(test_dir_);

    node_id_ = NodeID::FromRandom();
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  /// Create a spiller with the given number of IO threads.
  std::unique_ptr<LocalDiskSpiller> CreateSpiller(const std::vector<std::string> &dirs,
                                                  int num_threads = 2) {
    return std::make_unique<LocalDiskSpiller>(
        dirs, node_id_, num_threads, main_service_, restore_to_plasma_fn_);
  }

  /// Create a RayObject with the given data and metadata strings.
  std::unique_ptr<RayObject> CreateObject(const std::string &data,
                                          const std::string &metadata = "") {
    auto data_buf = std::make_shared<LocalMemoryBuffer>(
        reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())),
        data.size(),
        /*copy_data=*/true);
    std::shared_ptr<Buffer> meta_buf = nullptr;
    if (!metadata.empty()) {
      meta_buf = std::make_shared<LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(const_cast<char *>(metadata.data())),
          metadata.size(),
          /*copy_data=*/true);
    }
    return std::make_unique<RayObject>(
        data_buf, meta_buf, std::vector<rpc::ObjectReference>(), /*copy_data=*/true);
  }

  /// Create a test owner address.
  rpc::Address CreateOwnerAddress(const std::string &worker_id = "test_worker") {
    rpc::Address addr;
    addr.set_node_id(node_id_.Binary());
    addr.set_ip_address("127.0.0.1");
    addr.set_port(12345);
    addr.set_worker_id(worker_id);
    return addr;
  }

  /// Run the main io_service until there are no more handlers to execute.
  void RunMainService() { main_service_.run(); }

  /// Reset and run the main io_service.
  void ResetAndRunMainService() {
    main_service_.restart();
    main_service_.run();
  }

  std::filesystem::path test_dir_;
  NodeID node_id_;
  instrumented_io_context main_service_;

  // Track restored objects for verification.
  struct RestoredObject {
    ObjectID object_id;
    rpc::Address owner_address;
    std::shared_ptr<Buffer> data;
    std::shared_ptr<Buffer> metadata;
  };
  std::vector<RestoredObject> restored_objects_;

  RestoreObjectToPlasmaFn restore_to_plasma_fn_ =
      [this](const ObjectID &object_id,
             const rpc::Address &owner_address,
             std::shared_ptr<Buffer> data,
             std::shared_ptr<Buffer> metadata) -> Status {
    restored_objects_.push_back(
        {object_id, owner_address, std::move(data), std::move(metadata)});
    return Status::OK();
  };
};

TEST_F(LocalDiskSpillerTest, SpillAndReadBackSingleObject) {
  auto spiller = CreateSpiller({test_dir_.string()});

  ObjectID obj_id = ObjectID::FromRandom();
  auto obj = CreateObject("hello world", "meta");
  auto owner = CreateOwnerAddress();

  std::vector<ObjectID> ids = {obj_id};
  std::vector<const RayObject *> objs = {obj.get()};
  std::vector<rpc::Address> owners = {owner};

  Status spill_status;
  std::vector<std::string> spill_urls;

  spiller->SpillObjects(
      ids, objs, owners, [&](const Status &s, std::vector<std::string> urls) {
        spill_status = s;
        spill_urls = std::move(urls);
      });

  // Wait for the callback to be posted to main_service_.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  ASSERT_TRUE(spill_status.ok()) << spill_status.ToString();
  ASSERT_EQ(spill_urls.size(), 1);

  // Verify the spilled object can be read back by SpilledObjectReader.
  auto reader = SpilledObjectReader::CreateSpilledObjectReader(spill_urls[0]);
  ASSERT_TRUE(reader.has_value());
  EXPECT_EQ(reader->GetDataSize(), 11);     // "hello world"
  EXPECT_EQ(reader->GetMetadataSize(), 4);  // "meta"

  // Read back data.
  std::string data_out;
  EXPECT_TRUE(reader->ReadFromDataSection(0, reader->GetDataSize(), data_out));
  EXPECT_EQ(data_out, "hello world");

  // Read back metadata.
  std::string meta_out;
  EXPECT_TRUE(reader->ReadFromMetadataSection(0, reader->GetMetadataSize(), meta_out));
  EXPECT_EQ(meta_out, "meta");

  // Read back owner address.
  EXPECT_EQ(reader->GetOwnerAddress().ip_address(), "127.0.0.1");
  EXPECT_EQ(reader->GetOwnerAddress().port(), 12345);
}

TEST_F(LocalDiskSpillerTest, SpillMultipleFusedObjects) {
  auto spiller = CreateSpiller({test_dir_.string()});

  std::vector<ObjectID> ids;
  std::vector<std::unique_ptr<RayObject>> obj_storage;
  std::vector<const RayObject *> objs;
  std::vector<rpc::Address> owners;

  for (int i = 0; i < 3; i++) {
    ids.push_back(ObjectID::FromRandom());
    obj_storage.push_back(
        CreateObject("data_" + std::to_string(i), "meta_" + std::to_string(i)));
    objs.push_back(obj_storage.back().get());
    owners.push_back(CreateOwnerAddress("worker_" + std::to_string(i)));
  }

  Status spill_status;
  std::vector<std::string> spill_urls;

  spiller->SpillObjects(
      ids, objs, owners, [&](const Status &s, std::vector<std::string> urls) {
        spill_status = s;
        spill_urls = std::move(urls);
      });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  ASSERT_TRUE(spill_status.ok());
  ASSERT_EQ(spill_urls.size(), 3);

  // Verify each object is readable with correct offsets.
  for (int i = 0; i < 3; i++) {
    auto reader = SpilledObjectReader::CreateSpilledObjectReader(spill_urls[i]);
    ASSERT_TRUE(reader.has_value()) << "Failed to read object " << i;

    std::string expected_data = "data_" + std::to_string(i);
    std::string expected_meta = "meta_" + std::to_string(i);
    EXPECT_EQ(reader->GetDataSize(), expected_data.size());
    EXPECT_EQ(reader->GetMetadataSize(), expected_meta.size());

    std::string data_out;
    EXPECT_TRUE(reader->ReadFromDataSection(0, reader->GetDataSize(), data_out));
    EXPECT_EQ(data_out, expected_data);

    std::string meta_out;
    EXPECT_TRUE(reader->ReadFromMetadataSection(0, reader->GetMetadataSize(), meta_out));
    EXPECT_EQ(meta_out, expected_meta);
  }

  // All URLs should reference the same file (fused).
  auto base_url = [](const std::string &url) { return url.substr(0, url.find('?')); };
  EXPECT_EQ(base_url(spill_urls[0]), base_url(spill_urls[1]));
  EXPECT_EQ(base_url(spill_urls[1]), base_url(spill_urls[2]));
}

TEST_F(LocalDiskSpillerTest, DirectoryRoundRobin) {
  // Create two spill directories.
  std::string dir1 = (test_dir_ / "dir1").string();
  std::string dir2 = (test_dir_ / "dir2").string();
  std::filesystem::create_directories(dir1);
  std::filesystem::create_directories(dir2);

  auto spiller = CreateSpiller({dir1, dir2});

  std::vector<std::string> all_urls;

  // Spill 4 objects, each in a separate spill call.
  for (int i = 0; i < 4; i++) {
    ObjectID obj_id = ObjectID::FromRandom();
    auto obj = CreateObject("data_" + std::to_string(i));
    auto owner = CreateOwnerAddress();

    std::vector<ObjectID> ids = {obj_id};
    std::vector<const RayObject *> objs = {obj.get()};
    std::vector<rpc::Address> owners = {owner};

    spiller->SpillObjects(
        ids, objs, owners, [&all_urls](const Status &s, std::vector<std::string> urls) {
          for (auto &u : urls) {
            all_urls.push_back(std::move(u));
          }
        });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ResetAndRunMainService();

  ASSERT_EQ(all_urls.size(), 4);

  // Extract file paths and verify they alternate between directories.
  auto extract_path = [](const std::string &url) { return url.substr(0, url.find('?')); };

  int dir1_count = 0, dir2_count = 0;
  std::string node_hex_subdir = "ray_spilled_objects_" + node_id_.Hex();
  for (const auto &url : all_urls) {
    std::string path = extract_path(url);
    if (path.find(dir1) != std::string::npos) {
      dir1_count++;
    } else if (path.find(dir2) != std::string::npos) {
      dir2_count++;
    }
    // Verify the per-node subdirectory is used.
    EXPECT_NE(path.find(node_hex_subdir), std::string::npos);
  }

  // Both directories should be used.
  EXPECT_EQ(dir1_count, 2);
  EXPECT_EQ(dir2_count, 2);
}

TEST_F(LocalDiskSpillerTest, DeleteRemovesFiles) {
  auto spiller = CreateSpiller({test_dir_.string()});

  ObjectID obj_id = ObjectID::FromRandom();
  auto obj = CreateObject("delete me");
  auto owner = CreateOwnerAddress();

  std::vector<std::string> spill_urls;
  spiller->SpillObjects({obj_id},
                        {obj.get()},
                        {owner},
                        [&](const Status &s, std::vector<std::string> urls) {
                          spill_urls = std::move(urls);
                        });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  ASSERT_EQ(spill_urls.size(), 1);

  // Verify file exists.
  std::string file_path = spill_urls[0].substr(0, spill_urls[0].find('?'));
  EXPECT_TRUE(std::filesystem::exists(file_path));

  // Delete the spilled objects.
  Status delete_status;
  spiller->DeleteSpilledObjects(spill_urls, [&](const Status &s) { delete_status = s; });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  EXPECT_TRUE(delete_status.ok());
  EXPECT_FALSE(std::filesystem::exists(file_path));
}

TEST_F(LocalDiskSpillerTest, RestoreCallsRestoreToPlasma) {
  auto spiller = CreateSpiller({test_dir_.string()});

  ObjectID obj_id = ObjectID::FromRandom();
  auto obj = CreateObject("restore me", "rmeta");
  auto owner = CreateOwnerAddress();

  // Spill first.
  std::vector<std::string> spill_urls;
  spiller->SpillObjects({obj_id},
                        {obj.get()},
                        {owner},
                        [&](const Status &s, std::vector<std::string> urls) {
                          spill_urls = std::move(urls);
                        });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  ASSERT_EQ(spill_urls.size(), 1);

  // Restore.
  Status restore_status;
  int64_t bytes_restored = 0;
  spiller->RestoreSpilledObject(
      obj_id, spill_urls[0], [&](const Status &s, int64_t bytes) {
        restore_status = s;
        bytes_restored = bytes;
      });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  EXPECT_TRUE(restore_status.ok());
  EXPECT_EQ(bytes_restored, 10 + 5);  // "restore me" (10) + "rmeta" (5)

  // Check that restore_to_plasma was called with correct data.
  ASSERT_EQ(restored_objects_.size(), 1);
  EXPECT_EQ(restored_objects_[0].object_id, obj_id);
  EXPECT_EQ(restored_objects_[0].owner_address.ip_address(), "127.0.0.1");

  // Verify data content.
  ASSERT_NE(restored_objects_[0].data, nullptr);
  std::string data_str(reinterpret_cast<const char *>(restored_objects_[0].data->Data()),
                       restored_objects_[0].data->Size());
  EXPECT_EQ(data_str, "restore me");

  // Verify metadata content.
  ASSERT_NE(restored_objects_[0].metadata, nullptr);
  std::string meta_str(
      reinterpret_cast<const char *>(restored_objects_[0].metadata->Data()),
      restored_objects_[0].metadata->Size());
  EXPECT_EQ(meta_str, "rmeta");
}

TEST_F(LocalDiskSpillerTest, RestoreNonExistentFile) {
  auto spiller = CreateSpiller({test_dir_.string()});

  ObjectID obj_id = ObjectID::FromRandom();
  std::string fake_url = "/nonexistent/file?offset=0&size=100";

  Status restore_status;
  spiller->RestoreSpilledObject(
      obj_id, fake_url, [&](const Status &s, int64_t bytes) { restore_status = s; });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  EXPECT_FALSE(restore_status.ok());
}

TEST_F(LocalDiskSpillerTest, SpillObjectWithNoMetadata) {
  auto spiller = CreateSpiller({test_dir_.string()});

  ObjectID obj_id = ObjectID::FromRandom();
  auto obj = CreateObject("data only");  // No metadata.
  auto owner = CreateOwnerAddress();

  std::vector<std::string> spill_urls;
  spiller->SpillObjects({obj_id},
                        {obj.get()},
                        {owner},
                        [&](const Status &s, std::vector<std::string> urls) {
                          spill_urls = std::move(urls);
                        });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  ASSERT_EQ(spill_urls.size(), 1);

  auto reader = SpilledObjectReader::CreateSpilledObjectReader(spill_urls[0]);
  ASSERT_TRUE(reader.has_value());
  EXPECT_EQ(reader->GetDataSize(), 9);  // "data only"
  EXPECT_EQ(reader->GetMetadataSize(), 0);

  std::string data_out;
  EXPECT_TRUE(reader->ReadFromDataSection(0, reader->GetDataSize(), data_out));
  EXPECT_EQ(data_out, "data only");
}

TEST_F(LocalDiskSpillerTest, RestoreFusedObject) {
  auto spiller = CreateSpiller({test_dir_.string()});

  // Spill 3 objects fused.
  std::vector<ObjectID> ids;
  std::vector<std::unique_ptr<RayObject>> obj_storage;
  std::vector<const RayObject *> objs;
  std::vector<rpc::Address> owners;

  for (int i = 0; i < 3; i++) {
    ids.push_back(ObjectID::FromRandom());
    obj_storage.push_back(
        CreateObject("data_" + std::to_string(i), "meta_" + std::to_string(i)));
    objs.push_back(obj_storage.back().get());
    owners.push_back(CreateOwnerAddress("worker_" + std::to_string(i)));
  }

  std::vector<std::string> spill_urls;
  spiller->SpillObjects(
      ids, objs, owners, [&](const Status &s, std::vector<std::string> urls) {
        spill_urls = std::move(urls);
      });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();
  ASSERT_EQ(spill_urls.size(), 3);

  // Restore the middle object (index 1).
  Status restore_status;
  int64_t bytes_restored = 0;
  spiller->RestoreSpilledObject(
      ids[1], spill_urls[1], [&](const Status &s, int64_t bytes) {
        restore_status = s;
        bytes_restored = bytes;
      });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ResetAndRunMainService();

  EXPECT_TRUE(restore_status.ok());
  ASSERT_EQ(restored_objects_.size(), 1);
  EXPECT_EQ(restored_objects_[0].object_id, ids[1]);

  std::string data_str(reinterpret_cast<const char *>(restored_objects_[0].data->Data()),
                       restored_objects_[0].data->Size());
  EXPECT_EQ(data_str, "data_1");

  std::string meta_str(
      reinterpret_cast<const char *>(restored_objects_[0].metadata->Data()),
      restored_objects_[0].metadata->Size());
  EXPECT_EQ(meta_str, "meta_1");
}

}  // namespace raylet
}  // namespace ray
