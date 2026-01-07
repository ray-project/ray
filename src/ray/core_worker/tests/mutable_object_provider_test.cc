// Copyright 2024 The Ray Authors.
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

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/barrier.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/object_manager/plasma/client.h"
#include "ray/core_worker/experimental_mutable_object_provider.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray {
namespace core {
namespace experimental {

#if defined(__APPLE__) || defined(__linux__)

namespace {

class TestPlasma : public plasma::MockPlasmaClient {
 public:
  Status GetExperimentalMutableObject(
      const ObjectID &object_id,
      std::unique_ptr<plasma::MutableObject> *mutable_object) override {
    absl::MutexLock guard(&lock_);
    auto it = objects_.find(object_id);
    if (it == objects_.end()) {
      // Use a larger default size to support tests with larger objects
      // Need at least 2048 bytes to accommodate tests with variable chunk sizes
      auto obj = MakeObject(/*min_size=*/2048);
      uint8_t *ptr = reinterpret_cast<uint8_t *>(obj->header);
      objects_[object_id] = ptr;
      *mutable_object = std::move(obj);
    } else {
      // Object already exists - return a view of the same underlying memory
      uint8_t *ptr = it->second;
      plasma::PlasmaObject info{};
      info.header_offset = 0;
      info.data_offset = sizeof(PlasmaObjectHeader);
      info.allocated_size = 2048;  // Same size as initial allocation
      *mutable_object = std::make_unique<plasma::MutableObject>(ptr, info);
    }
    return Status::OK();
  }

  ~TestPlasma() override {
    // Free all allocated objects
    for (auto &pair : objects_) {
      free(pair.second);
    }
  }

 private:
  // Creates a new mutable object. It is the caller's responsibility to free the backing
  // store.
  std::unique_ptr<plasma::MutableObject> MakeObject(size_t min_size = 128) {
    // Allocate enough space for header + data + metadata
    // Round up to ensure we have enough space
    size_t payload_size = std::max(min_size, static_cast<size_t>(128));
    size_t total_size = sizeof(PlasmaObjectHeader) + payload_size;

    plasma::PlasmaObject info{};
    info.header_offset = 0;
    info.data_offset = sizeof(PlasmaObjectHeader);
    info.allocated_size = payload_size;

    uint8_t *ptr = static_cast<uint8_t *>(malloc(total_size));
    RAY_CHECK(ptr);
    auto ret = std::make_unique<plasma::MutableObject>(ptr, info);
    ret->header->Init();
    return ret;
  }

  absl::Mutex lock_;
  // Maps object IDs to their backing store pointers
  std::unordered_map<ObjectID, uint8_t *> objects_;
};

class MockRayletClient : public rpc::FakeRayletClient {
 public:
  virtual ~MockRayletClient() {}

  void PushMutableObject(const ObjectID &object_id,
                         uint64_t data_size,
                         uint64_t metadata_size,
                         void *data,
                         void *metadata,
                         int64_t version,
                         const rpc::ClientCallback<rpc::PushMutableObjectReply> &callback,
                         int64_t timeout_ms = -1) override {
    absl::MutexLock guard(&lock_);
    pushed_objects_.push_back(object_id);
  }

  std::vector<ObjectID> pushed_objects() {
    absl::MutexLock guard(&lock_);
    return pushed_objects_;
  }

 private:
  absl::Mutex lock_;
  std::vector<ObjectID> pushed_objects_;
};

std::shared_ptr<RayletClientInterface> GetMockRayletClient(
    std::shared_ptr<MockRayletClient> &interface, const NodeID &node_id) {
  return interface;
}

}  // namespace

TEST(MutableObjectProvider, RegisterWriterChannel) {
  ObjectID object_id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  auto interface = std::make_shared<MockRayletClient>();

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetMockRayletClient, interface),
      nullptr);
  provider.RegisterWriterChannel(object_id, {node_id});

  std::shared_ptr<Buffer> data;
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::OK);
  EXPECT_EQ(provider.WriteRelease(object_id).code(), StatusCode::OK);

  while (interface->pushed_objects().empty()) {
  }

  EXPECT_EQ(interface->pushed_objects().size(), 1);
  EXPECT_EQ(interface->pushed_objects().front(), object_id);
}

TEST(MutableObjectProvider, MutableObjectBufferReadRelease) {
  ObjectID object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  MutableObjectProvider provider(plasma,
                                 /*factory=*/nullptr,
                                 nullptr);
  provider.RegisterWriterChannel(object_id, {});

  std::shared_ptr<Buffer> data;
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::OK);
  EXPECT_EQ(provider.WriteRelease(object_id).code(), StatusCode::OK);

  provider.RegisterReaderChannel(object_id);

  // `next_version_to_read` should be initialized to 1.
  EXPECT_EQ(provider.object_manager_->GetChannel(object_id)->next_version_to_read, 1);
  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(object_id, result).code(), StatusCode::OK);
  }
  // The result (RayObject) together with the underlying MutableObjectBuffer
  // goes out of scope here, this will trigger the call to ReadRelease() in
  // the destructor of MutableObjectBuffer. This is verified by checking
  // `next_version_to_read` of the channel, which is only incremented inside
  // ReadRelease().
  EXPECT_EQ(provider.object_manager_->GetChannel(object_id)->next_version_to_read, 2);
}

TEST(MutableObjectProvider, HandlePushMutableObject) {
  ObjectID object_id = ObjectID::FromRandom();
  ObjectID local_object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  auto interface = std::make_shared<MockRayletClient>();

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetMockRayletClient, interface),
      nullptr);
  provider.HandleRegisterMutableObject(object_id, /*num_readers=*/1, local_object_id);

  ray::rpc::PushMutableObjectRequest request;
  request.set_writer_object_id(object_id.Binary());
  request.set_total_data_size(0);
  request.set_total_metadata_size(0);
  request.set_version(1);

  ray::rpc::PushMutableObjectReply reply;
  provider.HandlePushMutableObject(request, &reply);

  std::shared_ptr<RayObject> result;
  EXPECT_EQ(provider.ReadAcquire(local_object_id, result).code(), StatusCode::OK);
  EXPECT_EQ(result->GetSize(), 0UL);
  EXPECT_EQ(provider.ReadRelease(local_object_id).code(), StatusCode::OK);
}

TEST(MutableObjectProvider, MutableObjectBufferSetError) {
  ObjectID object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  MutableObjectProvider provider(plasma,
                                 /*factory=*/nullptr,
                                 nullptr);
  provider.RegisterWriterChannel(object_id, {});

  std::shared_ptr<Buffer> data;
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::OK);
  EXPECT_EQ(provider.WriteRelease(object_id).code(), StatusCode::OK);

  provider.RegisterReaderChannel(object_id);

  // Set error.
  EXPECT_EQ(provider.SetError(object_id).code(), StatusCode::OK);
  // Set error is idempotent and should never block.
  EXPECT_EQ(provider.SetError(object_id).code(), StatusCode::OK);

  // All future reads and writes return ChannelError.
  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(object_id, result).code(), StatusCode::ChannelError);
  }
  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(object_id, result).code(), StatusCode::ChannelError);
  }
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::ChannelError);
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::ChannelError);
}

TEST(MutableObjectProvider, MutableObjectBufferSetErrorBeforeWriteRelease) {
  ObjectID object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  MutableObjectProvider provider(plasma,
                                 /*factory=*/nullptr,
                                 nullptr);
  provider.RegisterWriterChannel(object_id, {});

  std::shared_ptr<Buffer> data;
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::OK);

  provider.RegisterReaderChannel(object_id);

  // Set error before the writer has released.
  EXPECT_EQ(provider.SetError(object_id).code(), StatusCode::OK);
  // Set error is idempotent and should never block.
  EXPECT_EQ(provider.SetError(object_id).code(), StatusCode::OK);

  // All future reads and writes return ChannelError.
  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(object_id, result).code(), StatusCode::ChannelError);
  }
  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(object_id, result).code(), StatusCode::ChannelError);
  }
  EXPECT_EQ(provider.WriteRelease(object_id).code(), StatusCode::ChannelError);
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::ChannelError);
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::ChannelError);
}

TEST(MutableObjectProvider, MutableObjectBufferSetErrorBeforeReadRelease) {
  ObjectID object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  MutableObjectProvider provider(plasma,
                                 /*factory=*/nullptr,
                                 nullptr);
  provider.RegisterWriterChannel(object_id, {});

  std::shared_ptr<Buffer> data;
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::OK);
  EXPECT_EQ(provider.WriteRelease(object_id).code(), StatusCode::OK);

  provider.RegisterReaderChannel(object_id);

  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(object_id, result).code(), StatusCode::OK);
    // Set error before the reader has released.
    EXPECT_EQ(provider.SetError(object_id).code(), StatusCode::OK);

    // When the error is set, reading again before releasing does not block.
    // Also immediately returns the error.
    EXPECT_EQ(provider.ReadAcquire(object_id, result).code(), StatusCode::ChannelError);
  }

  // All future reads and writes return ChannelError.
  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(object_id, result).code(), StatusCode::ChannelError);
  }
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::ChannelError);
  EXPECT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/0,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::ChannelError);
}

// Test retry handling with out-of-order chunks
// Simulates the scenario where a chunk is retried and arrives out of order
TEST(MutableObjectProvider, HandleRetryOutOfOrderChunks) {
  constexpr size_t kChunk0Size = 256;
  constexpr size_t kChunk1Size = 512;
  constexpr size_t kChunk2Size = 384;
  constexpr size_t kTotalDataSize = kChunk0Size + kChunk1Size + kChunk2Size;  // 3 chunks
  constexpr size_t kMetadataSize = 16;

  ObjectID writer_object_id = ObjectID::FromRandom();
  ObjectID reader_object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  MutableObjectProvider provider(plasma, /*factory=*/nullptr, nullptr);

  provider.HandleRegisterMutableObject(
      writer_object_id, /*num_readers=*/1, reader_object_id);

  // Prepare chunk data
  std::vector<std::vector<uint8_t>> chunk_data(3);
  std::vector<uint8_t> metadata(kMetadataSize, 0xAB);
  chunk_data[0].resize(kChunk0Size, static_cast<uint8_t>(0));
  chunk_data[1].resize(kChunk1Size, static_cast<uint8_t>(1));
  chunk_data[2].resize(kChunk2Size, static_cast<uint8_t>(2));

  // Send chunks out of order: chunk 1, then chunk 0 (retry scenario),
  // then chunk 2
  std::vector<ray::rpc::PushMutableObjectReply> replies(3);

  // Chunk 1 arrives first (offset = kChunk0Size)
  {
    ray::rpc::PushMutableObjectRequest request;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(kTotalDataSize);
    request.set_total_metadata_size(kMetadataSize);
    request.set_offset(kChunk0Size);
    request.set_chunk_size(kChunk1Size);
    request.set_data(chunk_data[1].data(), kChunk1Size);
    request.set_metadata(metadata.data(), kMetadataSize);
    request.set_version(1);  // All chunks in this write have version 1
    provider.HandlePushMutableObject(request, &replies[1]);
    EXPECT_FALSE(replies[1].done()) << "Chunk 1 should not complete the object";
  }

  // Chunk 0 arrives second (offset = 0) - simulates retry or out-of-order
  {
    ray::rpc::PushMutableObjectRequest request;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(kTotalDataSize);
    request.set_total_metadata_size(kMetadataSize);
    request.set_offset(0);
    request.set_chunk_size(kChunk0Size);
    request.set_data(chunk_data[0].data(), kChunk0Size);
    request.set_metadata(metadata.data(), kMetadataSize);
    request.set_version(1);  // Same version as chunk 1
    provider.HandlePushMutableObject(request, &replies[0]);
    EXPECT_FALSE(replies[0].done()) << "Chunk 0 should not complete the object";
  }

  // Retry chunk 0 (idempotent - should be handled gracefully)
  {
    ray::rpc::PushMutableObjectRequest request;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(kTotalDataSize);
    request.set_total_metadata_size(kMetadataSize);
    request.set_offset(0);
    request.set_chunk_size(kChunk0Size);
    request.set_data(chunk_data[0].data(), kChunk0Size);
    request.set_metadata(metadata.data(), kMetadataSize);
    request.set_version(1);  // Same version - legitimate retry
    ray::rpc::PushMutableObjectReply retry_reply;
    provider.HandlePushMutableObject(request, &retry_reply);
    // Retry should return current status without error
    EXPECT_FALSE(retry_reply.done()) << "Retry of chunk 0 should return current status";
  }

  // Chunk 2 arrives last (offset = kChunk0Size + kChunk1Size)
  {
    ray::rpc::PushMutableObjectRequest request;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(kTotalDataSize);
    request.set_total_metadata_size(kMetadataSize);
    request.set_offset(kChunk0Size + kChunk1Size);
    request.set_chunk_size(kChunk2Size);
    request.set_data(chunk_data[2].data(), kChunk2Size);
    request.set_metadata(metadata.data(), kMetadataSize);
    request.set_version(1);  // Same version
    provider.HandlePushMutableObject(request, &replies[2]);
    EXPECT_TRUE(replies[2].done()) << "Chunk 2 should complete the object";
  }

  // Verify all chunks were received correctly
  std::shared_ptr<RayObject> result;
  EXPECT_EQ(provider.ReadAcquire(reader_object_id, result).code(), StatusCode::OK);

  EXPECT_EQ(result->GetData()->Size(), kTotalDataSize);
  EXPECT_EQ(result->GetMetadata()->Size(), kMetadataSize);

  // Verify data integrity - check each chunk
  const uint8_t *data_ptr = result->GetData()->Data();
  size_t chunk_offsets[3] = {0, kChunk0Size, kChunk0Size + kChunk1Size};
  size_t chunk_sizes[3] = {kChunk0Size, kChunk1Size, kChunk2Size};
  for (int chunk = 0; chunk < 3; chunk++) {
    for (size_t i = 0; i < chunk_sizes[chunk]; i++) {
      EXPECT_EQ(data_ptr[chunk_offsets[chunk] + i], static_cast<uint8_t>(chunk))
          << "Data mismatch at chunk " << chunk << " offset " << i;
    }
  }

  EXPECT_EQ(provider.ReadRelease(reader_object_id).code(), StatusCode::OK);
}

// Test that version tracking correctly distinguishes chunks from different write epochs
// This verifies chunks with different versions are not incorrectly treated as duplicates
TEST(MutableObjectProvider, HandleVersionBasedRetryDetection) {
  constexpr size_t kDataSize = 512;
  constexpr size_t kMetadataSize = 16;

  ObjectID writer_object_id = ObjectID::FromRandom();
  ObjectID reader_object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  MutableObjectProvider provider(plasma, /*factory=*/nullptr, nullptr);

  provider.HandleRegisterMutableObject(
      writer_object_id, /*num_readers=*/1, reader_object_id);

  // Write with version 1, single chunk at offset 0
  std::vector<uint8_t> write1_data(kDataSize, 0xAA);
  std::vector<uint8_t> metadata1(kMetadataSize, 0x11);
  {
    ray::rpc::PushMutableObjectRequest request;
    ray::rpc::PushMutableObjectReply reply;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(kDataSize);
    request.set_total_metadata_size(kMetadataSize);
    request.set_offset(0);
    request.set_chunk_size(kDataSize);
    request.set_data(write1_data.data(), kDataSize);
    request.set_metadata(metadata1.data(), kMetadataSize);
    request.set_version(1);
    provider.HandlePushMutableObject(request, &reply);
    EXPECT_TRUE(reply.done());
  }

  // Retry of same chunk (same version) - should be treated as duplicate
  {
    ray::rpc::PushMutableObjectRequest request;
    ray::rpc::PushMutableObjectReply reply;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(kDataSize);
    request.set_total_metadata_size(kMetadataSize);
    request.set_offset(0);
    request.set_chunk_size(kDataSize);
    request.set_data(write1_data.data(), kDataSize);
    request.set_metadata(metadata1.data(), kMetadataSize);
    request.set_version(1);  // Same version
    provider.HandlePushMutableObject(request, &reply);
    EXPECT_TRUE(reply.done())
        << "Legitimate retry with same version recognized as duplicate";
  }

  // Read and release
  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(reader_object_id, result).code(), StatusCode::OK);
    EXPECT_EQ(provider.ReadRelease(reader_object_id).code(), StatusCode::OK);
  }

  // New write with version 2, same offset 0 - should NOT be treated as duplicate
  std::vector<uint8_t> write2_data(kDataSize, 0xBB);
  std::vector<uint8_t> metadata2(kMetadataSize, 0x22);
  {
    ray::rpc::PushMutableObjectRequest request;
    ray::rpc::PushMutableObjectReply reply;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(kDataSize);
    request.set_total_metadata_size(kMetadataSize);
    request.set_offset(0);
    request.set_chunk_size(kDataSize);
    request.set_data(write2_data.data(), kDataSize);
    request.set_metadata(metadata2.data(), kMetadataSize);
    request.set_version(2);  // DIFFERENT version
    provider.HandlePushMutableObject(request, &reply);
    EXPECT_TRUE(reply.done()) << "New write with different version correctly processed";
  }

  // Verify we got Write 2's data (version 2 overwrote version 1)
  {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(reader_object_id, result).code(), StatusCode::OK);
    const uint8_t *data_ptr = result->GetData()->Data();
    for (size_t i = 0; i < kDataSize; i++) {
      EXPECT_EQ(data_ptr[i], 0xBB) << "Version 2 data correctly written at offset " << i;
    }
    EXPECT_EQ(provider.ReadRelease(reader_object_id).code(), StatusCode::OK);
  }
}

#endif  // defined(__APPLE__) || defined(__linux__)

}  // namespace experimental
}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
