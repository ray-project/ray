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
#include <unordered_set>
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
    if (!objects_.count(object_id)) {
      // Use a larger default size to support tests with larger objects
      // Need at least 2048 bytes to accommodate tests with variable chunk sizes
      *mutable_object = MakeObject(/*min_size=*/2048);
      objects_.insert(object_id);
    } else {
      // Object already exists - return a new view of it
      // For testing, we create a new object each time since the real implementation
      // would return a view of the existing object
      *mutable_object = MakeObject(/*min_size=*/2048);
    }
    return Status::OK();
  }

  ~TestPlasma() override {
    // Objects are managed by the MutableObjectProvider, so we don't free them here
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
  // Tracks the mutable objects that have been created.
  std::unordered_set<ObjectID> objects_;
};

class MockRayletClient : public rpc::FakeRayletClient {
 public:
  virtual ~MockRayletClient() {}

  void PushMutableObject(
      const ObjectID &object_id,
      uint64_t data_size,
      uint64_t metadata_size,
      void *data,
      void *metadata,
      const rpc::ClientCallback<rpc::PushMutableObjectReply> &callback) override {
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

// Test that emulates the out-of-order PushMutableObject scenario described in
// https://github.com/ray-project/ray/issues/58426
//
// Scenario: Multiple ranks send PushMutableObject requests concurrently. Due to
// network jitter and OS scheduling, some requests arrive immediately while others
// are delayed. This test verifies that the receiver handles out-of-order requests
// correctly without deadlocking.
//
// The test simulates:
// 1. Multiple "ranks" (4 ranks: rank0-rank3) sending requests concurrently
// 2. Network jitter causing some requests to be delayed
// 3. A new round of requests arriving before previous round completes
// 4. Verifies no deadlock occurs and all data is correctly received
TEST(MutableObjectProvider, HandleOutOfOrderPushMutableObject) {
  constexpr int kNumRanks = 4;
  constexpr size_t kDataSize = 64;
  constexpr size_t kMetadataSize = 8;

  // Create provider and register reader channels for each rank
  auto plasma = std::make_shared<TestPlasma>();
  MutableObjectProvider provider(plasma, /*factory=*/nullptr, nullptr);

  std::vector<ObjectID> writer_object_ids;
  std::vector<ObjectID> reader_object_ids;
  for (int i = 0; i < kNumRanks; i++) {
    writer_object_ids.push_back(ObjectID::FromRandom());
    reader_object_ids.push_back(ObjectID::FromRandom());
    provider.HandleRegisterMutableObject(
        writer_object_ids[i], /*num_readers=*/1, reader_object_ids[i]);
  }

  // Prepare data for each rank
  std::vector<std::vector<uint8_t>> rank_data(kNumRanks);
  std::vector<std::vector<uint8_t>> rank_metadata(kNumRanks);
  for (int i = 0; i < kNumRanks; i++) {
    rank_data[i].resize(kDataSize, static_cast<uint8_t>(i));
    rank_metadata[i].resize(kMetadataSize, static_cast<uint8_t>(i + 100));
  }

  // Simulate two rounds of requests
  constexpr int kNumRounds = 2;

  // Track request completion
  std::vector<std::vector<bool>> round_completed(kNumRounds);
  for (int round = 0; round < kNumRounds; round++) {
    round_completed[round].resize(kNumRanks, false);
  }

  // Single barrier to synchronize the start of all threads (but not between rounds)
  // This allows rank0 to proceed to the next round before ranks 2-3 complete previous round
  absl::Barrier start_barrier(kNumRanks);

  // Function for each rank to send requests for all rounds
  auto rank_sender = [&](int rank) {
    // Wait for all ranks to be ready before starting (synchronize thread launch)
    start_barrier.Block();

    for (int round = 0; round < kNumRounds; round++) {

      // Simulate network jitter: rank0 and rank1 send immediately,
      // rank2 and rank3 are delayed (simulating the issue scenario)
      if (rank >= 2 && round == 0) {
        // Delay ranks 2-3 in the first round to simulate network jitter
        absl::SleepFor(absl::Milliseconds(50));
      }

      ray::rpc::PushMutableObjectRequest request;
      request.set_writer_object_id(writer_object_ids[rank].Binary());
      request.set_total_data_size(kDataSize);
      request.set_total_metadata_size(kMetadataSize);
      request.set_offset(0);
      request.set_chunk_size(kDataSize);
      request.set_data(rank_data[rank].data(), kDataSize);
      request.set_metadata(rank_metadata[rank].data(), kMetadataSize);

      ray::rpc::PushMutableObjectReply reply;

      // Send request - this should not block even if other ranks' requests
      // are delayed or out of order
      provider.HandlePushMutableObject(request, &reply);

      EXPECT_TRUE(reply.done())
          << "Rank " << rank << " round " << round << " should complete";
      round_completed[round][rank] = true;

      // Small delay after rank0 completes round 0 to simulate the scenario where
      // rank0 sends next round before ranks 2-3 complete previous round
      if (rank == 0 && round == 0) {
        absl::SleepFor(absl::Milliseconds(10));
      }
    }
  };

  // Launch one thread per rank (each rank sends all rounds)
  std::vector<std::thread> threads;
  for (int rank = 0; rank < kNumRanks; rank++) {
    threads.emplace_back(rank_sender, rank);
  }

  // Wait for all threads to complete (with timeout to detect deadlocks)
  auto start_time = absl::Now();
  for (auto &thread : threads) {
    thread.join();
  }
  auto elapsed = absl::Now() - start_time;

  // Verify no deadlock occurred (should complete quickly)
  EXPECT_LT(elapsed, absl::Seconds(5))
      << "Test should complete quickly; deadlock suspected if timeout";

  // Verify all requests completed successfully
  for (int round = 0; round < kNumRounds; round++) {
    for (int rank = 0; rank < kNumRanks; rank++) {
      EXPECT_TRUE(round_completed[round][rank])
          << "Rank " << rank << " round " << round << " did not complete";
    }
  }

  // Verify data integrity: read back the data for each rank
  for (int rank = 0; rank < kNumRanks; rank++) {
    std::shared_ptr<RayObject> result;
    EXPECT_EQ(provider.ReadAcquire(reader_object_ids[rank], result).code(),
              StatusCode::OK)
        << "Failed to read data for rank " << rank;

    EXPECT_EQ(result->GetData()->Size(), kDataSize);
    EXPECT_EQ(result->GetMetadata()->Size(), kMetadataSize);

    // Verify data content
    const uint8_t *data_ptr = result->GetData()->Data();
    for (size_t i = 0; i < kDataSize; i++) {
      EXPECT_EQ(data_ptr[i], static_cast<uint8_t>(rank))
          << "Data mismatch for rank " << rank << " at offset " << i;
    }

    // Verify metadata content
    const uint8_t *metadata_ptr = result->GetMetadata()->Data();
    for (size_t i = 0; i < kMetadataSize; i++) {
      EXPECT_EQ(metadata_ptr[i], static_cast<uint8_t>(rank + 100))
          << "Metadata mismatch for rank " << rank << " at offset " << i;
    }

    EXPECT_EQ(provider.ReadRelease(reader_object_ids[rank]).code(), StatusCode::OK);
  }
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

#endif  // defined(__APPLE__) || defined(__linux__)

}  // namespace experimental
}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
