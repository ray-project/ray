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

#include <limits>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
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
    if (!objects_.count(object_id)) {
      *mutable_object = MakeObject();
      objects_.insert(object_id);
    } else {
      *mutable_object = nullptr;
    }
    return Status::OK();
  }

 private:
  // Creates a new mutable object. It is the caller's responsibility to free the backing
  // store.
  std::unique_ptr<plasma::MutableObject> MakeObject() {
    constexpr size_t kPayloadSize = 128;
    constexpr size_t kSize = sizeof(PlasmaObjectHeader) + kPayloadSize;

    plasma::PlasmaObject info{};
    info.header_offset = 0;
    info.data_offset = sizeof(PlasmaObjectHeader);
    info.allocated_size = kPayloadSize;

    uint8_t *ptr = static_cast<uint8_t *>(malloc(kSize));
    RAY_CHECK(ptr);
    auto ret = std::make_unique<plasma::MutableObject>(ptr, info);
    ret->header->Init();
    return ret;
  }

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

#endif  // defined(__APPLE__) || defined(__linux__)

}  // namespace experimental
}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
