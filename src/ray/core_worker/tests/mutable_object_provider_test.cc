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
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "ray/core_worker/experimental_mutable_object_provider.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/object_manager/plasma/fake_plasma_client.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray {
namespace core {
namespace experimental {

#if defined(__APPLE__) || defined(__linux__)

namespace {

class TestPlasma : public plasma::FakePlasmaClient {
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

class FakeRayletClientForTest : public rpc::FakeRayletClient {
 public:
  virtual ~FakeRayletClientForTest() {}

  void PushMutableObject(
      const ObjectID &object_id,
      uint64_t data_size,
      uint64_t metadata_size,
      void *data,
      void *metadata,
      const rpc::ClientCallback<rpc::PushMutableObjectReply> &callback) override {
    std::optional<Status> status;
    {
      absl::MutexLock guard(&lock_);
      pushed_objects_.push_back(object_id);
      status = push_status_;
      push_callbacks_.push_back(status.has_value() ? nullptr : callback);
    }
    if (status.has_value()) {
      rpc::PushMutableObjectReply reply;
      callback(*status, std::move(reply));
      MarkPushCompleted();
    }
  }

  void SetPushStatus(Status status) {
    absl::MutexLock guard(&lock_);
    push_status_ = std::move(status);
  }

  bool WaitForPush() { return WaitForCompletedPushCount(1); }

  bool WaitForPushCount(size_t count) {
    struct PushCountCondition {
      const FakeRayletClientForTest *client;
      size_t count;
    };
    PushCountCondition condition{this, count};
    absl::MutexLock guard(&lock_);
    return lock_.AwaitWithTimeout(
        absl::Condition(
            +[](PushCountCondition *condition) {
              return condition->client->pushed_objects_.size() >= condition->count;
            },
            &condition),
        absl::Seconds(5));
  }

  bool WaitForCompletedPushCount(size_t count) {
    struct PushCountCondition {
      const FakeRayletClientForTest *client;
      size_t count;
    };
    PushCountCondition condition{this, count};
    absl::MutexLock guard(&lock_);
    return lock_.AwaitWithTimeout(absl::Condition(
                                      +[](PushCountCondition *condition) {
                                        condition->client->lock_.AssertHeld();
                                        return condition->client->num_completed_pushes_ >=
                                               condition->count;
                                      },
                                      &condition),
                                  absl::Seconds(5));
  }

  void CompletePush(size_t index, Status status) {
    rpc::ClientCallback<rpc::PushMutableObjectReply> callback;
    {
      absl::MutexLock guard(&lock_);
      RAY_CHECK_LT(index, push_callbacks_.size());
      RAY_CHECK(push_callbacks_[index]);
      callback = std::move(push_callbacks_[index]);
      push_callbacks_[index] = nullptr;
    }
    rpc::PushMutableObjectReply reply;
    callback(status, std::move(reply));
    MarkPushCompleted();
  }

  void ClearPendingPushes() {
    std::vector<rpc::ClientCallback<rpc::PushMutableObjectReply>> callbacks;
    {
      absl::MutexLock guard(&lock_);
      callbacks.swap(push_callbacks_);
    }
  }

  std::vector<ObjectID> pushed_objects() {
    absl::MutexLock guard(&lock_);
    return pushed_objects_;
  }

 private:
  void MarkPushCompleted() {
    absl::MutexLock guard(&lock_);
    ++num_completed_pushes_;
  }

  absl::Mutex lock_;
  std::optional<Status> push_status_ ABSL_GUARDED_BY(lock_);
  std::vector<ObjectID> pushed_objects_;
  size_t num_completed_pushes_ ABSL_GUARDED_BY(lock_) = 0;
  std::vector<rpc::ClientCallback<rpc::PushMutableObjectReply>> push_callbacks_
      ABSL_GUARDED_BY(lock_);
};

std::shared_ptr<RayletClientInterface> GetFakeRayletClient(
    std::shared_ptr<FakeRayletClientForTest> &interface, const NodeID &node_id) {
  return interface;
}

}  // namespace

TEST(FakeRayletClientForTest, ReleasesCompletedPushCallbacks) {
  for (bool complete_automatically : {false, true}) {
    std::weak_ptr<FakeRayletClientForTest> weak_interface;
    {
      auto interface = std::make_shared<FakeRayletClientForTest>();
      weak_interface = interface;
      if (complete_automatically) {
        interface->SetPushStatus(Status::OK());
      }
      interface->PushMutableObject(
          ObjectID::FromRandom(),
          0,
          0,
          nullptr,
          nullptr,
          [interface](const Status &, const rpc::PushMutableObjectReply &) {
            EXPECT_EQ(interface->pushed_objects().size(), 1);
          });
      if (!complete_automatically) {
        interface->CompletePush(0, Status::OK());
      }
      ASSERT_TRUE(interface->WaitForCompletedPushCount(1));
    }
    EXPECT_TRUE(weak_interface.expired());
  }
}

TEST(FakeRayletClientForTest, ReleasesPendingPushCallbacks) {
  std::weak_ptr<FakeRayletClientForTest> weak_interface;
  {
    auto interface = std::make_shared<FakeRayletClientForTest>();
    weak_interface = interface;
    interface->PushMutableObject(
        ObjectID::FromRandom(),
        0,
        0,
        nullptr,
        nullptr,
        [interface](const Status &, const rpc::PushMutableObjectReply &) {
          EXPECT_EQ(interface->pushed_objects().size(), 1);
          ADD_FAILURE() << "A cleared push must not invoke its callback";
        });
    interface->ClearPendingPushes();
  }
  EXPECT_TRUE(weak_interface.expired());
}

TEST(MutableObjectProvider, RegisterWriterChannel) {
  ObjectID object_id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  auto interface = std::make_shared<FakeRayletClientForTest>();

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetFakeRayletClient, interface),
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
  interface->ClearPendingPushes();
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
  auto interface = std::make_shared<FakeRayletClientForTest>();

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetFakeRayletClient, interface),
      nullptr);
  provider.HandleRegisterMutableObject(object_id, /*num_readers=*/1, local_object_id);

  ray::rpc::PushMutableObjectRequest request;
  request.set_writer_object_id(object_id.Binary());
  request.set_total_data_size(0);
  request.set_total_metadata_size(0);

  ray::rpc::PushMutableObjectReply reply;
  EXPECT_EQ(provider.HandlePushMutableObject(request, &reply).code(), StatusCode::OK);

  std::shared_ptr<RayObject> result;
  EXPECT_EQ(provider.ReadAcquire(local_object_id, result).code(), StatusCode::OK);
  EXPECT_EQ(result->GetSize(), 0UL);
  EXPECT_EQ(provider.ReadRelease(local_object_id).code(), StatusCode::OK);
}

TEST(MutableObjectProvider, HandlePushMutableObjectOnClosedChannel) {
  ObjectID object_id = ObjectID::FromRandom();
  ObjectID local_object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  MutableObjectProvider provider(plasma, /*factory=*/nullptr, nullptr);
  provider.HandleRegisterMutableObject(object_id, /*num_readers=*/1, local_object_id);
  ASSERT_EQ(provider.SetError(local_object_id).code(), StatusCode::OK);

  rpc::PushMutableObjectRequest request;
  request.set_writer_object_id(object_id.Binary());
  request.set_total_data_size(1);
  request.set_total_metadata_size(0);
  request.set_offset(0);
  request.set_chunk_size(1);
  request.set_data("x");

  rpc::PushMutableObjectReply reply;
  EXPECT_EQ(provider.HandlePushMutableObject(request, &reply).code(),
            StatusCode::ChannelError);
  EXPECT_FALSE(reply.done());
}

TEST(MutableObjectProvider, RemotePushFailureClosesWriterChannel) {
  ObjectID object_id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  auto interface = std::make_shared<FakeRayletClientForTest>();
  interface->SetPushStatus(Status::ChannelError("reader channel is closed"));

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetFakeRayletClient, interface),
      nullptr);
  provider.RegisterWriterChannel(object_id, {node_id});

  std::shared_ptr<Buffer> data;
  ASSERT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/1,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::OK);
  data->Data()[0] = 'x';
  ASSERT_EQ(provider.WriteRelease(object_id).code(), StatusCode::OK);

  ASSERT_TRUE(interface->WaitForPush());
  EXPECT_EQ(provider.GetChannelStatus(object_id, /*is_reader=*/false).code(),
            StatusCode::ChannelError);
  EXPECT_EQ(interface->pushed_objects().size(), 1);
}

TEST(MutableObjectProvider, RemotePushFailureClosesWriterBeforeOtherReadersReply) {
  ObjectID object_id = ObjectID::FromRandom();
  NodeID first_node_id = NodeID::FromRandom();
  NodeID second_node_id = NodeID::FromRandom();
  NodeID third_node_id = NodeID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  auto interface = std::make_shared<FakeRayletClientForTest>();

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetFakeRayletClient, interface),
      nullptr);
  provider.RegisterWriterChannel(object_id,
                                 {first_node_id, second_node_id, third_node_id});

  std::shared_ptr<Buffer> data;
  ASSERT_EQ(provider
                .WriteAcquire(object_id,
                              /*data_size=*/1,
                              /*metadata=*/nullptr,
                              /*metadata_size=*/0,
                              /*num_readers=*/1,
                              data)
                .code(),
            StatusCode::OK);
  data->Data()[0] = 'x';
  ASSERT_EQ(provider.WriteRelease(object_id).code(), StatusCode::OK);
  ASSERT_TRUE(interface->WaitForPushCount(3));

  interface->CompletePush(0, Status::ChannelError("first reader channel is closed"));
  EXPECT_EQ(provider.GetChannelStatus(object_id, /*is_reader=*/false).code(),
            StatusCode::ChannelError);

  interface->CompletePush(1, Status::ChannelError("second reader channel is closed"));
  EXPECT_EQ(provider.GetChannelStatus(object_id, /*is_reader=*/false).code(),
            StatusCode::ChannelError);

  interface->CompletePush(2, Status::OK());
  EXPECT_EQ(provider.GetChannelStatus(object_id, /*is_reader=*/false).code(),
            StatusCode::ChannelError);
  EXPECT_EQ(interface->pushed_objects().size(), 3);
}

TEST(MutableObjectProvider, SuccessfulRemotePushesContinuePolling) {
  ObjectID object_id = ObjectID::FromRandom();
  NodeID first_node_id = NodeID::FromRandom();
  NodeID second_node_id = NodeID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  auto interface = std::make_shared<FakeRayletClientForTest>();
  interface->SetPushStatus(Status::OK());

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetFakeRayletClient, interface),
      nullptr);
  provider.RegisterWriterChannel(object_id, {first_node_id, second_node_id});

  for (size_t value = 0; value < 2; ++value) {
    std::shared_ptr<Buffer> data;
    ASSERT_EQ(provider
                  .WriteAcquire(object_id,
                                /*data_size=*/1,
                                /*metadata=*/nullptr,
                                /*metadata_size=*/0,
                                /*num_readers=*/1,
                                data)
                  .code(),
              StatusCode::OK);
    data->Data()[0] = 'x';
    ASSERT_EQ(provider.WriteRelease(object_id).code(), StatusCode::OK);
    ASSERT_TRUE(interface->WaitForCompletedPushCount((value + 1) * 2));
  }

  EXPECT_EQ(provider.GetChannelStatus(object_id, /*is_reader=*/false).code(),
            StatusCode::OK);
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
