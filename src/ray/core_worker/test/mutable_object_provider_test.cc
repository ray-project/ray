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

#include "absl/functional/bind_front.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/core_worker/experimental_mutable_object_provider.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/client.h"

using namespace testing;

namespace ray {
namespace core {
namespace experimental {

#if defined(__APPLE__) || defined(__linux__)

namespace {

class TestPlasma : public plasma::PlasmaClientInterface {
 public:
  virtual ~TestPlasma() {}

  Status Release(const ObjectID &object_id) override { return Status::OK(); }

  Status Disconnect() override { return Status::OK(); }

  Status Get(const std::vector<ObjectID> &object_ids,
             int64_t timeout_ms,
             std::vector<plasma::ObjectBuffer> *object_buffers,
             bool is_from_worker) override {
    return Status::OK();
  }

  Status ExperimentalMutableObjectRegisterWriter(const ObjectID &object_id) override {
    return Status::OK();
  }

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

  Status Seal(const ObjectID &object_id) override { return Status::OK(); }

  Status Abort(const ObjectID &object_id) override { return Status::OK(); }

  Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                const ray::rpc::Address &owner_address,
                                bool is_mutable,
                                int64_t data_size,
                                const uint8_t *metadata,
                                int64_t metadata_size,
                                std::shared_ptr<Buffer> *data,
                                plasma::flatbuf::ObjectSource source,
                                int device_num = 0) override {
    return Status::OK();
  }

  Status Delete(const std::vector<ObjectID> &object_ids) override { return Status::OK(); }

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

class TestInterface : public MutableObjectReaderInterface {
 public:
  virtual ~TestInterface() {}

  void RegisterMutableObjectReader(
      const ObjectID &object_id,
      int64_t num_readers,
      const ObjectID &local_reader_object_id,
      const rpc::ClientCallback<rpc::RegisterMutableObjectReply> &callback) override {}

  void PushMutableObject(
      const ObjectID &object_id,
      uint64_t data_size,
      uint64_t metadata_size,
      void *data,
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

std::shared_ptr<MutableObjectReaderInterface> GetTestInterface(
    std::shared_ptr<TestInterface> &interface, const NodeID &node_id) {
  return interface;
}

}  // namespace

TEST(MutableObjectProvider, RegisterWriterChannel) {
  ObjectID object_id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  auto interface = std::make_shared<TestInterface>();

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetTestInterface, interface));
  provider.RegisterWriterChannel(object_id, &node_id);

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

TEST(MutableObjectProvider, HandlePushMutableObject) {
  ObjectID object_id = ObjectID::FromRandom();
  ObjectID local_object_id = ObjectID::FromRandom();
  auto plasma = std::make_shared<TestPlasma>();
  auto interface = std::make_shared<TestInterface>();

  MutableObjectProvider provider(
      plasma,
      /*factory=*/absl::bind_front(GetTestInterface, interface));
  provider.HandleRegisterMutableObject(object_id, /*num_readers=*/1, local_object_id);

  ray::rpc::PushMutableObjectRequest request;
  request.set_writer_object_id(object_id.Binary());
  request.set_data_size(0);
  request.set_metadata_size(0);

  ray::rpc::PushMutableObjectReply reply;
  provider.HandlePushMutableObject(request, &reply);

  std::shared_ptr<RayObject> result;
  EXPECT_EQ(provider.ReadAcquire(local_object_id, result).code(), StatusCode::OK);
  EXPECT_EQ(result->GetSize(), 0UL);
  EXPECT_EQ(provider.ReadRelease(local_object_id).code(), StatusCode::OK);
}

#endif  // defined(__APPLE__) || defined(__linux__)

}  // namespace experimental
}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
