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

#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/core_worker/experimental_mutable_object_provider.h"
#include "ray/object_manager/common.h"

using namespace testing;

namespace ray {
namespace core {
namespace experimental {

#if defined(__APPLE__) || defined(__linux__)

namespace {

class TestInterface : public MutableObjectReaderInterface {
 public:
  void RegisterMutableObject(
      const ObjectID &object_id,
      int64_t num_readers,
      const ObjectID &local_reader_object_id,
      const rpc::ClientCallback<rpc::RegisterMutableObjectReply> &callback) override {
    absl::PrintF("RegisterMutableObject\n");
  }

  void PushMutableObject(
      const ObjectID &object_id,
      uint64_t data_size,
      uint64_t metadata_size,
      void *data,
      const rpc::ClientCallback<rpc::PushMutableObjectReply> &callback) override {
    absl::PrintF("PushMutableObject\n");
  }
};

std::shared_ptr<MutableObjectReaderInterface> CreateTestInterface(const NodeID &node_id) {
  return std::make_shared<TestInterface>();
}

}  // namespace

TEST(MutableObjectProvider, RegisterWriterChannel) {
  MutableObjectProvider provider(/*plasma=*/std::make_shared<plasma::PlasmaClient>(),
                                 /*factory=*/CreateTestInterface);
  provider.RegisterWriterChannel(ObjectID::FromRandom(), NodeID::FromRandom());
}

#endif  // defined(__APPLE__) || defined(__linux__)

}  // namespace experimental
}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
