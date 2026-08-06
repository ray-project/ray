// Copyright 2025 The Ray Authors.
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

#include "ray/core_worker/future_resolver.h"

#include <memory>

#include "gtest/gtest.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/asio/asio_util.h"
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_subscriber.h"
#include "ray/util/clock.h"

namespace ray {
namespace core {

class FutureResolverTest : public ::testing::Test {
 public:
  FutureResolverTest()
      : io_context_("TestOnly.FutureResolverTest"),
        publisher_(std::make_shared<pubsub::MockPublisher>()),
        subscriber_(std::make_shared<pubsub::FakeSubscriber>()),
        memory_store_(
            std::make_shared<CoreWorkerMemoryStore>(io_context_.GetIoService(), clock_)),
        ref_counter_(std::make_shared<ReferenceCounter>(
            rpc::Address(),
            publisher_.get(),
            subscriber_.get(),
            /*is_node_dead=*/[](const NodeID &) { return false; },
            /*free_object_on_nodes_async=*/
            [](const ObjectID &, const absl::flat_hash_set<NodeID> &) {},
            owned_object_count_by_state_,
            owned_object_sizes_by_state_)),
        resolver_(
            memory_store_,
            ref_counter_,
            /*report_locality_data_callback=*/
            [](const ObjectID &, const absl::flat_hash_set<NodeID> &, uint64_t) {},
            // These tests only drive ProcessResolvedObject, which never touches the
            // client pool. A test for ResolveFutureAsync would need a real pool and a
            // non-empty owner worker ID, otherwise it either dereferences this null
            // pool or silently returns early as if we owned the object.
            /*core_worker_client_pool=*/nullptr,
            rpc::Address()) {}

  // Registers a local reference so that CoreWorkerMemoryStore::Put actually inserts
  // the object. Without a reference, Put treats the object as added-and-immediately-
  // deleted and GetIfExists would return null regardless of the resolved status.
  ObjectID AddBorrowedObject() {
    ObjectID object_id = ObjectID::FromRandom();
    ref_counter_->AddLocalReference(object_id, "");
    return object_id;
  }

  // Stop the io thread before the members it may run callbacks against are
  // destroyed.
  void TearDown() override { io_context_.Stop(); }

 protected:
  Clock clock_;
  InstrumentedIOContextWithThread io_context_;
  ray::observability::FakeGauge owned_object_count_by_state_;
  ray::observability::FakeGauge owned_object_sizes_by_state_;
  std::shared_ptr<pubsub::MockPublisher> publisher_;
  std::shared_ptr<pubsub::FakeSubscriber> subscriber_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  std::shared_ptr<ReferenceCounter> ref_counter_;
  FutureResolver resolver_;
};

// The owner replies FREED when the object was freed (ray.internal.free) while the
// reference was still in scope. The borrower must store an error, otherwise a
// ray.get() on the reference blocks forever with nothing ever filling the store.
TEST_F(FutureResolverTest, ProcessResolvedObjectFreedStoresError) {
  ObjectID object_id = AddBorrowedObject();
  rpc::GetObjectStatusReply reply;
  reply.set_status(rpc::GetObjectStatusReply::FREED);

  resolver_.ProcessResolvedObject(object_id, rpc::Address(), Status::OK(), reply);

  auto object = memory_store_->GetIfExists(object_id);
  ASSERT_NE(object, nullptr);
  rpc::ErrorType error_type;
  ASSERT_TRUE(object->IsException(&error_type));
  ASSERT_EQ(error_type, rpc::ErrorType::OBJECT_FREED);
}

// Sibling statuses, kept as regression anchors for the branch above.
TEST_F(FutureResolverTest, ProcessResolvedObjectOutOfScopeStoresError) {
  ObjectID object_id = AddBorrowedObject();
  rpc::GetObjectStatusReply reply;
  reply.set_status(rpc::GetObjectStatusReply::OUT_OF_SCOPE);

  resolver_.ProcessResolvedObject(object_id, rpc::Address(), Status::OK(), reply);

  auto object = memory_store_->GetIfExists(object_id);
  ASSERT_NE(object, nullptr);
  rpc::ErrorType error_type;
  ASSERT_TRUE(object->IsException(&error_type));
  ASSERT_EQ(error_type, rpc::ErrorType::OBJECT_DELETED);
}

TEST_F(FutureResolverTest, ProcessResolvedObjectOwnerUnreachableStoresError) {
  ObjectID object_id = AddBorrowedObject();
  rpc::GetObjectStatusReply reply;

  resolver_.ProcessResolvedObject(
      object_id, rpc::Address(), Status::IOError("owner unreachable"), reply);

  auto object = memory_store_->GetIfExists(object_id);
  ASSERT_NE(object, nullptr);
  rpc::ErrorType error_type;
  ASSERT_TRUE(object->IsException(&error_type));
  ASSERT_EQ(error_type, rpc::ErrorType::OWNER_DIED);
}

}  // namespace core
}  // namespace ray
