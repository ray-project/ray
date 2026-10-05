// Copyright 2026 The Ray Authors.
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
#include <string>
#include <string_view>

#include "absl/container/flat_hash_set.h"
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
            [this](const ObjectID &object_id,
                   const absl::flat_hash_set<NodeID> &locations,
                   uint64_t object_size) {
              locality_reports_++;
              reported_object_id_ = object_id;
              reported_locations_ = locations;
              reported_object_size_ = object_size;
            },
            // These tests only drive ProcessResolvedObject, which never touches the
            // client pool. A test for ResolveFutureAsync would need a real pool and a
            // non-empty owner worker ID, otherwise it either dereferences this null
            // pool or silently returns early as if we owned the object.
            /*core_worker_client_pool=*/nullptr,
            rpc::Address()) {}

  // Registers a local reference so that CoreWorkerMemoryStore::Put actually inserts
  // the object. Without a reference, Put treats the object as added-and-immediately-
  // deleted and GetIfExists would return null regardless of the resolved status.
  ObjectID NewObjectWithLocalRef() {
    ObjectID object_id = ObjectID::FromRandom();
    ref_counter_->AddLocalReference(object_id, "");
    return object_id;
  }

  // Stop the io thread before the members it may run callbacks against are
  // destroyed.
  void TearDown() override { io_context_.Stop(); }

 protected:
  FakeClock clock_;
  InstrumentedIOContextWithThread io_context_;
  ray::observability::FakeGauge owned_object_count_by_state_;
  ray::observability::FakeGauge owned_object_sizes_by_state_;
  std::shared_ptr<pubsub::MockPublisher> publisher_;
  std::shared_ptr<pubsub::FakeSubscriber> subscriber_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  std::shared_ptr<ReferenceCounter> ref_counter_;
  // Filled by report_locality_data_callback_.
  int locality_reports_ = 0;
  ObjectID reported_object_id_;
  absl::flat_hash_set<NodeID> reported_locations_;
  uint64_t reported_object_size_ = 0;
  FutureResolver resolver_;
};

// A value this build has no name for. One case the catch-all exists for: proto3 enums
// are open, so a reply can carry one. 2 is such a value now that FREED is reserved, and
// an owner on an older build can still send it.
TEST_F(FutureResolverTest, ProcessResolvedObjectUnknownStatusStoresError) {
  ObjectID object_id = NewObjectWithLocalRef();
  rpc::GetObjectStatusReply reply;
  reply.set_status(static_cast<rpc::GetObjectStatusReply::ObjectStatus>(2));

  resolver_.ProcessResolvedObject(object_id, rpc::Address(), Status::OK(), reply);

  auto object = memory_store_->GetIfExists(object_id);
  ASSERT_NE(object, nullptr);
  rpc::ErrorType error_type;
  ASSERT_TRUE(object->IsException(&error_type));
  ASSERT_EQ(error_type, rpc::ErrorType::OBJECT_LOST);
}

// The branches that predate the catch-all. All of them pass without it, and are here so
// that a later edit cannot reroute them into it unnoticed.

// CREATED is the branch that carries the owner's reply through to the caller. An object
// already in plasma comes back with data empty and only the marker in metadata, so this
// is the case that catches a CREATED check narrowed to replies carrying data.
TEST_F(FutureResolverTest, ProcessResolvedObjectCreatedInPlasmaStoresPlasmaMarker) {
  ObjectID object_id = NewObjectWithLocalRef();
  NodeID node_id = NodeID::FromRandom();
  rpc::GetObjectStatusReply reply;
  reply.set_status(rpc::GetObjectStatusReply::CREATED);
  reply.mutable_object()->set_metadata(
      std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA)));
  reply.add_node_ids(node_id.Binary());
  reply.set_object_size(1234);

  resolver_.ProcessResolvedObject(object_id, rpc::Address(), Status::OK(), reply);

  auto object = memory_store_->GetIfExists(object_id);
  ASSERT_NE(object, nullptr);
  ASSERT_FALSE(object->HasData());
  ASSERT_TRUE(object->IsInPlasmaError());
  ASSERT_EQ(locality_reports_, 1);
  ASSERT_EQ(reported_object_id_, object_id);
  ASSERT_EQ(reported_locations_, absl::flat_hash_set<NodeID>{node_id});
  ASSERT_EQ(reported_object_size_, 1234U);
}

// Python inlines a value with metadata alongside it, so set both. The assertion is on
// the data because metadata of "PYTHON" makes IsException() false whether or not the
// payload arrived.
TEST_F(FutureResolverTest, ProcessResolvedObjectCreatedInlineStoresValue) {
  ObjectID object_id = NewObjectWithLocalRef();
  rpc::GetObjectStatusReply reply;
  reply.set_status(rpc::GetObjectStatusReply::CREATED);
  reply.mutable_object()->set_data("hello");
  reply.mutable_object()->set_metadata("PYTHON");

  resolver_.ProcessResolvedObject(object_id, rpc::Address(), Status::OK(), reply);

  auto object = memory_store_->GetIfExists(object_id);
  ASSERT_NE(object, nullptr);
  ASSERT_TRUE(object->HasData());
  ASSERT_EQ(std::string_view(reinterpret_cast<const char *>(object->GetData()->Data()),
                             object->GetData()->Size()),
            "hello");
}

TEST_F(FutureResolverTest, ProcessResolvedObjectOutOfScopeStoresError) {
  ObjectID object_id = NewObjectWithLocalRef();
  rpc::GetObjectStatusReply reply;
  reply.set_status(rpc::GetObjectStatusReply::OUT_OF_SCOPE);

  resolver_.ProcessResolvedObject(object_id, rpc::Address(), Status::OK(), reply);

  auto object = memory_store_->GetIfExists(object_id);
  ASSERT_NE(object, nullptr);
  rpc::ErrorType error_type;
  ASSERT_TRUE(object->IsException(&error_type));
  ASSERT_EQ(error_type, rpc::ErrorType::OBJECT_DELETED);
}

// An RPC failure rather than a reply status, so this one enters through !status.ok().
TEST_F(FutureResolverTest, ProcessResolvedObjectOwnerUnreachableStoresError) {
  ObjectID object_id = NewObjectWithLocalRef();
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
