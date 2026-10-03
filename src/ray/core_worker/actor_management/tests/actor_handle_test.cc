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

#include "ray/core_worker/actor_management/actor_handle.h"

#include <string>
#include <unordered_map>
#include <utility>

#include "gtest/gtest.h"

namespace ray {
namespace core {
namespace {

const ActorID kActorId =
    ActorID::Of(JobID::FromInt(1), TaskID::ForDriverTask(JobID::FromInt(1)), 1);

// Builds the handle the actor's creator would serialize into the creation task spec.
ActorHandle CreatedHandle(bool enable_tensor_transport, int32_t max_pending_calls = -1) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  return ActorHandle(kActorId,
                     TaskID::ForDriverTask(JobID::FromInt(1)),
                     owner_address,
                     JobID::FromInt(1),
                     /*initial_cursor=*/ObjectID::FromRandom(),
                     Language::PYTHON,
                     FunctionDescriptorBuilder::BuildPython("mod", "Actor", "", ""),
                     /*extension_data=*/"",
                     /*max_task_retries=*/0,
                     /*name=*/"actor_name",
                     /*ray_namespace=*/"ns",
                     max_pending_calls,
                     /*allow_out_of_order_execution=*/false,
                     /*enable_tensor_transport=*/enable_tensor_transport);
}

// Builds the ActorTableData and TaskSpec pair that the GCS returns for a named actor
// lookup, with the creator's serialized handle carried on the creation task spec.
std::pair<rpc::ActorTableData, rpc::TaskSpec> NamedLookupResult(
    ActorHandle &created_handle) {
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(kActorId.Binary());
  actor_table_data.set_job_id(JobID::FromInt(1).Binary());
  actor_table_data.set_name("actor_name");
  actor_table_data.set_ray_namespace("ns");

  rpc::TaskSpec task_spec;
  task_spec.set_type(rpc::TaskType::ACTOR_CREATION_TASK);
  task_spec.set_language(Language::PYTHON);
  std::string serialized;
  created_handle.Serialize(&serialized);
  task_spec.mutable_actor_creation_task_spec()->set_serialized_actor_handle(serialized);
  return {actor_table_data, task_spec};
}

TEST(ActorHandleTest, NamedLookupPreservesEnableTensorTransport) {
  auto created = CreatedHandle(/*enable_tensor_transport=*/true);
  ASSERT_TRUE(created.EnableTensorTransport());

  const auto [actor_table_data, task_spec] = NamedLookupResult(created);
  const ActorHandle from_lookup(actor_table_data, task_spec);

  EXPECT_TRUE(from_lookup.EnableTensorTransport());
  // The fields this path already carried must keep coming from the GCS data.
  EXPECT_EQ(from_lookup.GetActorID(), kActorId);
  EXPECT_EQ(from_lookup.GetName(), "actor_name");
}

TEST(ActorHandleTest, NamedLookupPreservesMaxPendingCalls) {
  auto created =
      CreatedHandle(/*enable_tensor_transport=*/false, /*max_pending_calls=*/7);
  const auto [actor_table_data, task_spec] = NamedLookupResult(created);

  // The creation task spec has a max_pending_calls field, but nothing on the path that
  // reaches the GCS writes it, so reading it there yields 0 and the limit is lost.
  EXPECT_EQ(ActorHandle(actor_table_data, task_spec).MaxPendingCalls(), 7);
}

// Passes without the fix too: the field was simply never set. Guards against setting it
// unconditionally, or reading it inverted.
TEST(ActorHandleTest, NamedLookupKeepsTensorTransportDisabledWhenNotRequested) {
  auto created = CreatedHandle(/*enable_tensor_transport=*/false);
  const auto [actor_table_data, task_spec] = NamedLookupResult(created);

  EXPECT_FALSE(ActorHandle(actor_table_data, task_spec).EnableTensorTransport());
}

// Also passes without the fix. The empty-handle branch is defensive: every writer whose
// spec reaches the GCS sets serialized_actor_handle, for all languages.
TEST(ActorHandleTest, NamedLookupToleratesMissingSerializedHandle) {
  auto created = CreatedHandle(/*enable_tensor_transport=*/true);
  auto [actor_table_data, task_spec] = NamedLookupResult(created);
  task_spec.mutable_actor_creation_task_spec()->clear_serialized_actor_handle();

  const ActorHandle from_lookup(actor_table_data, task_spec);
  EXPECT_FALSE(from_lookup.EnableTensorTransport());
  EXPECT_EQ(from_lookup.GetActorID(), kActorId);
}

TEST(ActorHandleTest, NamedLookupDiscardsUnparseableSerializedHandle) {
  auto created = CreatedHandle(/*enable_tensor_transport=*/true, /*max_pending_calls=*/7);
  auto [actor_table_data, task_spec] = NamedLookupResult(created);
  // max_pending_calls=7 and enable_tensor_transport=true, then a length-delimited field
  // declaring 127 bytes it does not have. ParseFromString fails only at the last field,
  // so the first two survive in the message unless the failure path clears it.
  task_spec.mutable_actor_creation_task_spec()->set_serialized_actor_handle(
      std::string("\x68\x07\x80\x01\x01\x0a\x7f", 7));

  const ActorHandle from_lookup(actor_table_data, task_spec);
  EXPECT_FALSE(from_lookup.EnableTensorTransport());
  EXPECT_EQ(from_lookup.MaxPendingCalls(), 0);
  EXPECT_EQ(from_lookup.GetActorID(), kActorId);
}

}  // namespace
}  // namespace core
}  // namespace ray
