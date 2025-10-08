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

#pragma once
#include "gmock/gmock.h"
#include "ray/core_worker/reference_counter.h"
namespace ray {
namespace core {

class MockReferenceCounter : public ReferenceCounterInterface {
 public:
  MockReferenceCounter() : ReferenceCounterInterface() {}

  MOCK_METHOD2(AddLocalReference,
               void(const ObjectID &object_id, const std::string &call_sit));

  MOCK_METHOD4(AddBorrowedObject,
               bool(const ObjectID &object_id,
                    const ObjectID &outer_id,
                    const rpc::Address &owner_address,
                    bool foreign_owner_already_monitoring));

  MOCK_METHOD9(AddOwnedObject,
               void(const ObjectID &object_id,
                    const std::vector<ObjectID> &contained_ids,
                    const rpc::Address &owner_address,
                    const std::string &call_site,
                    const int64_t object_size,
                    bool is_reconstructable,
                    bool add_local_ref,
                    const std::optional<NodeID> &pinned_at_node_id,
                    rpc::TensorTransport tensor_transport));

  MOCK_METHOD2(AddObjectOutOfScopeOrFreedCallback,
               bool(const ObjectID &object_id,
                    const std::function<void(const ObjectID &)> callback));

  MOCK_METHOD2(SetObjectRefDeletedCallback,
               bool(const ObjectID &object_id,
                    const std::function<void(const ObjectID &)> callback));

  virtual ~MockReferenceCounter() {}
};

}  // namespace core
}  // namespace ray
