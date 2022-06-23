// Copyright 2017 The Ray Authors.
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

#include <memory>

#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

using ReportLocalityDataCallback =
    std::function<void(const ObjectID &, const absl::flat_hash_set<NodeID> &, uint64_t)>;

// Resolve values for futures that were given to us before the value
// was available. This class is thread-safe.
class FutureResolver {
 public:
  FutureResolver(std::shared_ptr<CoreWorkerMemoryStore> store,
                 std::shared_ptr<ReferenceCounter> ref_counter,
                 ReportLocalityDataCallback report_locality_data_callback,
                 std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
                 const rpc::Address &rpc_address)
      : in_memory_store_(store),
        reference_counter_(ref_counter),
        report_locality_data_callback_(std::move(report_locality_data_callback)),
        owner_clients_(core_worker_client_pool),
        rpc_address_(rpc_address) {}

  /// Resolve the value for a future. This will periodically contact the given
  /// owner until the owner dies or the owner has finished creating the object.
  /// In either case, this will put an OBJECT_IN_PLASMA error as the future's
  /// value.
  ///
  /// \param[in] object_id The ID of the future to resolve.
  /// \param[in] owner_address The address of the task or actor that owns the
  /// future.
  void ResolveFutureAsync(const ObjectID &object_id, const rpc::Address &owner_address);

  /// Process a resolved future. This can be used if we already have the objec
  /// status and don't need to ask the owner for it right away.
  ///
  /// \param[in] object_id The ID of the future to resolve.
  /// \param[in] status Any error code from the owner obtaining the object status.
  /// \param[in] object_status The object status.
  void ProcessResolvedObject(const ObjectID &object_id,
                             const rpc::Address &owner_address,
                             const Status &status,
                             const rpc::GetObjectStatusReply &object_status);

 private:
  /// Used to store values of resolved futures.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Used to record nested ObjectRefs of resolved futures.
  std::shared_ptr<ReferenceCounter> reference_counter_;

  /// Used to report locality data received during future resolution.
  const ReportLocalityDataCallback report_locality_data_callback_;

  /// Pool of owner core worker clients.
  std::shared_ptr<rpc::CoreWorkerClientPool> owner_clients_;

  /// Address of our RPC server. Used to notify borrowed objects' owners of our
  /// address, so the owner can contact us to ask when our reference to the
  /// object has gone out of scope.
  const rpc::Address rpc_address_;
};

}  // namespace core
}  // namespace ray
