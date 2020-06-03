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

#ifndef RAY_RAYLET_BUNDLE_H
#define RAY_RAYLET_BUNDLE_H

#include <memory>
#include <unordered_map>

#include "ray/common/client_connection.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/cluster_resource_scheduler.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/common/bundle_spec.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/util/process.h"

namespace ray {

namespace raylet {
    class Bundle {
 public:
  /// A constructor that initializes a resource object.
  Bundle(const BundleID &bundle_id, std::unordered_map<std::string,double>resource,
         const std::string &ip_address, std::shared_ptr<ClientConnection> connection,
         rpc::ClientCallManager &client_call_manager);
  ~Bundle();
  const std::string IpAddress() const;
  /// Connect this worker's gRPC client.
  void Connect(int port);
  int Port() const;

  /// Return the resource's ID.
  BundleID BundleId() const;

  // Setter, geter, and clear methods  for allocated_instances_.
  void SetAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    allocated_instances_ = allocated_instances;
  };

  std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() {
    return allocated_instances_;
  };

  void ClearAllocatedInstances() { allocated_instances_ = nullptr; }

 private:

   /// The Bundle's ID.
  BundleID bundle_id_;
  /// IP address of this worker.
  std::string ip_address_;
  /// Port assigned to this worker by the raylet. If this is 0, the actual
  /// port the worker listens (port_) on will be a random one. This is required
  /// because a worker could crash before announcing its port, in which case
  /// we still need to be able to mark that port as free.
  int assigned_port_;
  /// Port that this worker listens on.
  int port_;
  /// Connection state of a worker.
  std::shared_ptr<ClientConnection> connection_;
  /// The capacity of each resource instance allocated to this bundle in order
  /// to satisfy the resource requests.
  std::shared_ptr<TaskResourceInstances> allocated_instances_;

  /// The address of this bundle's owner. The owner is the bundle that
  /// currently holds the lease on this bundle, if any.
  rpc::Address owner_address_;

  /// Task being assigned to this worker.
  BundleSpecification assigned_bundle_;
  

  // TODO(AlisaWu): add other method.

};

} // namespace
} // namespace

#endif  // RAY_RAYLET_BUNDLE_H