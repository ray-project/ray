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

  std::unordered_map<std::string,double> resource();

 private:
  std::unordered_map<std::string,double> resource_;
  

  // TODO(AlisaWu): add other method.

};

} // namespace
} // namespace

#endif  // RAY_RAYLET_BUNDLE_H