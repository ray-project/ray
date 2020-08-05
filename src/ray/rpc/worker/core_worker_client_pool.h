// Copyright 2020 The Ray Authors.
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

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/container/flat_hash_map.h"

#include "ray/common/id.h"
#include "ray/rpc/worker/core_worker_client.h"

using std::shared_ptr;
using absl::optional;


namespace ray {
namespace rpc {

class CoreWorkerClientPool {
 public:
  CoreWorkerClientPool() = delete;
  CoreWorkerClientPool(rpc::ClientCallManager& ccm):
      client_factory_(defaultClientFactory(ccm)) {};

  CoreWorkerClientPool(ClientFactoryFn client_factory):
      client_factory_(client_factory) {};

  optional<shared_ptr<CoreWorkerClientInterface>> GetByID(ray::WorkerID id);
  shared_ptr<CoreWorkerClientInterface> GetOrConnect(const WorkerAddress& addr);
  shared_ptr<CoreWorkerClientInterface> GetOrConnect(const Address& addr_proto);
  void Disconnect(ray::WorkerID id);

 private:
  ClientFactoryFn defaultClientFactory(rpc::ClientCallManager& ccm) const {
    return [&](const rpc::Address& addr) {
      return std::shared_ptr<rpc::CoreWorkerClient>(
        new rpc::CoreWorkerClient(addr, ccm));
    };
  };

  ClientFactoryFn client_factory_;

  absl::Mutex mu_;

  absl::flat_hash_map<ray::WorkerID, shared_ptr<CoreWorkerClientInterface>> client_map_ GUARDED_BY(mu_);
};

}  // namespace rpc
}  // namespace ray
