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

#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {
namespace rpc {

std::shared_ptr<CoreWorkerClientInterface> CoreWorkerClientPool::GetByID(
    ray::WorkerID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return nullptr;
  }
  auto cli = it->second.lock();
  if (cli) {
    return cli;
  }
  auto addr = address_map_[id];
  cli = client_factory_(addr);
  it->second = cli;
  return cli;
}

std::shared_ptr<CoreWorkerClientInterface> CoreWorkerClientPool::GetOrConnect(
    const Address &addr_proto) {
  RAY_CHECK(addr_proto.worker_id() != "");
  absl::MutexLock lock(&mu_);
  auto id = WorkerID::FromBinary(addr_proto.worker_id());
  auto it = client_map_.find(id);
  std::shared_ptr<CoreWorkerClientInterface> cli;
  if (it != client_map_.end()) {
    cli = it->second.lock();
  }

  if (cli) {
    return cli;
  }

  if (it == client_map_.end()) {
    address_map_[id] = addr_proto;
  }

  cli = client_factory_(addr_proto);
  client_map_[id] = cli;

  RAY_LOG(DEBUG) << "Connected to " << addr_proto.ip_address() << ":"
                 << addr_proto.port();
  return cli;
}

void CoreWorkerClientPool::Disconnect(ray::WorkerID id) {
  absl::MutexLock lock(&mu_);
  client_map_.erase(id);
  address_map_.erase(id);
}

}  // namespace rpc
}  // namespace ray
