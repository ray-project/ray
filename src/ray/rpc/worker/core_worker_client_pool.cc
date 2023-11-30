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

shared_ptr<CoreWorkerClientInterface> CoreWorkerClientPool::GetOrConnect(
    const Address &addr_proto) {
  RAY_CHECK_NE(addr_proto.worker_id(), "");
  absl::MutexLock lock(&mu_);

  RemoveIdleClients();

  shared_ptr<CoreWorkerClientInterface> connection;
  auto id = WorkerID::FromBinary(addr_proto.worker_id());
  auto it = client_map_.find(id);
  if (it != client_map_.end()) {
    connection = *it->second;
    client_list_.erase(it->second);
  } else {
    connection = client_factory_(addr_proto);
  }
  client_list_.emplace_front(connection);
  client_map_[id] = client_list_.begin();

  RAY_LOG(DEBUG) << "Connected to worker " << id << " with address "
                 << addr_proto.ip_address() << ":" << addr_proto.port();
  return connection;
}

void CoreWorkerClientPool::RemoveIdleClients() {
  while (!client_list_.empty()) {
    // The last client in the list is the least recent accessed client.
    auto it = client_list_.end();
    it--;
    auto id = WorkerID::FromBinary((*it)->Addr().worker_id());
    if ((*it)->GetRpcClient() != nullptr &&
        (*it)->GetRpcClient()->IsChannelIdleAfterRPCs()) {
      client_map_.erase(id);
      client_list_.erase(it);
      RAY_LOG(DEBUG) << "Remove idle client to worker " << id
                     << " , num of clients is now " << client_list_.size();
    } else {
      auto connection = (*it);
      client_list_.erase(it);
      client_list_.emplace_front(connection);
      client_map_[id] = client_list_.begin();
      break;
    }
  }
}

void CoreWorkerClientPool::Disconnect(ray::WorkerID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return;
  }
  client_list_.erase(it->second);
  client_map_.erase(it);
}

}  // namespace rpc
}  // namespace ray
