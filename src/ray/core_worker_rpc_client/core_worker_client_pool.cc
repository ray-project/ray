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

#include "ray/core_worker_rpc_client/core_worker_client_pool.h"

#include <memory>
#include <utility>
#include <vector>

#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "ray/util/network_util.h"

namespace ray {
namespace rpc {

std::function<void()> CoreWorkerClientPool::GetDefaultUnavailableTimeoutCallback(
    gcs::GcsClient *gcs_client,
    rpc::CoreWorkerClientPool *worker_client_pool,
    rpc::RayletClientPool *raylet_client_pool,
    const rpc::Address &addr) {
  return [addr, gcs_client, worker_client_pool, raylet_client_pool]() {
    const NodeID node_id = NodeID::FromBinary(addr.node_id());
    const WorkerID worker_id = WorkerID::FromBinary(addr.worker_id());

    auto check_worker_alive = [raylet_client_pool,
                               worker_client_pool,
                               worker_id,
                               node_id](const rpc::GcsNodeInfo &node_info) {
      auto raylet_addr = RayletClientPool::GenerateRayletAddress(
          node_id, node_info.node_manager_address(), node_info.node_manager_port());
      auto raylet_client = raylet_client_pool->GetOrConnectByAddress(raylet_addr);
      raylet_client->IsLocalWorkerDead(
          worker_id,
          [worker_client_pool, worker_id, node_id](const Status &status,
                                                   rpc::IsLocalWorkerDeadReply &&reply) {
            if (!status.ok()) {
              // Will try again when unavailable timeout callback is retried.
              RAY_LOG(INFO).WithField(worker_id).WithField(node_id)
                  << "Failed to check if worker is dead on request to raylet";
              return;
            }
            if (reply.is_dead()) {
              RAY_LOG(INFO).WithField(worker_id).WithField(node_id)
                  << "Disconnecting core worker client because the worker is dead";
              worker_client_pool->Disconnect(worker_id);
            }
          });
    };

    auto gcs_check_node_alive =
        [check_worker_alive, node_id, worker_id, worker_client_pool, gcs_client]() {
          gcs_client->Nodes().AsyncGetAll(
              [check_worker_alive = std::move(check_worker_alive),
               worker_id,
               node_id,
               worker_client_pool](const Status &status,
                                   std::vector<rpc::GcsNodeInfo> &&nodes) {
                if (!status.ok()) {
                  // Will try again when unavailable timeout callback is retried.
                  RAY_LOG(INFO) << "Failed to get node info from GCS";
                  return;
                }
                if (nodes.empty() || nodes[0].state() != rpc::GcsNodeInfo::ALIVE) {
                  // The node is dead or GCS doesn't know about this node.
                  // There's only two reasons the GCS doesn't know about the node:
                  // 1. The node isn't registered yet.
                  // 2. The GCS erased the dead node based on
                  //    maximum_gcs_dead_node_cached_count.
                  // In this case, it must be 2 since there's no way for a component to
                  // know about a remote node id until the gcs has registered it.
                  RAY_LOG(INFO).WithField(worker_id).WithField(node_id)
                      << "Disconnecting core worker client because its node is dead";
                  worker_client_pool->Disconnect(worker_id);
                  return;
                }
                check_worker_alive(nodes[0]);
              },
              -1,
              {node_id});
        };

    if (gcs_client->Nodes().IsSubscribedToNodeChange()) {
      auto *node_info = gcs_client->Nodes().Get(node_id, /*filter_dead_nodes=*/false);
      if (node_info == nullptr) {
        // Node could be dead or info may have not made it to the subscriber cache yet.
        // Check with the GCS to confirm if the node is dead.
        gcs_check_node_alive();
        return;
      }
      if (node_info->state() == rpc::GcsNodeInfo::DEAD) {
        RAY_LOG(INFO).WithField(worker_id).WithField(node_id)
            << "Disconnecting core worker client because its node is dead.";
        worker_client_pool->Disconnect(worker_id);
        return;
      }
      // Node is alive so check worker.
      check_worker_alive(*node_info);
      return;
    }
    // Not subscribed so ask GCS.
    gcs_check_node_alive();
  };
}

std::shared_ptr<CoreWorkerClientInterface> CoreWorkerClientPool::GetOrConnect(
    const Address &addr_proto) {
  RAY_CHECK_NE(addr_proto.worker_id(), "");
  absl::MutexLock lock(&mu_);

  RemoveIdleClients();

  CoreWorkerClientEntry entry;
  auto node_id = NodeID::FromBinary(addr_proto.node_id());
  auto worker_id = WorkerID::FromBinary(addr_proto.worker_id());
  auto it = worker_client_map_.find(worker_id);
  if (it != worker_client_map_.end()) {
    entry = *it->second;
    client_list_.erase(it->second);
  } else {
    entry = CoreWorkerClientEntry(
        worker_id, node_id, core_worker_client_factory_(addr_proto));
  }
  client_list_.emplace_front(entry);
  worker_client_map_[worker_id] = client_list_.begin();
  node_clients_map_[node_id][worker_id] = client_list_.begin();

  RAY_LOG(DEBUG) << "Connected to worker " << worker_id << " with address "
                 << BuildAddress(addr_proto.ip_address(), addr_proto.port());
  return entry.core_worker_client_;
}

void CoreWorkerClientPool::RemoveIdleClients() {
  while (!client_list_.empty()) {
    auto worker_id = client_list_.back().worker_id_;
    auto node_id = client_list_.back().node_id_;
    // The last client in the list is the least recent accessed client.
    if (client_list_.back().core_worker_client_->IsIdleAfterRPCs()) {
      worker_client_map_.erase(worker_id);
      EraseFromNodeClientMap(node_id, worker_id);
      client_list_.pop_back();
      RAY_LOG(DEBUG) << "Remove idle client to worker " << worker_id
                     << " , num of clients is now " << client_list_.size();
    } else {
      auto entry = client_list_.back();
      client_list_.pop_back();
      client_list_.emplace_front(entry);
      worker_client_map_[worker_id] = client_list_.begin();
      node_clients_map_[node_id][worker_id] = client_list_.begin();
      break;
    }
  }
}

void CoreWorkerClientPool::Disconnect(const WorkerID &id) {
  absl::MutexLock lock(&mu_);
  auto it = worker_client_map_.find(id);
  if (it == worker_client_map_.end()) {
    return;
  }
  EraseFromNodeClientMap(it->second->node_id_, /*worker_id=*/id);
  client_list_.erase(it->second);
  worker_client_map_.erase(it);
}

void CoreWorkerClientPool::Disconnect(const NodeID &node_id) {
  absl::MutexLock lock(&mu_);
  auto node_client_map_it = node_clients_map_.find(node_id);
  if (node_client_map_it == node_clients_map_.end()) {
    return;
  }
  auto &node_worker_id_client_map = node_client_map_it->second;
  for (auto &[worker_id, client_iterator] : node_worker_id_client_map) {
    worker_client_map_.erase(worker_id);
    client_list_.erase(client_iterator);
  }
  node_clients_map_.erase(node_client_map_it);
}

void CoreWorkerClientPool::EraseFromNodeClientMap(const NodeID &node_id,
                                                  const WorkerID &worker_id) {
  auto node_client_map_it = node_clients_map_.find(node_id);
  if (node_client_map_it == node_clients_map_.end()) {
    return;
  }
  auto &node_worker_id_client_map = node_client_map_it->second;
  node_worker_id_client_map.erase(worker_id);
  if (node_worker_id_client_map.empty()) {
    node_clients_map_.erase(node_client_map_it);
  }
}

}  // namespace rpc
}  // namespace ray
