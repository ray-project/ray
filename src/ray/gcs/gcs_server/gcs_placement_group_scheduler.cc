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

#include <ray/protobuf/gcs.pb.h>
#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"

namespace ray {
namespace gcs {

GcsPlacementGroupScheduler::GcsPlacementGroupScheduler(
    boost::asio::io_context &io_context, gcs::PlacementGroupInfoAccessor &placement_group_info_accessor,
    const gcs::GcsNodeManager &gcs_node_manager,
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_success_handler)
    : io_context_(io_context),
      placement_group_info_accessor_(placement_group_info_accessor),
      gcs_node_manager_(gcs_node_manager),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      schedule_failure_handler_(std::move(schedule_failure_handler)),
      schedule_success_handler_(std::move(schedule_success_handler)) {
  RAY_CHECK(schedule_failure_handler_ != nullptr && schedule_success_handler_ != nullptr);
}

void GcsPlacementGroupScheduler::Schedule(std::shared_ptr<GcsPlacementGroup> placement_group) {
    auto bundles = placement_group->GetBundles();
    auto strategy = placement_group->GetStrategy();
    auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
    if(alive_nodes.empty()) {
        return;
    }
    std::vector<ClientID>schedule_bundles(bundles.size());
    // TODO(AlisaWu): add function to schedule the node.
    if(strategy == rpc::SPREAD) {
        // This is a algorithm to select node.
        auto iter = alive_nodes.begin();
        int index = 0;
        int alive_nodes_size = alive_nodes.size();
        for( ; iter != alive_nodes.end() ; iter++ , index++){
          for(int base = 0; ;base++){
            if(index + base * alive_nodes_size >= bundles.size()){
              break;
            } else {
              schedule_bundles[index + base * alive_nodes_size] = ClientID::FromBinary(iter->second->node_id());
            }
          }
        }
    } else if(strategy == rpc::PACK) {
        // This is another algorithm to select node.
        for(int i = 0; i < bundles.size(); i ++) {
            schedule_bundles[i] = ClientID::FromBinary(alive_nodes.begin()->second->node_id());
        }
    }

    bool lease_success = true;
    Status status;
    for(int i = 0;i < bundles.size();i ++) {
      status = LeaseWorkerFromNode(bundles[i], gcs_node_manager_.GetNode(schedule_bundles[i]));
      if (status.ok()) {
          continue;
      } else {
        lease_success = false;
        break;
      }
    }

    // TODO(AlisaWu): add the logic.
    if(lease_success){

        // store
    }else{
        // back to step 2
    }


}

Status GcsPlacementGroupScheduler::LeaseWorkerFromNode(const rpc::Bundle &bundle,
                                            std::shared_ptr<rpc::GcsNodeInfo> node) {
  RAY_CHECK(node);

  auto node_id = ClientID::FromBinary(node->node_id());
  RAY_LOG(INFO) << "Start leasing worker from node " << node_id << " for bundle "
                << bundle.bundle_id();

  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  auto lease_client = GetOrConnectLeaseClient(remote_address);
  // TODO(AlisaWu): add a api to connect gcs with client to lease resourse. 
  // ray::TaskSpecification task_spec;
  // task_spec.required_resources = MapFromProtobuf(bundle.unit_resources());
  // 
  // auto status = lease_client->RequestWorkerLease(
  //     task_spec,
  //     [this,node_id, bundle, node](const Status &status,
  //                                  const rpc::RequestWorkerLeaseForBundleReply &reply) {
  //         if (status.ok()) {
  //           RAY_LOG(INFO) << "Finished leasing worker from " <<  node_id << " for bundle "
  //                         << bundle.bundle_id();
  //           return HandleWorkerLeasedForBundleReply(bundle, reply);
  //         } else {
  //           return Status::OutOfMemory("Lease resource for placement group");
  //         }
  //       }
  //     );
  // if (!status.ok()) {
  //   return Status::OutOfMemory("Lease resource for placement group");
  // }
  return Status::OK();
}

Status GcsPlacementGroupScheduler::HandleWorkerLeasedForBundleReply(
  const rpc::Bundle &bundle, const ray::rpc::RequestWorkerLeaseForBundleReply &reply) {
  const auto &worker_address = reply.worker_address();
  if (worker_address.raylet_id().empty()) {
      return Status::OutOfMemory("Lease resource for placement group");
  }
  return Status::OK();
}

std::shared_ptr<WorkerLeaseInterface> GcsPlacementGroupScheduler::GetOrConnectLeaseClient(
    const rpc::Address &raylet_address) {
  auto node_id = ClientID::FromBinary(raylet_address.raylet_id());
  auto iter = remote_lease_clients_.find(node_id);
  if (iter == remote_lease_clients_.end()) {
    auto lease_client = lease_client_factory_(raylet_address);
    iter = remote_lease_clients_.emplace(node_id, std::move(lease_client)).first;
  }
  return iter->second;
}

} // namespace
} // namespace