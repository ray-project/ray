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
    boost::asio::io_context &io_context, 
    gcs::PlacementGroupInfoAccessor &placement_group_info_accessor,
    const gcs::GcsNodeManager &gcs_node_manager,
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_success_handler,
    LeaseClientFactoryFn lease_client_factory)
    : io_context_(io_context),
      placement_group_info_accessor_(placement_group_info_accessor),
      gcs_node_manager_(gcs_node_manager),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      schedule_failure_handler_(std::move(schedule_failure_handler)),
      schedule_success_handler_(std::move(schedule_success_handler)),
      lease_client_factory_(std::move(lease_client_factory)){
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
    // TODO(AlisaWu): add a callback function.
    bool lease_success = true;
    Status status;
    for(int i = 0;i < bundles.size();i ++) {
      RAY_CHECK(node_to_bundles_when_leasing_[schedule_bundles[i]]
                    .emplace(bundles[i].BundleID())
                    .second);
            
      GetLeaseResourceQueue().push_back(std::make_tuple(bundles[i], gcs_node_manager_.GetNode(schedule_bundles[i]),
      [](const Status &status){
        if(!status.ok()){
          
        }
      }));
    }
    LeaseResourceFromNode();

    // TODO(AlisaWu): add the logic.
    // if(lease_success){
      

    //     // store
    // }else{
      
    //     // back to step 2
    // }


}

void GcsPlacementGroupScheduler::LeaseResourceFromNode() {
  // const BundleSpecification &bundle,
  // std::shared_ptr<rpc::GcsNodeInfo> node
  auto lease_resourece_queue = GetLeaseResourceQueue();
  for(auto iter = lease_resourece_queue.begin(); iter != lease_resourece_queue.end(); iter++){
    auto bundle = std::get<0>(*iter);
    auto node = std::get<1>(*iter);
    auto callback = std::get<2>(*iter);
    RAY_CHECK(node);

    auto node_id = ClientID::FromBinary(node->node_id());
    RAY_LOG(INFO) << "Start leasing resource from node " << node_id << " for bundle "
                  << bundle.BundleID();

    rpc::Address remote_address;
    remote_address.set_raylet_id(node->node_id());
    remote_address.set_ip_address(node->node_manager_address());
    remote_address.set_port(node->node_manager_port());
    auto lease_client = GetOrConnectLeaseClient(remote_address);
    
    auto status = lease_client->RequestResourceLease(
        bundle,
        [this,node_id, bundle, node](const Status &status,
                                    const rpc::RequestResourceLeaseReply &reply) {
            // If the bundle is still in the leasing map and the status is ok, remove the bundle
            // from the leasing map and handle the reply. Otherwise, lease again, because it
            // may be a network exception.
            // If the bundle is not in the leasing map, it means that the placement_group has been
            // cancelled as the node is dead, just do nothing in this case because the
            // gcs_placement_group_manager will reconstruct it again.
            // TODO(AlisaWu): Add placement group cancel
            auto iter = node_to_bundles_when_leasing_.find(node_id);
            if(iter != node_to_bundles_when_leasing_.end()){
              // If the node is still available, the actor must be still in the leasing map as
              // it is erased from leasing map only when `CancelOnNode` or the
              // `RequestWorkerLeaseReply` is received from the node, so try lease again.
              auto bundle_iter = iter->second.find(bundle.BundleID());
              RAY_CHECK(bundle_iter != iter->second.end());
              if (status.ok()) {
                // Remove the actor from the leasing map as the reply is returned from the
                // remote node.
                iter->second.erase(bundle_iter);
                if (iter->second.empty()) {
                  node_to_bundles_when_leasing_.erase(iter);
                }
                RAY_LOG(INFO) << "Finished leasing resource from " <<  node_id << " for bundle "
                            << bundle.BundleID();
                // HandleResourceLeasedReply(bundle, reply);
              } else {
                // TODO(AlisaWu): try to return a fail to the manager
                // Status::OutOfMemory("Lease resource for placement group");
              }
            }
          }
        );
    if(!status.ok()){
      // TODO(AlisaWu): try to return a fail to the manager
    }
  }
  
}

Status GcsPlacementGroupScheduler::HandleResourceLeasedReply(
  const rpc::Bundle &bundle, const ray::rpc::RequestResourceLeaseReply &reply) {
  if (reply.resource_mapping_size() == 0){
    return Status::OutOfMemory("Lease resource for placement group");
  }
  return Status::OK();
}

std::shared_ptr<ResourceLeaseInterface> GcsPlacementGroupScheduler::GetOrConnectLeaseClient(
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