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

#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"
#include <ray/protobuf/gcs.pb.h>
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"

namespace ray {
namespace gcs {

GcsPlacementGroupScheduler::GcsPlacementGroupScheduler(
    boost::asio::io_context &io_context,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    const gcs::GcsNodeManager &gcs_node_manager,
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_success_handler,
    LeaseResourceClientFactoryFn lease_client_factory)
    : io_context_(io_context),
      gcs_table_storage_(gcs_table_storage),
      gcs_node_manager_(gcs_node_manager),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      schedule_failure_handler_(std::move(schedule_failure_handler)),
      schedule_success_handler_(std::move(schedule_success_handler)),
      lease_client_factory_(std::move(lease_client_factory)) {
  RAY_CHECK(schedule_failure_handler_ != nullptr && schedule_success_handler_ != nullptr);
}

void GcsPlacementGroupScheduler::Schedule(
    std::shared_ptr<GcsPlacementGroup> placement_group) {
  auto bundles = placement_group->GetBundles();
  auto strategy = placement_group->GetStrategy();
  auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();

  if (bundles.size() == 0) {
    schedule_success_handler_(placement_group);
    return;
  }

  if (alive_nodes.empty()) {
    schedule_failure_handler_(placement_group);
    return;
  }

  std::vector<ClientID> schedule_bundles(bundles.size());
  // TODO(AlisaWu): add function to schedule the node.
  if (strategy == rpc::SPREAD) {
    // This is a algorithm to select node.
    auto iter = alive_nodes.begin();
    int index = 0;
    int alive_nodes_size = alive_nodes.size();
    for (; iter != alive_nodes.end(); iter++, index++) {
      for (int base = 0;; base++) {
        if (index + base * alive_nodes_size >= bundles.size()) {
          break;
        } else {
          schedule_bundles[index + base * alive_nodes_size] =
              ClientID::FromBinary(iter->second->node_id());
        }
      }
    }
  } else if (strategy == rpc::PACK) {
    // This is another algorithm to select node.
    for (int pos = 0; pos < bundles.size(); pos++) {
      schedule_bundles[pos] =
          ClientID::FromBinary(alive_nodes.begin()->second->node_id());
    }
  }
  auto decision = GetDecision();
  decision.resize(bundles.size());
  resource_lease_.resize(bundles.size());
  for (int pos = 0; pos < bundles.size(); pos++) {
    RAY_CHECK(node_to_bundles_when_leasing_[schedule_bundles[pos]]
                  .emplace(bundles[pos].BundleID())
                  .second);

    GetLeaseResourceQueue().push_back(std::make_tuple(
        bundles[pos], gcs_node_manager_.GetNode(schedule_bundles[pos]),
        [this, pos, bundles, schedule_bundles, placement_group](
            const Status &status, const rpc::RequestResourceLeaseReply &reply) {
          finish_count++;
          auto decision = GetDecision();
          if (status.ok()) {
            decision[pos] = schedule_bundles[pos];
            for (int i = 0; i < reply.resource_mapping_size(); i++) {
              std::string name = std::string(reply.resource_mapping(i).name());
              for (int j = 0; j < reply.resource_mapping(i).resource_ids_size(); j++) {
                int64_t index =
                    int64_t(reply.resource_mapping(i).resource_ids(j).index());
                double quantity =
                    double(reply.resource_mapping(i).resource_ids(j).quantity());
                resource_lease_[pos].push_back(std::make_tuple(name, index, quantity));
              }
            }
          } else {
            decision[pos] = ClientID::Nil();
          }
          if (finish_count == schedule_bundles.size()) {
            bool lease_success = true;
            for (int i = 0; i < finish_count; i++) {
              if (decision[i] == ClientID::Nil()) {
                lease_success = false;
                break;
              }
            }
            if (lease_success) {
              for (int i = 0; i < bundles.size(); i++) {
                BundleID bundle_id = bundles[i].BundleID();
                rpc::ScheduleData Data;
                Data.set_client_id(decision[i].Binary());
                RAY_CHECK_OK(gcs_table_storage_->BundleScheduleTable().Put(
                    bundle_id, Data, [](Status status) {}));

                rpc::BundleResourceMap resource_map;
                for (int j = 0; j < resource_lease_[i].size(); j++) {
                  auto resource = resource_map.add_resource();
                  resource->set_name(std::get<0>(resource_lease_[i][j]));
                  resource->set_index(std::get<1>(resource_lease_[i][j]));
                  resource->set_quantity(std::get<2>(resource_lease_[i][j]));
                }

                RAY_CHECK_OK(gcs_table_storage_->BundleResourceTable().Put(
                    bundle_id, resource_map, [](Status status) {}));
              }
              schedule_success_handler_(placement_group);
            } else {
              for (int i = 0; i < finish_count; i++) {
                if (decision[i] != ClientID::Nil()) {
                  RetureReourceForNode(bundles[i],
                                       gcs_node_manager_.GetNode(schedule_bundles[pos]));
                  resource_lease_[i].clear();
                }
              }
              schedule_failure_handler_(placement_group);
            }
          }
        }));
  }
  LeaseResourceFromNode();
}

void GcsPlacementGroupScheduler::LeaseResourceFromNode() {
  // const BundleSpecification &bundle,
  // std::shared_ptr<rpc::GcsNodeInfo> node
  auto lease_resourece_queue = GetLeaseResourceQueue();
  for (auto iter = lease_resourece_queue.begin(); iter != lease_resourece_queue.end();
       iter++) {
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
        bundle, [this, node_id, bundle, node, callback](
                    const Status &status, const rpc::RequestResourceLeaseReply &reply) {
          // If the bundle is still in the leasing map and the status is ok, remove the
          // bundle from the leasing map and handle the reply. Otherwise, lease again,
          // because it may be a network exception. If the bundle is not in the leasing
          // map, it means that the placement_group has been cancelled as the node is
          // dead, just do nothing in this case because the gcs_placement_group_manager
          // will reconstruct it again.
          // TODO(AlisaWu): Add placement group cancel
          auto iter = node_to_bundles_when_leasing_.find(node_id);
          if (iter != node_to_bundles_when_leasing_.end()) {
            // If the node is still available, the actor must be still in the leasing map
            // as it is erased from leasing map only when `CancelOnNode` or the
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
              RAY_LOG(INFO) << "Finished leasing resource from " << node_id
                            << " for bundle " << bundle.BundleID();
              callback(status, reply);
              // HandleResourceLeasedReply(bundle, reply);

            } else {
              callback(Status::OutOfMemory("bundle lease resource failed"), reply);
              // TODO(AlisaWu): try to return a fail to the manager
              // Status::OutOfMemory("Lease resource for placement group");
            }
          }
        });
    if (!status.ok()) {
      rpc::RequestResourceLeaseReply reply;
      callback(Status::OutOfMemory("bundle lease resource failed"), reply);
      // TODO(AlisaWu): try to return a fail to the manager
    }
  }
}

void GcsPlacementGroupScheduler::RetureReourceForNode(
    BundleSpecification bundle_spec, std::shared_ptr<ray::rpc::GcsNodeInfo> node) {
  RAY_CHECK(node);

  auto node_id = ClientID::FromBinary(node->node_id());
  RAY_LOG(INFO) << "Start returning resource for node " << node_id << " for bundle "
                << bundle_spec.BundleID();

  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  auto return_client = GetOrConnectLeaseClient(remote_address);
  auto status = return_client->RequestResourceReturn(
      bundle_spec,
      [this, bundle_spec, node](const Status &status,
                                const rpc::RequestResourceReturnReply &reply) {
        if (!status.ok()) {
          // TODO(AlisaWu): add a wait time.
          RetureReourceForNode(bundle_spec, node);
        }
      }

  );
  if (!status.ok()) {
    RetureReourceForNode(bundle_spec, node);
  }
}

Status GcsPlacementGroupScheduler::HandleResourceLeasedReply(
    const rpc::Bundle &bundle, const ray::rpc::RequestResourceLeaseReply &reply) {
  if (reply.resource_mapping_size() == 0) {
    return Status::OutOfMemory("Lease resource for placement group");
  }
  return Status::OK();
}

std::shared_ptr<ResourceLeaseInterface>
GcsPlacementGroupScheduler::GetOrConnectLeaseClient(const rpc::Address &raylet_address) {
  auto node_id = ClientID::FromBinary(raylet_address.raylet_id());
  auto iter = remote_lease_clients_.find(node_id);
  if (iter == remote_lease_clients_.end()) {
    auto lease_client = lease_client_factory_(raylet_address);
    iter = remote_lease_clients_.emplace(node_id, std::move(lease_client)).first;
  }
  return iter->second;
}

}  // namespace gcs
}  // namespace ray