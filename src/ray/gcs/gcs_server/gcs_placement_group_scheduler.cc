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
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_success_handler,
    LeaseResourceClientFactoryFn lease_client_factory)
    : io_context_(io_context),
      gcs_table_storage_(gcs_table_storage),
      gcs_node_manager_(gcs_node_manager),
      schedule_failure_handler_(std::move(schedule_failure_handler)),
      schedule_success_handler_(std::move(schedule_success_handler)),
      lease_client_factory_(std::move(lease_client_factory)) {
  RAY_CHECK(schedule_failure_handler_ != nullptr && schedule_success_handler_ != nullptr);
  scheduler.push_back(std::make_shared<GcsPackStrategy>());
  scheduler.push_back(std::make_shared<GcsSpreadStrategy>());
}

std::unordered_map<BundleID, ClientID> GcsPackStrategy::Schedule(
    std::vector<ray::BundleSpecification> &bundles, const GcsNodeManager &node_manager) {
  std::unordered_map<BundleID, ClientID> schedule_map;
  auto &alive_nodes = node_manager.GetAllAliveNodes();
  for (size_t pos = 0; pos < bundles.size(); pos++) {
    schedule_map[bundles[pos].BundleId()] =
        ClientID::FromBinary(alive_nodes.begin()->second->node_id());
  }
  return schedule_map;
}

std::unordered_map<BundleID, ClientID> GcsSpreadStrategy::Schedule(
    std::vector<ray::BundleSpecification> &bundles, const GcsNodeManager &node_manager) {
  std::unordered_map<BundleID, ClientID> schedule_map;
  auto &alive_nodes = node_manager.GetAllAliveNodes();
  auto iter = alive_nodes.begin();
  size_t index = 0;
  size_t alive_nodes_size = alive_nodes.size();
  for (; iter != alive_nodes.end(); iter++, index++) {
    for (size_t base = 0;; base++) {
      if (index + base * alive_nodes_size >= bundles.size()) {
        break;
      } else {
        schedule_map[bundles[index + base * alive_nodes_size].BundleId()] =
            ClientID::FromBinary(iter->second->node_id());
      }
    }
  }
  return schedule_map;
}

void GcsPlacementGroupScheduler::Schedule(
    std::shared_ptr<GcsPlacementGroup> placement_group) {
  auto bundles = placement_group->GetBundles();
  auto strategy = placement_group->GetStrategy();
  if (bundles.size() == 0) {
    schedule_success_handler_(placement_group);
    return;
  }
  auto schedule_map = scheduler[strategy]->Schedule(bundles, gcs_node_manager_);
  std::vector<ClientID> decision;
  decision.resize(bundles.size());
  size_t finish_count = 0;
  for (size_t pos = 0; pos < bundles.size(); pos++) {
    RAY_CHECK(node_to_bundles_when_leasing_[schedule_map.at(bundles[pos].BundleId())]
                  .emplace(bundles[pos].BundleId())
                  .second);

    GetLeaseResourceQueue().push_back(std::make_tuple(
        bundles[pos], gcs_node_manager_.GetNode(schedule_map.at(bundles[pos].BundleId())),
        [this, pos, bundles, schedule_map, placement_group, &decision, &finish_count](
            const Status &status, const rpc::RequestResourceLeaseReply &reply) {
          finish_count++;
          if (status.ok()) {
            decision[pos] = schedule_map.at(bundles[pos].BundleId());
          } else {
            decision[pos] = ClientID::Nil();
          }
          if (finish_count == bundles.size()) {
            bool lease_success = true;
            for (size_t i = 0; i < finish_count; i++) {
              if (decision[i] == ClientID::Nil()) {
                lease_success = false;
                break;
              }
            }
            if (lease_success) {
              rpc::ScheduleData data;
              for (size_t i = 0; i < bundles.size(); i++) {
                auto schedule_plan = data.add_schedule_plan();
                schedule_plan->set_client_id(
                    schedule_map.at(bundles[i].BundleId()).Binary());
                schedule_plan->set_bundle_id(bundles[i].BundleId().Binary());
              }
              RAY_CHECK_OK(gcs_table_storage_->PlacementGroupScheduleTable().Put(
                  placement_group->GetPlacementGroupID(), data, [](Status status) {}));
              schedule_success_handler_(placement_group);
            } else {
              for (size_t i = 0; i < finish_count; i++) {
                if (decision[i] != ClientID::Nil()) {
                  RetureReourceForNode(bundles[i],
                                       gcs_node_manager_.GetNode(
                                           schedule_map.at(bundles[pos].BundleId())));
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
                  << bundle.BundleId();

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
            auto bundle_iter = iter->second.find(bundle.BundleId());
            RAY_CHECK(bundle_iter != iter->second.end());
            if (status.ok()) {
              // Remove the actor from the leasing map as the reply is returned from the
              // remote node.
              iter->second.erase(bundle_iter);
              if (iter->second.empty()) {
                node_to_bundles_when_leasing_.erase(iter);
              }
              RAY_LOG(INFO) << "Finished leasing resource from " << node_id
                            << " for bundle " << bundle.BundleId();
              callback(status, reply);
            } else {
              callback(Status::OutOfMemory("bundle lease resource failed"), reply);
            }
          }
        });
    if (!status.ok()) {
      rpc::RequestResourceLeaseReply reply;
      callback(Status::OutOfMemory("bundle lease resource failed"), reply);
    }
  }
}

void GcsPlacementGroupScheduler::RetureReourceForNode(
    BundleSpecification bundle_spec, std::shared_ptr<ray::rpc::GcsNodeInfo> node) {
  RAY_CHECK(node);

  auto node_id = ClientID::FromBinary(node->node_id());
  RAY_LOG(INFO) << "Start returning resource for node " << node_id << " for bundle "
                << bundle_spec.BundleId();

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