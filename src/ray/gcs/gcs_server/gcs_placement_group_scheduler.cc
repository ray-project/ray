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

#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

GcsPlacementGroupScheduler::GcsPlacementGroupScheduler(
    boost::asio::io_context &io_context,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    const gcs::GcsNodeManager &gcs_node_manager,
    ReserveResourceClientFactoryFn lease_client_factory)
    : return_timer_(io_context),
      gcs_table_storage_(gcs_table_storage),
      gcs_node_manager_(gcs_node_manager),
      lease_client_factory_(std::move(lease_client_factory)) {
  scheduler_strategies_.push_back(std::make_shared<GcsPackStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsSpreadStrategy>());
}

/// This is an initial algorithm to respect pack algorithm.
/// In this algorithm, we try to pack all the bundle in the first node
/// and don't care the real resource.
ScheduleMap GcsPackStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const GcsNodeManager &node_manager) {
  ScheduleMap schedule_map;
  auto &alive_nodes = node_manager.GetAllAliveNodes();
  for (auto &bundle : bundles) {
    schedule_map[bundle->BundleId()] =
        ClientID::FromBinary(alive_nodes.begin()->second->node_id());
  }
  return schedule_map;
}

/// This is an initial algorithm to respect spread algorithm.
/// In this algorithm, we try to spread all the bundle in different node
/// and don't care the real resource.
ScheduleMap GcsSpreadStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const GcsNodeManager &node_manager) {
  ScheduleMap schedule_map;
  auto &alive_nodes = node_manager.GetAllAliveNodes();
  auto iter = alive_nodes.begin();
  size_t index = 0;
  size_t alive_nodes_size = alive_nodes.size();
  for (; iter != alive_nodes.end(); iter++, index++) {
    for (size_t base = 0;; base++) {
      if (index + base * alive_nodes_size >= bundles.size()) {
        break;
      } else {
        schedule_map[bundles[index + base * alive_nodes_size]->BundleId()] =
            ClientID::FromBinary(iter->second->node_id());
      }
    }
  }
  return schedule_map;
}

void GcsPlacementGroupScheduler::Schedule(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_success_handler) {
  auto bundles = placement_group->GetBundles();
  auto strategy = placement_group->GetStrategy();
  auto alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  /// If the placement group don't have bundle, the placement group creates success.
  if (bundles.empty()) {
    schedule_success_handler(placement_group);
    return;
  }

  // If alive_node is empty, the the placement group creates fail.
  if (alive_nodes.empty()) {
    schedule_failure_handler(placement_group);
    return;
  }
  auto schedule_map =
      scheduler_strategies_[strategy]->Schedule(bundles, gcs_node_manager_);
  // If schedule success, the decision will be set as schedule_map[bundles[pos]]
  // else will be set ClientID::Nil().
  auto decision = std::shared_ptr<std::unordered_map<BundleID, ClientID, pair_hash>>(
      new std::unordered_map<BundleID, ClientID, pair_hash>());
  // To count how many scheduler have been return, which include success and failure.
  auto finish_count = std::make_shared<size_t>();
  /// TODO(AlisaWu): Change the strategy when reserve resource failed.
  for (size_t pos = 0; pos < bundles.size(); pos++) {
    RAY_CHECK(node_to_bundles_when_leasing_[schedule_map.at(bundles[pos]->BundleId())]
                  .emplace(bundles[pos]->BundleId())
                  .second);
    ReserveResourceFromNode(
        bundles[pos],
        gcs_node_manager_.GetNode(schedule_map.at(bundles[pos]->BundleId())),
        [this, pos, bundles, schedule_map, placement_group, decision, finish_count,
         schedule_failure_handler, schedule_success_handler](
            const Status &status, const rpc::RequestResourceReserveReply &reply) {
          (*finish_count)++;
          if (status.ok() && reply.success()) {
            (*decision)[bundles[pos]->BundleId()] =
                schedule_map.at(bundles[pos]->BundleId());
          } else {
            (*decision)[bundles[pos]->BundleId()] = ClientID::Nil();
          }
          if ((*finish_count) == bundles.size()) {
            bool lease_success = true;
            for (size_t i = 0; i < (*finish_count); i++) {
              if ((*decision)[bundles[i]->BundleId()] == ClientID::Nil()) {
                lease_success = false;
                break;
              }
            }
            if (lease_success) {
              rpc::ScheduleData data;
              for (size_t i = 0; i < bundles.size(); i++) {
                data.mutable_schedule_plan()->insert(
                    {bundles[i]->BundleIdAsString(),
                     (*decision)[bundles[i]->BundleId()].Binary()});
              }
              RAY_CHECK_OK(gcs_table_storage_->PlacementGroupScheduleTable().Put(
                  placement_group->GetPlacementGroupID(), data, [](Status status) {}));
              schedule_success_handler(placement_group);
            } else {
              for (size_t i = 0; i < (*finish_count); i++) {
                if ((*decision)[bundles[i]->BundleId()] != ClientID::Nil()) {
                  CancelResourceReserve(bundles[i],
                                        gcs_node_manager_.GetNode(
                                            schedule_map.at(bundles[pos]->BundleId())));
                }
              }
              schedule_failure_handler(placement_group);
            }
          }
        });
  }
}

void GcsPlacementGroupScheduler::ReserveResourceFromNode(
    std::shared_ptr<BundleSpecification> bundle,
    std::shared_ptr<ray::rpc::GcsNodeInfo> node, ReserveResourceCallback callback) {
  RAY_CHECK(node);

  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  auto node_id = ClientID::FromBinary(node->node_id());
  auto lease_client = GetOrConnectLeaseClient(remote_address);
  RAY_LOG(DEBUG) << "Start leasing resource from node " << node_id << " for bundle "
                 << bundle->BundleId().first << bundle->BundleId().second;
  auto status = lease_client->RequestResourceReserve(
      *bundle, [this, node_id, bundle, node, callback](
                   const Status &status, const rpc::RequestResourceReserveReply &reply) {
        // TODO(AlisaWu): Add placement group cancel.
        auto iter = node_to_bundles_when_leasing_.find(node_id);
        if (iter != node_to_bundles_when_leasing_.end()) {
          auto bundle_iter = iter->second.find(bundle->BundleId());
          RAY_CHECK(bundle_iter != iter->second.end());
          if (status.ok()) {
            if (reply.success()) {
              RAY_LOG(DEBUG) << "Finished leasing resource from " << node_id
                             << " for bundle " << bundle->BundleId().first
                             << bundle->BundleId().second;
            } else {
              RAY_LOG(DEBUG) << "Failed leasing resource from " << node_id
                             << " for bundle " << bundle->BundleId().first
                             << bundle->BundleId().second;
            }
            // Remove the bundle from the leasing map as the reply is returned from the
            // remote node.
            iter->second.erase(bundle_iter);
            if (iter->second.empty()) {
              node_to_bundles_when_leasing_.erase(iter);
            }
            callback(status, reply);
          }
        }
      });
  if (!status.ok()) {
    rpc::RequestResourceReserveReply reply;
    reply.set_success(false);
    callback(status, reply);
  }
}

void GcsPlacementGroupScheduler::CancelResourceReserve(
    std::shared_ptr<BundleSpecification> bundle_spec,
    std::shared_ptr<ray::rpc::GcsNodeInfo> node) {
  RAY_CHECK(node);

  auto node_id = ClientID::FromBinary(node->node_id());
  RAY_LOG(DEBUG) << "Start returning resource for node " << node_id << " for bundle "
                 << bundle_spec->BundleId().first << bundle_spec->BundleId().second;

  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  auto return_client = GetOrConnectLeaseClient(remote_address);
  auto status = return_client->CancelResourceReserve(
      *bundle_spec,
      [this, bundle_spec, node](const Status &status,
                                const rpc::CancelResourceReserveReply &reply) {
        if (!status.ok()) {
          return_timer_.expires_from_now(boost::posix_time::milliseconds(5));
          return_timer_.async_wait(
              [this, bundle_spec, node](const boost::system::error_code &error) {
                if (error == boost::asio::error::operation_aborted) {
                  return;
                } else {
                  CancelResourceReserve(bundle_spec, node);
                }
              });
        }
      });
}

std::shared_ptr<ResourceReserveInterface>
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
