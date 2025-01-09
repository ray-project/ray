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

#include "ray/gcs/gcs_client/accessor.h"

#include <future>

#include "ray/gcs/gcs_client/gcs_client.h"

namespace ray {
namespace gcs {

VirtualClusterInfoAccessor::VirtualClusterInfoAccessor(GcsClient *client_impl)
    : client_impl_(client_impl) {}

Status VirtualClusterInfoAccessor::AsyncGet(
    const VirtualClusterID &virtual_cluster_id,
    const OptionalItemCallback<rpc::VirtualClusterTableData> &callback) {
  RAY_LOG(DEBUG).WithField(virtual_cluster_id) << "Getting virtual cluster info";
  rpc::GetVirtualClustersRequest request;
  request.set_virtual_cluster_id(virtual_cluster_id.Binary());
  client_impl_->GetGcsRpcClient().GetVirtualClusters(
      request,
      [virtual_cluster_id, callback](const Status &status,
                                     rpc::GetVirtualClustersReply &&reply) {
        if (reply.virtual_cluster_data_list_size() == 0) {
          callback(status, std::nullopt);
        } else {
          RAY_CHECK(reply.virtual_cluster_data_list_size() == 1);
          callback(status, reply.virtual_cluster_data_list().at(0));
        }
        RAY_LOG(DEBUG).WithField(virtual_cluster_id)
            << "Finished getting virtual cluster info";
      });
  return Status::OK();
}

Status VirtualClusterInfoAccessor::AsyncGetAll(
    bool include_job_clusters,
    bool only_include_indivisible_clusters,
    const MultiItemCallback<rpc::VirtualClusterTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all virtual cluster info.";
  rpc::GetVirtualClustersRequest request;
  request.set_include_job_clusters(true);
  request.set_only_include_indivisible_clusters(true);
  client_impl_->GetGcsRpcClient().GetVirtualClusters(
      request, [callback](const Status &status, rpc::GetVirtualClustersReply &&reply) {
        callback(
            status,
            VectorFromProtobuf(std::move(*reply.mutable_virtual_cluster_data_list())));
        RAY_LOG(DEBUG) << "Finished getting all virtual cluster info, status = "
                       << status;
      });
  return Status::OK();
}

Status VirtualClusterInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<VirtualClusterID, rpc::VirtualClusterTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  const auto updated_subscribe =
      [this, subscribe](const VirtualClusterID &virtual_cluster_id,
                        rpc::VirtualClusterTableData &&virtual_cluster_data) {
        auto iter = virtual_clusters_.find(virtual_cluster_id);
        if (iter != virtual_clusters_.end()) {
          if (virtual_cluster_data.revision() < iter->second.revision()) {
            RAY_LOG(WARNING) << "The revision of the received virtual cluster ("
                             << virtual_cluster_id << ") is outdated. Ignore it.";
            return;
          }
          if (virtual_cluster_data.is_removed()) {
            virtual_clusters_.erase(iter);
          } else {
            iter->second = virtual_cluster_data;
          }
        } else {
          virtual_clusters_[virtual_cluster_id] = virtual_cluster_data;
        }

        subscribe(virtual_cluster_id, std::move(virtual_cluster_data));
      };
  fetch_all_data_operation_ =
      [this, subscribe, updated_subscribe](const StatusCallback &done) {
        auto callback =
            [this, subscribe, updated_subscribe, done](
                const Status &status,
                std::vector<rpc::VirtualClusterTableData> &&virtual_cluster_info_list) {
              absl::flat_hash_set<VirtualClusterID> virtual_cluster_id_set;
              for (auto &virtual_cluster_info : virtual_cluster_info_list) {
                auto virtual_cluster_id =
                    VirtualClusterID::FromBinary(virtual_cluster_info.id());
                updated_subscribe(virtual_cluster_id, std::move(virtual_cluster_info));
                virtual_cluster_id_set.emplace(virtual_cluster_id);
              }
              if (virtual_clusters_.size() > virtual_cluster_id_set.size()) {
                for (auto iter = virtual_clusters_.begin();
                     iter != virtual_clusters_.end();) {
                  auto curr_iter = iter++;
                  // If there is any virtual cluster not in `virtual_cluster_id_set`, it
                  // means the local node may miss the pub messages (when gcs removed
                  // virtual clusters) in the past. So we have to explicitely notify the
                  // subscriber to clean its local cache.
                  if (!virtual_cluster_id_set.contains(curr_iter->first)) {
                    auto virtual_cluster_data = std::move(curr_iter->second);
                    virtual_cluster_data.set_is_removed(true);
                    subscribe(curr_iter->first, std::move(virtual_cluster_data));
                    virtual_clusters_.erase(curr_iter);
                  }
                }
              }
              if (done) {
                done(status);
              }
            };
        RAY_CHECK_OK(AsyncGetAll(
            /*include_job_clusters=*/true,
            /*only_include_indivisible_clusters=*/true,
            callback));
      };
  subscribe_operation_ = [this, updated_subscribe](const StatusCallback &done) {
    return client_impl_->GetGcsSubscriber().SubscribeAllVirtualClusters(updated_subscribe,
                                                                        done);
  };
  return subscribe_operation_(
      [this, done](const Status &status) { fetch_all_data_operation_(done); });
}

void VirtualClusterInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for virtual cluster info.";
  auto fetch_all_done = [](const Status &status) {
    RAY_LOG(INFO)
        << "Finished fetching all virtual cluster information from gcs server after gcs "
           "server or pub-sub server is restarted.";
  };

  if (subscribe_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_operation_([this, fetch_all_done](const Status &status) {
      fetch_all_data_operation_(fetch_all_done);
    }));
  }
}

}  // namespace gcs
}  // namespace ray
