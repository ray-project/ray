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
  fetch_all_data_operation_ = [this, subscribe](const StatusCallback &done) {
    auto callback =
        [subscribe, done](
            const Status &status,
            std::vector<rpc::VirtualClusterTableData> &&virtual_cluster_info_list) {
          for (auto &virtual_cluster_info : virtual_cluster_info_list) {
            subscribe(VirtualClusterID::FromBinary(virtual_cluster_info.id()),
                      std::move(virtual_cluster_info));
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
  subscribe_operation_ = [this, subscribe](const StatusCallback &done) {
    return client_impl_->GetGcsSubscriber().SubscribeAllVirtualClusters(subscribe, done);
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
