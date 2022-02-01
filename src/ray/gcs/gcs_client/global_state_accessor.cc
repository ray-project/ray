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

#include "ray/gcs/gcs_client/global_state_accessor.h"

#include <boost/algorithm/string.hpp>

#include "ray/common/asio/instrumented_io_context.h"

namespace ray {
namespace gcs {

GlobalStateAccessor::GlobalStateAccessor(const GcsClientOptions &gcs_client_options) {
  gcs_client_ = std::make_unique<GcsClient>(gcs_client_options);
  io_service_ = std::make_unique<instrumented_io_context>();

  std::promise<bool> promise;
  thread_io_service_ = std::make_unique<std::thread>([this, &promise] {
    SetThreadName("global.accessor");
    std::unique_ptr<boost::asio::io_service::work> work(
        new boost::asio::io_service::work(*io_service_));
    promise.set_value(true);
    io_service_->run();
  });
  promise.get_future().get();
}

GlobalStateAccessor::~GlobalStateAccessor() { Disconnect(); }

bool GlobalStateAccessor::Connect() {
  absl::WriterMutexLock lock(&mutex_);
  if (!is_connected_) {
    is_connected_ = true;
    return gcs_client_->Connect(*io_service_).ok();
  }
  RAY_LOG(DEBUG) << "Duplicated connection for GlobalStateAccessor.";
  return true;
}

void GlobalStateAccessor::Disconnect() {
  absl::WriterMutexLock lock(&mutex_);
  RAY_LOG(DEBUG) << "Global state accessor disconnect";
  if (is_connected_) {
    io_service_->stop();
    thread_io_service_->join();
    gcs_client_->Disconnect();
    is_connected_ = false;
  }
}

std::vector<std::string> GlobalStateAccessor::GetAllJobInfo() {
  std::vector<std::string> job_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncGetAll(
        TransformForMultiItemCallback<rpc::JobTableData>(job_table_data, promise)));
  }
  promise.get_future().get();
  return job_table_data;
}

JobID GlobalStateAccessor::GetNextJobID() {
  std::promise<JobID> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncGetNextJobID(
        [&promise](const JobID &job_id) { promise.set_value(job_id); }));
  }
  return promise.get_future().get();
}

std::vector<std::string> GlobalStateAccessor::GetAllNodeInfo() {
  std::vector<std::string> node_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(
        TransformForMultiItemCallback<rpc::GcsNodeInfo>(node_table_data, promise)));
  }
  promise.get_future().get();
  return node_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllProfileInfo() {
  std::vector<std::string> profile_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Stats().AsyncGetAll(
        TransformForMultiItemCallback<rpc::ProfileTableData>(profile_table_data,
                                                             promise)));
  }
  promise.get_future().get();
  return profile_table_data;
}

std::string GlobalStateAccessor::GetNodeResourceInfo(const NodeID &node_id) {
  rpc::ResourceMap node_resource_map;
  std::promise<void> promise;
  auto on_done =
      [&node_resource_map, &promise](
          const Status &status,
          const boost::optional<ray::gcs::NodeResourceInfoAccessor::ResourceMap>
              &result) {
        RAY_CHECK_OK(status);
        if (result) {
          auto result_value = result.get();
          for (auto &data : result_value) {
            (*node_resource_map.mutable_items())[data.first] = *data.second;
          }
        }
        promise.set_value();
      };
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetResources(node_id, on_done));
  }
  promise.get_future().get();
  return node_resource_map.SerializeAsString();
}

std::vector<std::string> GlobalStateAccessor::GetAllAvailableResources() {
  std::vector<std::string> available_resources;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetAllAvailableResources(
        TransformForMultiItemCallback<rpc::AvailableResources>(available_resources,
                                                               promise)));
  }
  promise.get_future().get();
  return available_resources;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetAllResourceUsage() {
  std::unique_ptr<std::string> resource_batch_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetAllResourceUsage(
        TransformForItemCallback<rpc::ResourceUsageBatchData>(resource_batch_data,
                                                              promise)));
  }
  promise.get_future().get();
  return resource_batch_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllActorInfo() {
  std::vector<std::string> actor_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Actors().AsyncGetAll(
        TransformForMultiItemCallback<rpc::ActorTableData>(actor_table_data, promise)));
  }
  promise.get_future().get();
  return actor_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetActorInfo(const ActorID &actor_id) {
  std::unique_ptr<std::string> actor_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Actors().AsyncGet(
        actor_id, TransformForOptionalItemCallback<rpc::ActorTableData>(actor_table_data,
                                                                        promise)));
  }
  promise.get_future().get();
  return actor_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetWorkerInfo(
    const WorkerID &worker_id) {
  std::unique_ptr<std::string> worker_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Workers().AsyncGet(
        worker_id, TransformForOptionalItemCallback<rpc::WorkerTableData>(
                       worker_table_data, promise)));
  }
  promise.get_future().get();
  return worker_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllWorkerInfo() {
  std::vector<std::string> worker_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Workers().AsyncGetAll(
        TransformForMultiItemCallback<rpc::WorkerTableData>(worker_table_data, promise)));
  }
  promise.get_future().get();
  return worker_table_data;
}

bool GlobalStateAccessor::AddWorkerInfo(const std::string &serialized_string) {
  auto data_ptr = std::make_shared<rpc::WorkerTableData>();
  data_ptr->ParseFromString(serialized_string);
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(
        gcs_client_->Workers().AsyncAdd(data_ptr, [&promise](const Status &status) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        }));
  }
  promise.get_future().get();
  return true;
}

std::vector<std::string> GlobalStateAccessor::GetAllPlacementGroupInfo() {
  std::vector<std::string> placement_group_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->PlacementGroups().AsyncGetAll(
        TransformForMultiItemCallback<rpc::PlacementGroupTableData>(
            placement_group_table_data, promise)));
  }
  promise.get_future().get();
  return placement_group_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetPlacementGroupInfo(
    const PlacementGroupID &placement_group_id) {
  std::unique_ptr<std::string> placement_group_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->PlacementGroups().AsyncGet(
        placement_group_id,
        TransformForOptionalItemCallback<rpc::PlacementGroupTableData>(
            placement_group_table_data, promise)));
  }
  promise.get_future().get();
  return placement_group_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetPlacementGroupByName(
    const std::string &placement_group_name, const std::string &ray_namespace) {
  std::unique_ptr<std::string> placement_group_table_data;
  std::promise<bool> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->PlacementGroups().AsyncGetByName(
        placement_group_name, ray_namespace,
        TransformForOptionalItemCallback<rpc::PlacementGroupTableData>(
            placement_group_table_data, promise)));
  }
  promise.get_future().get();
  return placement_group_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetInternalKV(const std::string &ns,
                                                                const std::string &key) {
  absl::ReaderMutexLock lock(&mutex_);
  std::string value;

  Status status = gcs_client_->InternalKV().Get(ns, key, value);
  return status.ok() ? std::make_unique<std::string>(value) : nullptr;
}

std::string GlobalStateAccessor::GetSystemConfig() {
  std::promise<std::string> promise;
  {
    absl::ReaderMutexLock lock(&mutex_);
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetInternalConfig(
        [&promise](const Status &status,
                   const boost::optional<std::string> &stored_raylet_config) {
          RAY_CHECK_OK(status);
          promise.set_value(*stored_raylet_config);
        }));
  }
  auto future = promise.get_future();
  if (future.wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    RAY_LOG(FATAL) << "Failed to get system config within the timeout setting.";
  }
  return future.get();
}

ray::Status GlobalStateAccessor::GetNodeToConnectForDriver(
    const std::string &node_ip_address, std::string *node_to_connect) {
  auto start_ms = current_time_ms();
  while (true) {
    std::promise<std::pair<Status, std::vector<rpc::GcsNodeInfo>>> promise;
    {
      absl::ReaderMutexLock lock(&mutex_);
      RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(
          [&promise](Status status, const std::vector<rpc::GcsNodeInfo> &nodes) {
            promise.set_value(
                std::pair<Status, std::vector<rpc::GcsNodeInfo>>(status, nodes));
          }));
    }
    auto result = promise.get_future().get();
    auto status = result.first;
    if (!status.ok()) {
      return status;
    }

    // Deal with alive nodes only
    std::vector<rpc::GcsNodeInfo> nodes;
    std::copy_if(result.second.begin(), result.second.end(), std::back_inserter(nodes),
                 [](const rpc::GcsNodeInfo &node) {
                   return node.state() == rpc::GcsNodeInfo::ALIVE;
                 });

    if (nodes.empty()) {
      status = Status::NotFound("GCS has started but no raylets have registered yet.");
    } else {
      int relevant_client_index = -1;
      int head_node_client_index = -1;
      std::pair<std::string, int> gcs_address;
      {
        absl::WriterMutexLock lock(&mutex_);
        gcs_address = gcs_client_->GetGcsServerAddress();
      }

      for (int i = 0; i < static_cast<int>(nodes.size()); i++) {
        const auto &node = nodes[i];
        std::string ip_address = node.node_manager_address();
        if (ip_address == node_ip_address) {
          relevant_client_index = i;
          break;
        }
        // TODO(kfstorm): Do we need to replace `node_ip_address` with
        // `get_node_ip_address()`?
        if ((ip_address == "127.0.0.1" && gcs_address.first == node_ip_address) ||
            ip_address == gcs_address.first) {
          head_node_client_index = i;
        }
      }

      if (relevant_client_index < 0 && head_node_client_index >= 0) {
        RAY_LOG(INFO) << "This node has an IP address of " << node_ip_address
                      << ", while we can not found the matched Raylet address. "
                      << "This maybe come from when you connect the Ray cluster "
                      << "with a different IP address or connect a container.";
        relevant_client_index = head_node_client_index;
      }
      if (relevant_client_index < 0) {
        std::ostringstream oss;
        oss << "This node has an IP address of " << node_ip_address << ", and Ray "
            << "expects this IP address to be either the GCS address or one of"
            << " the Raylet addresses. Connected to GCS at " << gcs_address.first
            << " and found raylets at ";
        for (size_t i = 0; i < nodes.size(); i++) {
          if (i > 0) {
            oss << ", ";
          }
          oss << nodes[i].node_manager_address();
        }
        oss << " but none of these match this node's IP " << node_ip_address
            << ". Are any of these actually a different IP address for the same node?"
            << "You might need to provide --node-ip-address to specify the IP "
            << "address that the head should use when sending to this node.";
        status = Status::NotFound(oss.str());
      } else {
        *node_to_connect = nodes[relevant_client_index].SerializeAsString();
        return Status::OK();
      }
    }

    if (current_time_ms() - start_ms >=
        RayConfig::instance().raylet_start_wait_time_s() * 1000) {
      return status;
    }
    RAY_LOG(WARNING) << "Some processes that the driver needs to connect to have "
                        "not registered with GCS, so retrying. Have you run "
                        "'ray start' on this node?";
    // Some of the information may not be in GCS yet, so wait a little bit.
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

}  // namespace gcs
}  // namespace ray
