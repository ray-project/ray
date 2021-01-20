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

namespace ray {
namespace gcs {

GlobalStateAccessor::GlobalStateAccessor(const std::string &redis_address,
                                         const std::string &redis_password,
                                         bool is_test) {
  RAY_LOG(DEBUG) << "Redis server address = " << redis_address
                 << ", is test flag = " << is_test;
  std::vector<std::string> address;
  boost::split(address, redis_address, boost::is_any_of(":"));
  RAY_CHECK(address.size() == 2);
  GcsClientOptions options;
  options.server_ip_ = address[0];
  options.server_port_ = std::stoi(address[1]);
  options.password_ = redis_password;
  options.is_test_client_ = is_test;
  gcs_client_.reset(new ServiceBasedGcsClient(options));

  io_service_.reset(new boost::asio::io_service());

  std::promise<bool> promise;
  thread_io_service_.reset(new std::thread([this, &promise] {
    SetThreadName("global.accessor");
    std::unique_ptr<boost::asio::io_service::work> work(
        new boost::asio::io_service::work(*io_service_));
    promise.set_value(true);
    io_service_->run();
  }));
  promise.get_future().get();
}

GlobalStateAccessor::~GlobalStateAccessor() { Disconnect(); }

bool GlobalStateAccessor::Connect() {
  if (!is_connected_) {
    is_connected_ = true;
    return gcs_client_->Connect(*io_service_).ok();
  } else {
    RAY_LOG(DEBUG) << "Duplicated connection for GlobalStateAccessor.";
    return true;
  }
}

void GlobalStateAccessor::Disconnect() {
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
  RAY_CHECK_OK(gcs_client_->Jobs().AsyncGetAll(
      TransformForMultiItemCallback<rpc::JobTableData>(job_table_data, promise)));
  promise.get_future().get();
  return job_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllNodeInfo() {
  std::vector<std::string> node_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(
      TransformForMultiItemCallback<rpc::GcsNodeInfo>(node_table_data, promise)));
  promise.get_future().get();
  return node_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllProfileInfo() {
  std::vector<std::string> profile_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Stats().AsyncGetAll(
      TransformForMultiItemCallback<rpc::ProfileTableData>(profile_table_data, promise)));
  promise.get_future().get();
  return profile_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllObjectInfo() {
  std::vector<std::string> object_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Objects().AsyncGetAll(
      TransformForMultiItemCallback<rpc::ObjectLocationInfo>(object_table_data,
                                                             promise)));
  promise.get_future().get();
  return object_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetObjectInfo(
    const ObjectID &object_id) {
  std::unique_ptr<std::string> object_info;
  std::promise<bool> promise;
  auto on_done = [&object_info, &promise](
                     const Status &status,
                     const boost::optional<rpc::ObjectLocationInfo> &result) {
    RAY_CHECK_OK(status);
    if (result) {
      object_info.reset(new std::string(result->SerializeAsString()));
    }
    promise.set_value(true);
  };
  RAY_CHECK_OK(gcs_client_->Objects().AsyncGetLocations(object_id, on_done));
  promise.get_future().get();
  return object_info;
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
  RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetResources(node_id, on_done));
  promise.get_future().get();
  return node_resource_map.SerializeAsString();
}

std::vector<std::string> GlobalStateAccessor::GetAllAvailableResources() {
  std::vector<std::string> available_resources;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetAllAvailableResources(
      TransformForMultiItemCallback<rpc::AvailableResources>(available_resources,
                                                             promise)));
  promise.get_future().get();
  return available_resources;
}

std::string GlobalStateAccessor::GetInternalConfig() {
  rpc::StoredConfig config_proto;
  std::promise<void> promise;
  auto on_done = [&config_proto, &promise](
                     Status status,
                     const boost::optional<std::unordered_map<std::string, std::string>>
                         stored_raylet_config) {
    RAY_CHECK_OK(status);
    if (stored_raylet_config.has_value()) {
      config_proto.mutable_config()->insert(stored_raylet_config->begin(),
                                            stored_raylet_config->end());
    }
    promise.set_value();
  };

  RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetInternalConfig(on_done));
  promise.get_future().get();

  return config_proto.SerializeAsString();
}

std::unique_ptr<std::string> GlobalStateAccessor::GetAllResourceUsage() {
  std::unique_ptr<std::string> resource_batch_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetAllResourceUsage(
      TransformForItemCallback<rpc::ResourceUsageBatchData>(resource_batch_data,
                                                            promise)));
  promise.get_future().get();
  return resource_batch_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllActorInfo() {
  std::vector<std::string> actor_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Actors().AsyncGetAll(
      TransformForMultiItemCallback<rpc::ActorTableData>(actor_table_data, promise)));
  promise.get_future().get();
  return actor_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetActorInfo(const ActorID &actor_id) {
  std::unique_ptr<std::string> actor_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Actors().AsyncGet(
      actor_id,
      TransformForOptionalItemCallback<rpc::ActorTableData>(actor_table_data, promise)));
  promise.get_future().get();
  return actor_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetWorkerInfo(
    const WorkerID &worker_id) {
  std::unique_ptr<std::string> worker_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Workers().AsyncGet(
      worker_id, TransformForOptionalItemCallback<rpc::WorkerTableData>(worker_table_data,
                                                                        promise)));
  promise.get_future().get();
  return worker_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllWorkerInfo() {
  std::vector<std::string> worker_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Workers().AsyncGetAll(
      TransformForMultiItemCallback<rpc::WorkerTableData>(worker_table_data, promise)));
  promise.get_future().get();
  return worker_table_data;
}

bool GlobalStateAccessor::AddWorkerInfo(const std::string &serialized_string) {
  auto data_ptr = std::make_shared<rpc::WorkerTableData>();
  data_ptr->ParseFromString(serialized_string);
  std::promise<bool> promise;
  RAY_CHECK_OK(
      gcs_client_->Workers().AsyncAdd(data_ptr, [&promise](const Status &status) {
        RAY_CHECK_OK(status);
        promise.set_value(true);
      }));
  promise.get_future().get();
  return true;
}

std::vector<std::string> GlobalStateAccessor::GetAllPlacementGroupInfo() {
  std::vector<std::string> placement_group_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->PlacementGroups().AsyncGetAll(
      TransformForMultiItemCallback<rpc::PlacementGroupTableData>(
          placement_group_table_data, promise)));
  promise.get_future().get();
  return placement_group_table_data;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetPlacementGroupInfo(
    const PlacementGroupID &placement_group_id) {
  std::unique_ptr<std::string> placement_group_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->PlacementGroups().AsyncGet(
      placement_group_id, TransformForOptionalItemCallback<rpc::PlacementGroupTableData>(
                              placement_group_table_data, promise)));
  promise.get_future().get();
  return placement_group_table_data;
}

}  // namespace gcs
}  // namespace ray
