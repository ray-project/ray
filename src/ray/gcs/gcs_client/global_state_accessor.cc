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

#include <boost/algorithm/string.hpp>

#include "global_state_accessor.h"

namespace ray {
namespace gcs {

GlobalStateAccessor::GlobalStateAccessor(const std::string &redis_address,
                                         const std::string &redis_password,
                                         bool is_test) {
  RAY_LOG(INFO) << "Redis server address = " << redis_address
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
    std::unique_ptr<boost::asio::io_service::work> work(
        new boost::asio::io_service::work(*io_service_));
    promise.set_value(true);
    io_service_->run();
  }));
  promise.get_future().get();
}

GlobalStateAccessor::~GlobalStateAccessor() {
  Disconnect();
  io_service_->stop();
  thread_io_service_->join();
}

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
    gcs_client_->Disconnect();
    is_connected_ = false;
  }
}

std::vector<std::string> GlobalStateAccessor::GetAllJobInfo() {
  std::vector<std::string> job_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Jobs().AsyncGetAll(
      TransformForAccessorCallback<rpc::JobTableData>(job_table_data, promise)));
  promise.get_future().get();
  return job_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllNodeInfo() {
  std::vector<std::string> node_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(
      TransformForAccessorCallback<rpc::GcsNodeInfo>(node_table_data, promise)));
  promise.get_future().get();
  return node_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllProfileInfo() {
  std::vector<std::string> profile_table_data;
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Stats().AsyncGetAll(
      TransformForAccessorCallback<rpc::ProfileTableData>(profile_table_data, promise)));
  promise.get_future().get();
  return profile_table_data;
}

std::vector<std::string> GlobalStateAccessor::GetAllObjectInfo() {
  std::vector<std::string> all_object_info;
  std::promise<bool> promise;
  auto on_done = [&all_object_info, &promise](
                     const Status &status,
                     const std::vector<rpc::ObjectLocationInfo> &result) {
    RAY_CHECK_OK(status);
    for (auto &data : result) {
      all_object_info.push_back(data.SerializeAsString());
    }
    promise.set_value(true);
  };
  RAY_CHECK_OK(gcs_client_->Objects().AsyncGetAll(on_done));
  promise.get_future().get();
  return all_object_info;
}

std::unique_ptr<std::string> GlobalStateAccessor::GetObjectInfo(
    const ObjectID &object_id) {
  std::unique_ptr<std::string> object_info;
  std::promise<bool> promise;
  auto on_done = [object_id, &object_info, &promise](
                     const Status &status,
                     const std::vector<rpc::ObjectTableData> &result) {
    RAY_CHECK_OK(status);
    if (!result.empty()) {
      rpc::ObjectLocationInfo object_location_info;
      object_location_info.set_object_id(object_id.Binary());
      for (auto &data : result) {
        object_location_info.add_locations()->CopyFrom(data);
      }
      object_info.reset(new std::string(object_location_info.SerializeAsString()));
    }
    promise.set_value(true);
  };
  RAY_CHECK_OK(gcs_client_->Objects().AsyncGetLocations(object_id, on_done));
  promise.get_future().get();
  return object_info;
}

}  // namespace gcs
}  // namespace ray
