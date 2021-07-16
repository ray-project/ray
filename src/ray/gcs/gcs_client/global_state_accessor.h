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

#pragma once

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace gcs {

/// \class GlobalStateAccessor
///
/// `GlobalStateAccessor` is used to provide synchronous interfaces to access data in GCS
/// for the language front-end (e.g., Python's `state.py`).
class GlobalStateAccessor {
 public:
  /// Constructor of GlobalStateAccessor.
  ///
  /// \param redis_address The address of GCS Redis.
  /// \param redis_password The password of GCS Redis.
  explicit GlobalStateAccessor(const std::string &redis_address,
                               const std::string &redis_password);

  ~GlobalStateAccessor();

  /// Connect gcs server.
  ///
  /// \return Whether the connection is successful.
  bool Connect();

  /// Disconnect from gcs server.
  void Disconnect();

  /// Get information of all jobs from GCS Service.
  ///
  /// \return All job info. To support multi-language, we serialize each JobTableData and
  /// return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::vector<std::string> GetAllJobInfo();

  /// Get next job id from GCS Service.
  ///
  /// \return Next job id.
  JobID GetNextJobID();

  /// Get all node information from GCS.
  ///
  /// \return A list of `GcsNodeInfo` objects serialized in protobuf format.
  std::vector<std::string> GetAllNodeInfo();

  /// Get information of all profiles from GCS Service.
  ///
  /// \return All profile info. To support multi-language, we serialized each
  /// ProfileTableData and returned the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllProfileInfo();

  /// Get information of all objects from GCS Service.
  ///
  /// \return All object info. To support multi-language, we serialize each
  /// ObjectTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllObjectInfo();

  /// Get information of an object from GCS Service.
  ///
  /// \param object_id The ID of object to look up in the GCS Service.
  /// \return Object info. To support multi-language, we serialize each ObjectTableData
  /// and return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::unique_ptr<std::string> GetObjectInfo(const ObjectID &object_id);

  /// Get information of a node resource from GCS Service.
  ///
  /// \param node_id The ID of node to look up in the GCS Service.
  /// \return node resource map info. To support multi-language, we serialize each
  /// ResourceTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::string GetNodeResourceInfo(const NodeID &node_id);

  /// Get available resources of all nodes.
  ///
  /// \return available resources of all nodes. To support multi-language, we serialize
  /// each AvailableResources and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllAvailableResources();

  /// Get newest resource usage of all nodes from GCS Service. Only used when light
  /// rerouce usage report enabled.
  ///
  /// \return resource usage info. To support multi-language, we serialize each
  /// data and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::unique_ptr<std::string> GetAllResourceUsage();

  /// Get information of all actors from GCS Service.
  ///
  /// \return All actor info. To support multi-language, we serialize each ActorTableData
  /// and return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::vector<std::string> GetAllActorInfo();

  /// Get information of an actor from GCS Service.
  ///
  /// \param actor_id The ID of actor to look up in the GCS Service.
  /// \return Actor info. To support multi-language, we serialize each ActorTableData and
  /// return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::unique_ptr<std::string> GetActorInfo(const ActorID &actor_id);

  /// Get information of a worker from GCS Service.
  ///
  /// \param worker_id The ID of worker to look up in the GCS Service.
  /// \return Worker info. To support multi-language, we serialize each WorkerTableData
  /// and return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::unique_ptr<std::string> GetWorkerInfo(const WorkerID &worker_id);

  /// Get information of all workers from GCS Service.
  ///
  /// \return All worker info. To support multi-language, we serialize each
  /// WorkerTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllWorkerInfo();

  /// Add information of a worker to GCS Service.
  ///
  /// \param serialized_string The serialized data of worker to be added in the GCS
  /// Service, use string is convenient for python to use.
  /// \return Is operation success.
  bool AddWorkerInfo(const std::string &serialized_string);

  /// Get information of all placement group from GCS Service.
  ///
  /// \return All placement group info. To support multi-language, we serialize each
  /// PlacementGroupTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllPlacementGroupInfo();

  /// Get information of a placement group from GCS Service by ID.
  ///
  /// \param placement_group_id The ID of placement group to look up in the GCS Service.
  /// \return Placement group info. To support multi-language, we serialize each
  /// PlacementGroupTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::unique_ptr<std::string> GetPlacementGroupInfo(
      const PlacementGroupID &placement_group_id);

  /// Get information of a placement group from GCS Service by name.
  ///
  /// \param placement_group_name The name of placement group to look up in the GCS
  /// Service.
  /// \return Placement group info. To support multi-language, we serialize each
  /// PlacementGroupTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::unique_ptr<std::string> GetPlacementGroupByName(
      const std::string &placement_group_name, const std::string &ray_namespace);

  /// Get value of the key from GCS Service.
  ///
  /// \param key key to get.
  /// \return Value of the key.
  std::unique_ptr<std::string> GetInternalKV(const std::string &key);

  /// Get the serialized system config from GCS.
  ///
  /// \return The serialized system config.
  std::string GetSystemConfig();

  /// Get the node to connect for a Ray driver.
  ///
  /// \param[in] node_ip_address The IP address of the desired node to connect.
  /// \param[out] node_to_connect The info of the node to connect. To support
  /// multi-language, we serialize each GcsNodeInfo and return the serialized string.
  /// Where used, it needs to be deserialized with protobuf function.
  ray::Status GetNodeToConnectForDriver(const std::string &node_ip_address,
                                        std::string *node_to_connect);

 private:
  /// MultiItem transformation helper in template style.
  ///
  /// \return MultiItemCallback within in rpc type DATA.
  template <class DATA>
  MultiItemCallback<DATA> TransformForMultiItemCallback(
      std::vector<std::string> &data_vec, std::promise<bool> &promise) {
    return [&data_vec, &promise](const Status &status, const std::vector<DATA> &result) {
      RAY_CHECK_OK(status);
      std::transform(result.begin(), result.end(), std::back_inserter(data_vec),
                     [](const DATA &data) { return data.SerializeAsString(); });
      promise.set_value(true);
    };
  }

  /// OptionalItem transformation helper in template style.
  ///
  /// \return OptionalItemCallback within in rpc type DATA.
  template <class DATA>
  OptionalItemCallback<DATA> TransformForOptionalItemCallback(
      std::unique_ptr<std::string> &data, std::promise<bool> &promise) {
    return [&data, &promise](const Status &status, const boost::optional<DATA> &result) {
      RAY_CHECK_OK(status);
      if (result) {
        data.reset(new std::string(result->SerializeAsString()));
      }
      promise.set_value(true);
    };
  }

  /// Item transformation helper in template style.
  ///
  /// \return ItemCallback within in rpc type DATA.
  template <class DATA>
  ItemCallback<DATA> TransformForItemCallback(std::unique_ptr<std::string> &data,
                                              std::promise<bool> &promise) {
    return [&data, &promise](const DATA &result) {
      data.reset(new std::string(result.SerializeAsString()));
      promise.set_value(true);
    };
  }

  std::string redis_address_;
  std::string redis_ip_address_;

  /// Whether this client is connected to gcs server.
  bool is_connected_{false};

  std::unique_ptr<ServiceBasedGcsClient> gcs_client_;

  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<instrumented_io_context> io_service_;
};

}  // namespace gcs
}  // namespace ray
