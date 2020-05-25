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

#ifndef RAY_GCS_GLOBAL_STATE_ACCESSOR_H
#define RAY_GCS_GLOBAL_STATE_ACCESSOR_H

#include "ray/rpc/server_call.h"
#include "service_based_gcs_client.h"

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
  /// \param is_test Whether this accessor is used for tests.
  explicit GlobalStateAccessor(const std::string &redis_address,
                               const std::string &redis_password, bool is_test = false);

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

 private:
  /// MultiItem tranformation helper in template style.
  ///
  /// \return MultiItemCallback within in rpc type DATA.
  template <class DATA>
  MultiItemCallback<DATA> TransformForAccessorCallback(std::vector<std::string> &data_vec,
                                                       std::promise<bool> &promise) {
    return [&data_vec, &promise](const Status &status, const std::vector<DATA> &result) {
      RAY_CHECK_OK(status);
      std::transform(result.begin(), result.end(), std::back_inserter(data_vec),
                     [](const DATA &data) { return data.SerializeAsString(); });
      promise.set_value(true);
    };
  }

 private:
  /// Whether this client is connected to gcs server.
  bool is_connected_{false};

  std::unique_ptr<ServiceBasedGcsClient> gcs_client_;

  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<boost::asio::io_service> io_service_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GLOBAL_STATE_ACCESSOR_H
