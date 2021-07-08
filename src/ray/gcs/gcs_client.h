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

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/accessor.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class GcsClientOptions
/// GCS client's options (configuration items), such as service address, and service
/// password.
class GcsClientOptions {
 public:
  /// Constructor of GcsClientOptions.
  ///
  /// \param ip GCS service ip.
  /// \param port GCS service port.
  /// \param password GCS service password.
  GcsClientOptions(const std::string &ip, int port, const std::string &password,
                   bool enable_sync_conn = true, bool enable_async_conn = true,
                   bool enable_subscribe_conn = true)
      : server_ip_(ip),
        server_port_(port),
        password_(password),
        enable_sync_conn_(enable_sync_conn),
        enable_async_conn_(enable_async_conn),
        enable_subscribe_conn_(enable_subscribe_conn) {}

  GcsClientOptions() {}

  // GCS server address
  std::string server_ip_;
  int server_port_;

  // Password of GCS server.
  std::string password_;

  // Whether to enable connection for contexts.
  bool enable_sync_conn_{true};
  bool enable_async_conn_{true};
  bool enable_subscribe_conn_{true};
};

/// \class GcsClient
/// Abstract interface of the GCS client.
///
/// To read and write from the GCS, `Connect()` must be called and return Status::OK.
/// Before exit, `Disconnect()` must be called.
class GcsClient : public std::enable_shared_from_this<GcsClient> {
 public:
  virtual ~GcsClient() {}

  /// Connect to GCS Service. Non-thread safe.
  /// This function must be called before calling other functions.
  ///
  /// \return Status
  virtual Status Connect(instrumented_io_context &io_service) = 0;

  /// Disconnect with GCS Service. Non-thread safe.
  virtual void Disconnect() = 0;

  virtual std::pair<std::string, int> GetGcsServerAddress() {
    return std::make_pair("", 0);
  }

  /// Return client information for debug.
  virtual std::string DebugString() const { return ""; }

  /// Get the sub-interface for accessing actor information in GCS.
  /// This function is thread safe.
  ActorInfoAccessor &Actors() {
    RAY_CHECK(actor_accessor_ != nullptr);
    return *actor_accessor_;
  }

  /// Get the sub-interface for accessing job information in GCS.
  /// This function is thread safe.
  JobInfoAccessor &Jobs() {
    RAY_CHECK(job_accessor_ != nullptr);
    return *job_accessor_;
  }

  /// Get the sub-interface for accessing object information in GCS.
  /// This function is thread safe.
  ObjectInfoAccessor &Objects() {
    RAY_CHECK(object_accessor_ != nullptr);
    return *object_accessor_;
  }

  /// Get the sub-interface for accessing node information in GCS.
  /// This function is thread safe.
  NodeInfoAccessor &Nodes() {
    RAY_CHECK(node_accessor_ != nullptr);
    return *node_accessor_;
  }

  /// Get the sub-interface for accessing node resource information in GCS.
  /// This function is thread safe.
  NodeResourceInfoAccessor &NodeResources() {
    RAY_CHECK(node_resource_accessor_ != nullptr);
    return *node_resource_accessor_;
  }

  /// Get the sub-interface for accessing task information in GCS.
  /// This function is thread safe.
  TaskInfoAccessor &Tasks() {
    RAY_CHECK(task_accessor_ != nullptr);
    return *task_accessor_;
  }

  /// Get the sub-interface for accessing error information in GCS.
  /// This function is thread safe.
  ErrorInfoAccessor &Errors() {
    RAY_CHECK(error_accessor_ != nullptr);
    return *error_accessor_;
  }

  /// Get the sub-interface for accessing stats information in GCS.
  /// This function is thread safe.
  StatsInfoAccessor &Stats() {
    RAY_CHECK(stats_accessor_ != nullptr);
    return *stats_accessor_;
  }

  /// Get the sub-interface for accessing worker information in GCS.
  /// This function is thread safe.
  WorkerInfoAccessor &Workers() {
    RAY_CHECK(worker_accessor_ != nullptr);
    return *worker_accessor_;
  }

  /// Get the sub-interface for accessing worker information in GCS.
  /// This function is thread safe.
  PlacementGroupInfoAccessor &PlacementGroups() {
    RAY_CHECK(placement_group_accessor_ != nullptr);
    return *placement_group_accessor_;
  }

  /// Get the sub-interface for accessing worker information in GCS.
  /// This function is thread safe.
  InternalKVAccessor &InternalKV() { return *internal_kv_accessor_; }

 protected:
  /// Constructor of GcsClient.
  ///
  /// \param options Options for client.
  GcsClient(const GcsClientOptions &options) : options_(options) {}

  GcsClientOptions options_;

  /// Whether this client is connected to GCS.
  bool is_connected_{false};

  std::unique_ptr<ActorInfoAccessor> actor_accessor_;
  std::unique_ptr<JobInfoAccessor> job_accessor_;
  std::unique_ptr<ObjectInfoAccessor> object_accessor_;
  std::unique_ptr<NodeInfoAccessor> node_accessor_;
  std::unique_ptr<NodeResourceInfoAccessor> node_resource_accessor_;
  std::unique_ptr<TaskInfoAccessor> task_accessor_;
  std::unique_ptr<ErrorInfoAccessor> error_accessor_;
  std::unique_ptr<StatsInfoAccessor> stats_accessor_;
  std::unique_ptr<WorkerInfoAccessor> worker_accessor_;
  std::unique_ptr<PlacementGroupInfoAccessor> placement_group_accessor_;
  std::unique_ptr<InternalKVAccessor> internal_kv_accessor_;
};

}  // namespace gcs

}  // namespace ray
