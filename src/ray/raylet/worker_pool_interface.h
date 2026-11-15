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

#include <boost/optional.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/time/time.h"
#include "ray/common/id.h"
#include "ray/common/lease/lease.h"
#include "ray/common/status.h"
#include "ray/raylet/runtime_env_agent_client.h"
#include "ray/raylet/worker_interface.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace raylet {

enum PopWorkerStatus {
  // OK.
  // A registered worker will be returned with callback.
  OK = 0,
  // Job config is not found.
  // A nullptr worker will be returned with callback.
  JobConfigMissing = 1,
  // Worker process startup rate is limited.
  // A nullptr worker will be returned with callback.
  TooManyStartingWorkerProcesses = 2,
  // Worker process has been started, but the worker did not register at the raylet within
  // the timeout.
  // A nullptr worker will be returned with callback.
  WorkerPendingRegistration = 3,
  // Any fails of runtime env creation.
  // A nullptr worker will be returned with callback.
  RuntimeEnvCreationFailed = 4,
  // The lease's job has finished.
  // A nullptr worker will be returned with callback.
  JobFinished = 5,
};

/// \param[in] worker The started worker instance. Nullptr if worker is not started.
/// \param[in] status The pop worker status. OK if things go well. Otherwise, it will
/// contain the error status.
/// \param[in] runtime_env_setup_error_message The error message
/// when runtime env setup is failed. This should be empty unless status ==
/// RuntimeEnvCreationFailed.
/// \return true if the worker was used. Otherwise, return false
/// and the worker will be returned to the worker pool.
using PopWorkerCallback =
    std::function<bool(const std::shared_ptr<WorkerInterface> &worker,
                       PopWorkerStatus status,
                       const std::string &runtime_env_setup_error_message)>;

struct PopWorkerRequest {
  const rpc::Language language_;
  const rpc::WorkerType worker_type_;
  const JobID job_id_;                    // can be Nil
  const ActorID root_detached_actor_id_;  // can be Nil
  const std::optional<bool> is_gpu_;
  const std::optional<bool> is_actor_worker_;
  const rpc::RuntimeEnvInfo runtime_env_info_;
  const int runtime_env_hash_;
  const std::vector<std::string> dynamic_options_;
  std::optional<absl::Duration> worker_startup_keep_alive_duration_;

  PopWorkerCallback callback_;

  PopWorkerRequest(rpc::Language lang,
                   rpc::WorkerType worker_type,
                   JobID job,
                   ActorID root_actor_id,
                   std::optional<bool> gpu,
                   std::optional<bool> actor_worker,
                   rpc::RuntimeEnvInfo runtime_env_info,
                   int runtime_env_hash,
                   std::vector<std::string> options,
                   std::optional<absl::Duration> worker_startup_keep_alive_duration,
                   PopWorkerCallback callback)
      : language_(lang),
        worker_type_(worker_type),
        job_id_(job),
        root_detached_actor_id_(root_actor_id),
        is_gpu_(gpu),
        is_actor_worker_(actor_worker),
        runtime_env_info_(std::move(runtime_env_info)),
        runtime_env_hash_(runtime_env_hash),
        dynamic_options_(std::move(options)),
        worker_startup_keep_alive_duration_(worker_startup_keep_alive_duration),
        callback_(std::move(callback)) {}
};

/// \class IOWorkerPoolInterface
///
/// Used for object spilling manager unit tests.
class IOWorkerPoolInterface {
 public:
  virtual void PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual void PopSpillWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) = 0;

  virtual void PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual void PopRestoreWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) = 0;

  virtual void PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual void PopDeleteWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) = 0;

  virtual ~IOWorkerPoolInterface() = default;
};

/// \class WorkerPoolInterface
///
/// Used for new scheduler unit tests.
class WorkerPoolInterface : public IOWorkerPoolInterface {
 public:
  /// Pop an idle worker from the pool. The caller is responsible for pushing
  /// the worker back onto the pool once the worker has completed its work.
  ///
  /// \param lease_spec The returned worker must be able to execute this lease.
  /// \param callback The callback function that executed when gets the result of
  /// worker popping.
  /// The callback will be executed with an empty worker in following cases:
  /// Case 1: Job config not found.
  /// Case 2: Worker process startup rate limited.
  /// Case 3: Worker process has been started, but the worker registered back to raylet
  /// timeout.
  //  Case 4: Any fails of runtime env creation.
  /// The callback will also be executed when a valid worker is found in the following
  /// cases:
  /// Case 1: A suitable worker was found in idle worker pool.
  /// Case 2: A suitable worker registered with the raylet.
  /// The corresponding PopWorkerStatus will be passed to the callback.
  virtual void PopWorker(const LeaseSpecification &lease_spec,
                         const PopWorkerCallback &callback) = 0;
  /// Add an idle worker to the pool.
  ///
  /// \param The idle worker to add.
  virtual void PushWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  /// Get all the registered workers.
  ///
  /// \param filter_dead_workers whether or not if this method will filter dead workers
  /// \param filter_io_workers whether or not if this method will filter io workers
  /// non-retriable workers that are still registered.
  ///
  /// \return A list containing all the workers.
  virtual std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredWorkers(
      bool filter_dead_workers = false, bool filter_io_workers = false) const = 0;

  /// Returns true if this node's workers are solely actors.
  virtual bool AllAliveWorkersAreActors() const = 0;

  /// Get registered worker process by id or nullptr if not found.
  virtual std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const WorkerID &worker_id) const = 0;

  virtual std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const std::shared_ptr<ClientConnection> &connection) const = 0;

  /// Get registered driver process by id or nullptr if not found.
  virtual std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const WorkerID &worker_id) const = 0;

  virtual std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const std::shared_ptr<ClientConnection> &connection) const = 0;

  virtual ~WorkerPoolInterface() = default;

  virtual void HandleJobStarted(const JobID &job_id,
                                const rpc::JobConfig &job_config) = 0;

  virtual void HandleJobFinished(const JobID &job_id) = 0;

  virtual void Start() = 0;

  virtual void SetNodeManagerPort(int node_manager_port) = 0;

  virtual void SetRuntimeEnvAgentClient(
      std::unique_ptr<RuntimeEnvAgentClient> runtime_env_agent_client) = 0;

  virtual std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredDrivers(
      bool filter_dead_drivers = false, bool filter_system_drivers = false) const = 0;

  virtual Status RegisterDriver(const std::shared_ptr<WorkerInterface> &worker,
                                const rpc::JobConfig &job_config,
                                std::function<void(Status, int)> send_reply_callback) = 0;

  virtual Status RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                                pid_t pid,
                                std::function<void(Status, int)> send_reply_callback) = 0;

  virtual boost::optional<const rpc::JobConfig &> GetJobConfig(
      const JobID &job_id) const = 0;

  virtual void OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual void DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                                rpc::WorkerExitType disconnect_type) = 0;

  virtual void DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) = 0;

  virtual void PrestartWorkers(const LeaseSpecification &lease_spec,
                               int64_t backlog_size) = 0;

  virtual void StartNewWorker(
      const std::shared_ptr<PopWorkerRequest> &pop_worker_request) = 0;

  virtual std::string DebugString() const = 0;
};

}  // namespace raylet
}  // namespace ray
