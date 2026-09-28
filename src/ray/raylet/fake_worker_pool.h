// Copyright The Ray Authors.
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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/raylet/worker_pool.h"

namespace ray::raylet {

// Hand-written fake for WorkerPoolInterface. Methods default to trivial
// return values; public fields let tests program return values and inspect
// recorded calls.
class FakeWorkerPool : public WorkerPoolInterface {
 public:
  // --- WorkerPoolInterface ---

  void PopWorker(const LeaseSpecification &lease_spec,
                 const PopWorkerCallback &callback) override {
    pop_worker_calls++;
    if (pop_worker_hook) {
      pop_worker_hook(lease_spec, callback);
    }
  }

  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    pushed_workers.push_back(worker);
  }

  std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredWorkers(
      bool filter_dead_workers, bool filter_io_workers) const override {
    return registered_workers;
  }

  bool AllAliveWorkersAreActors() const override { return all_alive_workers_are_actors; }

  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const WorkerID &worker_id) const override {
    return registered_worker_by_id;
  }

  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const std::shared_ptr<ClientConnection> &connection) const override {
    return registered_worker_by_connection;
  }

  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const WorkerID &worker_id) const override {
    return registered_driver_by_id;
  }

  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const std::shared_ptr<ClientConnection> &connection) const override {
    return registered_driver_by_connection;
  }

  void HandleJobStarted(const JobID &job_id, const rpc::JobConfig &job_config) override {
    started_jobs.push_back(job_id);
  }

  void HandleJobFinished(const JobID &job_id) override {
    finished_jobs.push_back(job_id);
  }

  void Start() override { start_calls++; }

  void SetNodeManagerPort(int port) override {
    node_manager_port = port;
    set_node_manager_port_calls++;
  }

  void SetRuntimeEnvAgentClient(
      std::unique_ptr<RuntimeEnvAgentClient> runtime_env_agent_client) override {
    set_runtime_env_agent_client_calls++;
  }

  std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredDrivers(
      bool filter_dead_drivers, bool filter_system_drivers) const override {
    return registered_drivers;
  }

  Status RegisterDriver(const std::shared_ptr<WorkerInterface> &worker,
                        const rpc::JobConfig &job_config,
                        std::function<void(Status, int)> send_reply_callback) override {
    registered_driver_calls.push_back(worker);
    return register_driver_status;
  }

  Status RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                        pid_t pid,
                        std::function<void(Status, int)> send_reply_callback) override {
    registered_worker_calls.push_back(worker);
    return register_worker_status;
  }

  boost::optional<const rpc::JobConfig &> GetJobConfig(
      const JobID &job_id) const override {
    return boost::none;
  }

  void OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) override {
    on_worker_started_calls.push_back(worker);
  }

  void DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                        rpc::WorkerExitType disconnect_type) override {
    disconnected_workers.push_back(worker);
  }

  void DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) override {
    disconnected_drivers.push_back(driver);
  }

  void PrestartWorkers(const LeaseSpecification &lease_spec,
                       int64_t backlog_size) override {
    prestart_workers_calls++;
  }

  void StartNewWorker(
      const std::shared_ptr<PopWorkerRequest> &pop_worker_request) override {
    start_new_worker_calls.push_back(pop_worker_request);
  }

  std::string DebugString() const override { return "FakeWorkerPool"; }

  // --- IOWorkerPoolInterface ---

  void PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    pushed_spill_workers.push_back(worker);
  }

  void PopSpillWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    if (pop_spill_worker_hook) {
      pop_spill_worker_hook(std::move(callback));
    }
  }

  void PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    pushed_restore_workers.push_back(worker);
  }

  void PopRestoreWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    if (pop_restore_worker_hook) {
      pop_restore_worker_hook(std::move(callback));
    }
  }

  void PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    pushed_delete_workers.push_back(worker);
  }

  void PopDeleteWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    if (pop_delete_worker_hook) {
      pop_delete_worker_hook(std::move(callback));
    }
  }

  // --- Programmable state / recorded calls. ---

  // Behavior-injection hooks.
  std::function<void(const LeaseSpecification &, const PopWorkerCallback &)>
      pop_worker_hook;
  std::function<void(std::function<void(std::shared_ptr<WorkerInterface>)>)>
      pop_spill_worker_hook;
  std::function<void(std::function<void(std::shared_ptr<WorkerInterface>)>)>
      pop_restore_worker_hook;
  std::function<void(std::function<void(std::shared_ptr<WorkerInterface>)>)>
      pop_delete_worker_hook;

  // Settable return values.
  std::vector<std::shared_ptr<WorkerInterface>> registered_workers;
  std::vector<std::shared_ptr<WorkerInterface>> registered_drivers;
  std::shared_ptr<WorkerInterface> registered_worker_by_id;
  std::shared_ptr<WorkerInterface> registered_worker_by_connection;
  std::shared_ptr<WorkerInterface> registered_driver_by_id;
  std::shared_ptr<WorkerInterface> registered_driver_by_connection;
  bool all_alive_workers_are_actors = false;
  Status register_driver_status = Status::OK();
  Status register_worker_status = Status::OK();
  int node_manager_port = 0;

  // Recorded calls.
  int pop_worker_calls = 0;
  int start_calls = 0;
  int set_node_manager_port_calls = 0;
  int set_runtime_env_agent_client_calls = 0;
  int prestart_workers_calls = 0;
  std::vector<std::shared_ptr<WorkerInterface>> pushed_workers;
  std::vector<std::shared_ptr<WorkerInterface>> pushed_spill_workers;
  std::vector<std::shared_ptr<WorkerInterface>> pushed_restore_workers;
  std::vector<std::shared_ptr<WorkerInterface>> pushed_delete_workers;
  std::vector<std::shared_ptr<WorkerInterface>> registered_worker_calls;
  std::vector<std::shared_ptr<WorkerInterface>> registered_driver_calls;
  std::vector<std::shared_ptr<WorkerInterface>> on_worker_started_calls;
  std::vector<std::shared_ptr<WorkerInterface>> disconnected_workers;
  std::vector<std::shared_ptr<WorkerInterface>> disconnected_drivers;
  std::vector<std::shared_ptr<PopWorkerRequest>> start_new_worker_calls;
  std::vector<JobID> started_jobs;
  std::vector<JobID> finished_jobs;
};

}  // namespace ray::raylet
