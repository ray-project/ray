// Copyright 2025 The Ray Authors.
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

#include "gmock/gmock.h"
#include "ray/raylet/worker_pool.h"

namespace ray::raylet {
class MockWorkerPool : public WorkerPoolInterface {
 public:
  MOCK_METHOD(void,
              PopWorker,
              (const LeaseSpecification &lease_spec, const PopWorkerCallback &callback),
              (override));
  MOCK_METHOD(void,
              PushWorker,
              (const std::shared_ptr<WorkerInterface> &worker),
              (override));
  MOCK_METHOD((std::vector<std::shared_ptr<WorkerInterface>>),
              GetAllRegisteredWorkers,
              (bool filter_dead_workers, bool filter_io_workers),
              (const, override));
  MOCK_METHOD(bool, IsWorkerAvailableForScheduling, (), (const, override));
  MOCK_METHOD(std::shared_ptr<WorkerInterface>,
              GetRegisteredWorker,
              (const WorkerID &worker_id),
              (const, override));
  MOCK_METHOD(std::shared_ptr<WorkerInterface>,
              GetRegisteredWorker,
              (const std::shared_ptr<ClientConnection> &connection),
              (const, override));
  MOCK_METHOD(std::shared_ptr<WorkerInterface>,
              GetRegisteredDriver,
              (const WorkerID &worker_id),
              (const, override));
  MOCK_METHOD(std::shared_ptr<WorkerInterface>,
              GetRegisteredDriver,
              (const std::shared_ptr<ClientConnection> &connection),
              (const, override));
  MOCK_METHOD(void,
              HandleJobStarted,
              (const JobID &job_id, const rpc::JobConfig &job_config),
              (override));
  MOCK_METHOD(void, HandleJobFinished, (const JobID &job_id), (override));
  MOCK_METHOD(void, Start, (), (override));
  MOCK_METHOD(void, SetNodeManagerPort, (int node_manager_port), (override));
  MOCK_METHOD(void,
              SetRuntimeEnvAgentClient,
              (std::unique_ptr<RuntimeEnvAgentClient> runtime_env_agent_client),
              (override));
  MOCK_METHOD((std::vector<std::shared_ptr<WorkerInterface>>),
              GetAllRegisteredDrivers,
              (bool filter_dead_drivers),
              (const, override));
  MOCK_METHOD(Status,
              RegisterDriver,
              (const std::shared_ptr<WorkerInterface> &worker,
               const rpc::JobConfig &job_config,
               std::function<void(Status, int)> send_reply_callback),
              (override));
  MOCK_METHOD(Status,
              RegisterWorker,
              (const std::shared_ptr<WorkerInterface> &worker,
               pid_t pid,
               StartupToken worker_startup_token,
               std::function<void(Status, int)> send_reply_callback),
              (override));
  MOCK_METHOD(void,
              OnWorkerStarted,
              (const std::shared_ptr<WorkerInterface> &worker),
              (override));
  MOCK_METHOD(void,
              PushSpillWorker,
              (const std::shared_ptr<WorkerInterface> &worker),
              (override));
  MOCK_METHOD(void,
              PushRestoreWorker,
              (const std::shared_ptr<WorkerInterface> &worker),
              (override));
  MOCK_METHOD(void,
              DisconnectWorker,
              (const std::shared_ptr<WorkerInterface> &worker,
               rpc::WorkerExitType disconnect_type),
              (override));
  MOCK_METHOD(void,
              DisconnectDriver,
              (const std::shared_ptr<WorkerInterface> &driver),
              (override));
  MOCK_METHOD(void,
              PrestartWorkers,
              (const LeaseSpecification &lease_spec, int64_t backlog_size),
              (override));
  MOCK_METHOD(void,
              StartNewWorker,
              (const std::shared_ptr<PopWorkerRequest> &pop_worker_request),
              (override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));

  MOCK_METHOD(void,
              PopSpillWorker,
              (std::function<void(std::shared_ptr<WorkerInterface>)> callback),
              (override));

  MOCK_METHOD(void,
              PopRestoreWorker,
              (std::function<void(std::shared_ptr<WorkerInterface>)> callback),
              (override));

  MOCK_METHOD(void,
              PushDeleteWorker,
              (const std::shared_ptr<WorkerInterface> &worker),
              (override));

  MOCK_METHOD(void,
              PopDeleteWorker,
              (std::function<void(std::shared_ptr<WorkerInterface>)> callback),
              (override));

  boost::optional<const rpc::JobConfig &> GetJobConfig(
      const JobID &job_id) const override {
    RAY_CHECK(false) << "Not used.";
    return boost::none;
  }
};
}  // namespace ray::raylet
