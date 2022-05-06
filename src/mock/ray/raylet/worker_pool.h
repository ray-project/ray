// Copyright 2021 The Ray Authors.
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

namespace ray {
namespace raylet {

class MockWorkerPoolInterface : public WorkerPoolInterface {
 public:
  MOCK_METHOD(void,
              PopWorker,
              (const TaskSpecification &task_spec,
               const PopWorkerCallback &callback,
               const std::string &allocated_instances_serialized_json),
              (override));
  MOCK_METHOD(void,
              PushWorker,
              (const std::shared_ptr<WorkerInterface> &worker),
              (override));
  MOCK_METHOD(const std::vector<std::shared_ptr<WorkerInterface>>,
              GetAllRegisteredWorkers,
              (bool filter_dead_workers),
              (override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockIOWorkerPoolInterface : public IOWorkerPoolInterface {
 public:
  MOCK_METHOD(void,
              PushSpillWorker,
              (const std::shared_ptr<WorkerInterface> &worker),
              (override));
  MOCK_METHOD(void,
              PopSpillWorker,
              (std::function<void(std::shared_ptr<WorkerInterface>)> callback),
              (override));
  MOCK_METHOD(void,
              PushRestoreWorker,
              (const std::shared_ptr<WorkerInterface> &worker),
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
  MOCK_METHOD(void,
              PushUtilWorker,
              (const std::shared_ptr<WorkerInterface> &worker),
              (override));
  MOCK_METHOD(void,
              PopUtilWorker,
              (std::function<void(std::shared_ptr<WorkerInterface>)> callback),
              (override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockWorkerPool : public WorkerPool {
 public:
  MOCK_METHOD(Process,
              StartProcess,
              (const std::vector<std::string> &worker_command_args,
               const ProcessEnvironment &env),
              (override));
  MOCK_METHOD(void, WarnAboutSize, (), (override));
  MOCK_METHOD(void,
              PopWorkerCallbackAsync,
              (const PopWorkerCallback &callback,
               std::shared_ptr<WorkerInterface> worker,
               PopWorkerStatus status),
              (override));
};

}  // namespace raylet
}  // namespace ray
