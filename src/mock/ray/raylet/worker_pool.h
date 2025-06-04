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
              (const TaskSpecification &task_spec, const PopWorkerCallback &callback),
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
              GetRegisteredDriver,
              (const WorkerID &worker_id),
              (const, override));
};
}  // namespace ray::raylet
