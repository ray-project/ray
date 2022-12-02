// Copyright 2022 The Ray Authors.
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

#include <sys/sysinfo.h>

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/test/util.h"
#include "ray/raylet/worker_killing_policy.h"

namespace ray {

namespace raylet {

class WorkerKillerTest : public ::testing::Test {
 protected:
  int32_t port_ = 2389;
  instrumented_io_context io_context_;

  std::shared_ptr<WorkerInterface> CreateActorWorker(int32_t max_restarts) {
    rpc::TaskSpec message;
    message.mutable_actor_creation_task_spec()->set_max_actor_restarts(max_restarts);
    message.set_type(ray::rpc::TaskType::ACTOR_TASK);
    TaskSpecification task_spec(message);
    RayTask task(task_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->SetAssignedTask(task);
    return worker;
  }

  std::shared_ptr<WorkerInterface> CreateActorCreationWorker(int32_t max_restarts) {
    rpc::TaskSpec message;
    message.mutable_actor_creation_task_spec()->set_max_actor_restarts(max_restarts);
    message.set_type(ray::rpc::TaskType::ACTOR_CREATION_TASK);
    TaskSpecification task_spec(message);
    RayTask task(task_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->SetAssignedTask(task);
    return worker;
  }

  std::shared_ptr<WorkerInterface> CreateTaskWorker(int32_t max_retries,
                                                    int32_t depth = 1) {
    rpc::TaskSpec message;
    message.set_max_retries(max_retries);
    message.set_type(ray::rpc::TaskType::NORMAL_TASK);
    message.set_depth(depth);
    TaskSpecification task_spec(message);
    RayTask task(task_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->SetAssignedTask(task);
    return worker;
  }
};

}  // namespace raylet

}  // namespace ray
