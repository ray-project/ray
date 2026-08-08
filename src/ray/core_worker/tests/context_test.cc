// Copyright 2026 The Ray Authors.
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

#include "ray/core_worker/context.h"

#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_util.h"

namespace ray {
namespace core {

namespace {

constexpr int kTestJobId = 1;

WorkerContext MakeWorkerContext() {
  return WorkerContext(
      WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(kTestJobId));
}

// The job ID has to match the context's, otherwise WorkerContext::SetCurrentTask
// rejects the spec.
TaskSpecification MakeTaskSpec(const TaskID &task_id, uint64_t num_returns) {
  TaskSpecBuilder builder;
  rpc::Address empty_address;
  rpc::JobConfig config;
  std::unordered_map<std::string, double> resources = {{"CPU", 1}};
  builder.SetCommonTaskSpec(
      task_id,
      "dummy_task",
      Language::PYTHON,
      FunctionDescriptorBuilder::BuildPython("dummy_module", "", "dummy_function", ""),
      JobID::FromInt(kTestJobId),
      config,
      TaskID::Nil(),
      0,
      TaskID::Nil(),
      empty_address,
      num_returns,
      /*returns_dynamic=*/false,
      /*is_streaming_generator=*/false,
      /*generator_backpressure_num_objects=*/-1,
      resources,
      resources,
      "",
      0,
      TaskID::Nil(),
      "");
  return std::move(builder).ConsumeAndBuild();
}

}  // namespace

// Supplying both arguments keys the ObjectID to the caller's task and index rather
// than to whatever this thread's context happens to hold.
TEST(WorkerContextTest, GeneratorReturnIdUsesBothSuppliedArguments) {
  WorkerContext context = MakeWorkerContext();
  const TaskID task_id = TaskID::FromRandom(JobID::FromInt(kTestJobId));

  const ObjectID object_id =
      context.GetGeneratorReturnId(task_id, /*put_index=*/ObjectIDIndexType{1});

  EXPECT_EQ(object_id, ObjectID::FromIndex(task_id, 1));
}

// Omitting both is the form the production callers use: the task ID and the put index
// both come from the worker context. Asserting the deduced values, not a literal
// index, keeps this independent of the max_num_generator_returns config.
TEST(WorkerContextTest, GeneratorReturnIdDeducesBothWhenOmitted) {
  WorkerContext context = MakeWorkerContext();
  const TaskID task_id = TaskID::FromRandom(JobID::FromInt(kTestJobId));
  const uint64_t num_returns = 1;
  // thread_context_ is a static thread_local shared by every WorkerContext on this
  // thread, so reset the put counter: SetCurrentTask requires it to be zero.
  context.ResetCurrentTask();
  context.SetCurrentTask(MakeTaskSpec(task_id, num_returns));

  const ObjectID object_id =
      context.GetGeneratorReturnId(TaskID::Nil(), /*put_index=*/std::nullopt);

  EXPECT_EQ(object_id.TaskId(), task_id);
  // GetNextPutIndex reserves the generator window, so the deduced index lands past
  // both the return indices and that window.
  EXPECT_EQ(object_id.ObjectIndex(),
            static_cast<ObjectIDIndexType>(
                num_returns + RayConfig::instance().max_num_generator_returns() + 1));
}

// Supplying a task ID without a put index would take the index from this thread's put
// counter, which belongs to whatever task this thread is running, not to task_id. The
// resulting ObjectID can collide with one the other task mints.
TEST(WorkerContextDeathTest, GeneratorReturnIdRejectsTaskIdWithoutPutIndex) {
  WorkerContext context = MakeWorkerContext();
  const TaskID task_id = TaskID::FromRandom(JobID::FromInt(kTestJobId));

  EXPECT_DEATH((void)context.GetGeneratorReturnId(task_id, /*put_index=*/std::nullopt),
               "task_id and put_index must both be specified or both omitted");
}

// The mirror case: a put index with no task ID would attach a caller-chosen index to
// whichever task this thread happens to be running.
TEST(WorkerContextDeathTest, GeneratorReturnIdRejectsPutIndexWithoutTaskId) {
  WorkerContext context = MakeWorkerContext();

  EXPECT_DEATH((void)context.GetGeneratorReturnId(TaskID::Nil(),
                                                  /*put_index=*/ObjectIDIndexType{1}),
               "task_id and put_index must both be specified or both omitted");
}

}  // namespace core
}  // namespace ray
