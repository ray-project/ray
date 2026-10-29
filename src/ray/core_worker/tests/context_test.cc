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
#include "ray/common/task/task_util.h"

namespace ray {
namespace core {

namespace {

constexpr int kTestJobId = 1;

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

// ResetCurrentTask zeroes the per-thread task index and put counter, which is what
// SetCurrentTask requires to be zero. The counters live in a static thread_local that
// outlives any one WorkerContext, so without this a test that advances the put counter
// would crash the next test that sets a task. It leaves the current task spec in place;
// no test here reads a task it did not set itself.
class WorkerContextTest : public ::testing::Test {
 protected:
  void TearDown() override { context_.ResetCurrentTask(); }

  WorkerContext context_{
      WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(kTestJobId)};
};

// gtest runs suites whose name ends in DeathTest before the others; both share the
// fixture so the reset above applies to every case in this file.
class WorkerContextDeathTest : public WorkerContextTest {};

// Supplying both arguments keys the ObjectID to the caller's task and index rather
// than to whatever this thread's context happens to hold.
TEST_F(WorkerContextTest, GeneratorReturnIdUsesBothSuppliedArguments) {
  const TaskID task_id = TaskID::FromRandom(JobID::FromInt(kTestJobId));

  const ObjectID object_id =
      context_.GetGeneratorReturnId(task_id, /*put_index=*/ObjectIDIndexType{1});

  EXPECT_EQ(object_id, ObjectID::FromIndex(task_id, 1));
}

// Omitting both is the form the production callers use: the task ID comes from the
// current task and the index from the thread's put counter. Assert those two properties
// rather than the index GetNextPutIndex computes, which would just restate its formula.
TEST_F(WorkerContextTest, GeneratorReturnIdDeducesBothWhenOmitted) {
  const TaskID task_id = TaskID::FromRandom(JobID::FromInt(kTestJobId));
  const uint64_t num_returns = 1;
  context_.SetCurrentTask(MakeTaskSpec(task_id, num_returns));

  const ObjectID object_id =
      context_.GetGeneratorReturnId(TaskID::Nil(), /*put_index=*/std::nullopt);
  const ObjectID next_object_id =
      context_.GetGeneratorReturnId(TaskID::Nil(), /*put_index=*/std::nullopt);

  // The task came from the context, not from the Nil() we passed.
  EXPECT_EQ(object_id.TaskId(), task_id);
  EXPECT_EQ(next_object_id.TaskId(), task_id);
  // The index came from the put counter: it advances per call and stays clear of the
  // indices reserved for the task's own returns.
  EXPECT_GT(object_id.ObjectIndex(), num_returns);
  EXPECT_EQ(next_object_id.ObjectIndex(), object_id.ObjectIndex() + 1);
}

// Supplying a task ID without a put index would take the index from this thread's put
// counter, which belongs to whatever task this thread is running, not to task_id. The
// resulting ObjectID can collide with one the other task mints.
TEST_F(WorkerContextDeathTest, GeneratorReturnIdRejectsTaskIdWithoutPutIndex) {
  const TaskID task_id = TaskID::FromRandom(JobID::FromInt(kTestJobId));

  EXPECT_DEATH((void)context_.GetGeneratorReturnId(task_id, /*put_index=*/std::nullopt),
               "task_id and put_index must both be specified or both omitted");
}

// The mirror case: a put index with no task ID would attach a caller-chosen index to
// whichever task this thread happens to be running.
TEST_F(WorkerContextDeathTest, GeneratorReturnIdRejectsPutIndexWithoutTaskId) {
  EXPECT_DEATH((void)context_.GetGeneratorReturnId(TaskID::Nil(),
                                                   /*put_index=*/ObjectIDIndexType{1}),
               "task_id and put_index must both be specified or both omitted");
}

}  // namespace core
}  // namespace ray
