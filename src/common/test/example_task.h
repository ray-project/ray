#ifndef EXAMPLE_TASK_H
#define EXAMPLE_TASK_H

#include "task.h"

extern TaskBuilder *g_task_builder;

const int64_t arg_value_size = 1000;

static inline TaskExecutionSpec example_task_execution_spec_with_args(
    int64_t num_args,
    int64_t num_returns,
    ObjectID arg_ids[]) {
  TaskID parent_task_id = TaskID::from_random();
  FunctionID func_id = FunctionID::from_random();
  TaskSpec_start_construct(g_task_builder, UniqueID::nil(), parent_task_id, 0,
                           ActorID::nil(), ObjectID::nil(), ActorID::nil(),
                           ActorID::nil(), 0, false, func_id, num_returns);
  for (int64_t i = 0; i < num_args; ++i) {
    ObjectID arg_id;
    if (arg_ids == NULL) {
      arg_id = ObjectID::from_random();
    } else {
      arg_id = arg_ids[i];
    }
    TaskSpec_args_add_ref(g_task_builder, &arg_id, 1);
  }
  int64_t task_spec_size;
  TaskSpec *spec = TaskSpec_finish_construct(g_task_builder, &task_spec_size);
  std::vector<ObjectID> execution_dependencies;
  auto execution_spec =
      TaskExecutionSpec(execution_dependencies, spec, task_spec_size);
  TaskSpec_free(spec);
  return execution_spec;
}

static inline TaskExecutionSpec example_task_execution_spec(
    int64_t num_args,
    int64_t num_returns) {
  return example_task_execution_spec_with_args(num_args, num_returns, NULL);
}

static inline Task *example_task_with_args(int64_t num_args,
                                           int64_t num_returns,
                                           TaskStatus task_state,
                                           ObjectID arg_ids[]) {
  TaskExecutionSpec spec =
      example_task_execution_spec_with_args(num_args, num_returns, arg_ids);
  Task *instance = Task_alloc(spec, task_state, UniqueID::nil());
  return instance;
}

static inline Task *example_task(int64_t num_args,
                                 int64_t num_returns,
                                 TaskStatus task_state) {
  TaskExecutionSpec spec = example_task_execution_spec(num_args, num_returns);
  Task *instance = Task_alloc(spec, task_state, UniqueID::nil());
  return instance;
}

static inline bool Task_equals(Task *task1, Task *task2) {
  if (task1->state != task2->state) {
    return false;
  }
  if (!(task1->local_scheduler_id == task2->local_scheduler_id)) {
    return false;
  }
  auto execution_spec1 = Task_task_execution_spec(task1);
  auto execution_spec2 = Task_task_execution_spec(task2);
  if (execution_spec1->SpecSize() != execution_spec2->SpecSize()) {
    return false;
  }
  return memcmp(execution_spec1->Spec(), execution_spec2->Spec(),
                execution_spec1->SpecSize()) == 0;
}

#endif /* EXAMPLE_TASK_H */
