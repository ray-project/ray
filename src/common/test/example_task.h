#ifndef EXAMPLE_TASK_H
#define EXAMPLE_TASK_H

#include "task.h"

extern TaskBuilder *g_task_builder;

const int64_t arg_value_size = 1000;

static inline TaskExecutionSpec *example_task_execution_spec_with_args(
    int64_t num_args,
    int64_t num_returns,
    ObjectID arg_ids[]) {
  TaskID parent_task_id = globally_unique_id();
  FunctionID func_id = globally_unique_id();
  TaskSpec_start_construct(g_task_builder, NIL_ID, parent_task_id, 0,
                           NIL_ACTOR_ID, NIL_ACTOR_ID, 0, false, func_id,
                           num_returns);
  for (int64_t i = 0; i < num_args; ++i) {
    ObjectID arg_id;
    if (arg_ids == NULL) {
      arg_id = globally_unique_id();
    } else {
      arg_id = arg_ids[i];
    }
    TaskSpec_args_add_ref(g_task_builder, &arg_id, 1);
  }
  int64_t task_spec_size;
  TaskSpec *spec = TaskSpec_finish_construct(g_task_builder, &task_spec_size);
  std::vector<ObjectID> execution_dependencies = std::vector<ObjectID>();
  TaskExecutionSpec *execution_spec =
      TaskExecutionSpec_alloc(execution_dependencies, spec, task_spec_size);
  TaskSpec_free(spec);
  return execution_spec;
}

static inline TaskExecutionSpec *example_task_execution_spec(
    int64_t num_args,
    int64_t num_returns) {
  return example_task_execution_spec_with_args(num_args, num_returns, NULL);
}

static inline Task *example_task_with_args(int64_t num_args,
                                           int64_t num_returns,
                                           int task_state,
                                           ObjectID arg_ids[]) {
  TaskExecutionSpec *spec =
      example_task_execution_spec_with_args(num_args, num_returns, arg_ids);
  Task *instance = Task_alloc(spec, task_state, NIL_ID);
  TaskExecutionSpec_free(spec);
  return instance;
}

static inline Task *example_task(int64_t num_args,
                                 int64_t num_returns,
                                 int task_state) {
  TaskExecutionSpec *spec = example_task_execution_spec(num_args, num_returns);
  Task *instance = Task_alloc(spec, task_state, NIL_ID);
  TaskExecutionSpec_free(spec);
  return instance;
}

#endif /* EXAMPLE_TASK_H */
