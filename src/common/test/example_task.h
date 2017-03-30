#ifndef EXAMPLE_TASK_H
#define EXAMPLE_TASK_H

#include "task.h"

extern TaskBuilder *g_task_builder;

const int64_t arg_value_size = 1000;

static inline TaskSpec *example_task_spec_with_args(int64_t num_args,
                                                    int64_t num_returns,
                                                    ObjectID arg_ids[],
                                                    int64_t submit_depth,
                                                    int64_t *task_spec_size) {
  TaskID parent_task_id = globally_unique_id();
  FunctionID func_id = globally_unique_id();
  TaskSpec_start_construct(g_task_builder, NIL_ID, parent_task_id, 0,
                           submit_depth, NIL_ACTOR_ID, 0, func_id, num_returns);
  for (int64_t i = 0; i < num_args; ++i) {
    ObjectID arg_id;
    if (arg_ids == NULL) {
      arg_id = globally_unique_id();
    } else {
      arg_id = arg_ids[i];
    }
    TaskSpec_args_add_ref(g_task_builder, arg_id);
  }
  return TaskSpec_finish_construct(g_task_builder, task_spec_size);
}

static inline TaskSpec *example_task_spec_with_submit_depth(
    int64_t num_args,
    int64_t num_returns,
    int64_t submit_depth,
    int64_t *task_spec_size) {
  return example_task_spec_with_args(num_args, num_returns, NULL, submit_depth,
                                     task_spec_size);
}

static inline TaskSpec *example_task_spec(int64_t num_args,
                                          int64_t num_returns,
                                          int64_t *task_spec_size) {
  return example_task_spec_with_args(num_args, num_returns, NULL, 0,
                                     task_spec_size);
}

static inline Task *example_task_with_args(int64_t num_args,
                                           int64_t num_returns,
                                           int task_state,
                                           ObjectID arg_ids[]) {
  int64_t task_spec_size;
  TaskSpec *spec = example_task_spec_with_args(num_args, num_returns, arg_ids,
                                               0, &task_spec_size);
  Task *instance = Task_alloc(spec, task_spec_size, task_state, NIL_ID);
  TaskSpec_free(spec);
  return instance;
}

static inline Task *example_task(int64_t num_args,
                                 int64_t num_returns,
                                 int task_state) {
  int64_t task_spec_size;
  TaskSpec *spec = example_task_spec(num_args, num_returns, &task_spec_size);
  Task *instance = Task_alloc(spec, task_spec_size, task_state, NIL_ID);
  TaskSpec_free(spec);
  return instance;
}

#endif /* EXAMPLE_TASK_H */
