#ifndef EXAMPLE_TASK_H
#define EXAMPLE_TASK_H

#include "task.h"

extern TaskBuilder *g_task_builder;

const int64_t arg_value_size = 1000;

static inline task_spec *example_task_spec_with_args(int64_t num_args,
                                                     int64_t num_returns,
                                                     ObjectID arg_ids[],
                                                     int64_t *task_spec_size) {
  TaskID parent_task_id = globally_unique_id();
  FunctionID func_id = globally_unique_id();
  start_construct_task_spec(g_task_builder, NIL_ID, parent_task_id, 0,
                            NIL_ACTOR_ID, 0, func_id, num_returns);
  for (int64_t i = 0; i < num_args; ++i) {
    ObjectID arg_id;
    if (arg_ids == NULL) {
      arg_id = globally_unique_id();
    } else {
      arg_id = arg_ids[i];
    }
    task_args_add_ref(g_task_builder, arg_id);
  }
  return finish_construct_task_spec(g_task_builder, task_spec_size);
}

static inline task_spec *example_task_spec(int64_t num_args,
                                           int64_t num_returns,
                                           int64_t *task_spec_size) {
  return example_task_spec_with_args(num_args, num_returns, NULL,
                                     task_spec_size);
}

static inline Task *example_task_with_args(int64_t num_args,
                                           int64_t num_returns,
                                           int task_state,
                                           ObjectID arg_ids[]) {
  int64_t task_spec_size;
  task_spec *spec = example_task_spec_with_args(num_args, num_returns, arg_ids,
                                                &task_spec_size);
  Task *instance = Task_alloc(spec, task_spec_size, task_state, NIL_ID);
  TaskSpec_free(spec);
  return instance;
}

static inline Task *example_task(int64_t num_args,
                                 int64_t num_returns,
                                 int task_state) {
  int64_t task_spec_size;
  task_spec *spec = example_task_spec(num_args, num_returns, &task_spec_size);
  Task *instance = Task_alloc(spec, task_spec_size, task_state, NIL_ID);
  TaskSpec_free(spec);
  return instance;
}

#endif /* EXAMPLE_TASK_H */
