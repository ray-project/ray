#ifndef EXAMPLE_TASK_H
#define EXAMPLE_TASK_H

#include "task.h"

task_spec *example_task(void) {
  function_id func_id = globally_unique_id();
  task_spec *task = alloc_task_spec(func_id, 2, 1, 0);
  task_args_add_ref(task, globally_unique_id());
  task_args_add_ref(task, globally_unique_id());
  return task;
}

#endif
