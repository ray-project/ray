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

task_instance *example_task_instance(void) {
  task_iid iid = globally_unique_id();
  task_spec *spec = example_task();
  task_instance *instance =
      make_task_instance(iid, spec, TASK_STATUS_WAITING, NIL_ID);
  free_task_spec(spec);
  return instance;
}

#endif
