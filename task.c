#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "task.h"
#include "common.h"
#include "sockets.h"

/* Tasks are stored in a consecutive chunk of memory, the first
 * sizeof(task_spec) bytes are arranged according to the struct
 * task_spec. Then there is an array of task_args of length
 * (num_args + num_returns), and then follows the data of
 * pass-by-value arguments of size args_value_size. The offsets in the
 * task_arg.val are with respect to the end of the augmented structure,
 * i.e. with respect to the address &task_spec.args_and_returns[0] +
 * (task_spec->num_args + task_spec->num_returns) * sizeof(task_arg). */

typedef struct {
  /* Either ARG_BY_REF or ARG_BY_VAL. */
  int8_t type;
  union {
    object_id obj_id;
    struct {
      /* Offset where the data associated to this arg is located relative
       * to &task_spec.args_and_returns[0]. */
      ptrdiff_t offset;
      int64_t length;
    } value;
  };
} task_arg;

struct task_spec_impl {
  function_id func_id;
  /* Total number of arguments. */
  int64_t num_args;
  /* Index of the last argument that has been constructed. */
  int64_t arg_index;
  /* Number of return values. */
  int64_t num_returns;
  /* Number of bytes the pass-by-value arguments are occupying. */
  int64_t args_value_size;
  /* The offset of the number of bytes of pass-by-value data that
   * has been written so far, relative to &task_spec->args_and_returns[0] +
   * (task_spec->num_args + task_spec->num_returns) * sizeof(task_arg) */
  int64_t args_value_offset;
  /* Argument and return IDs as well as offsets for pass-by-value args. */
  task_arg args_and_returns[0];
};

task_spec *alloc_task_spec(function_id func_id,
                           int64_t num_args,
                           int64_t num_returns,
                           int64_t args_value_size) {
  int64_t size = sizeof(task_spec) +
                 (num_args + num_returns) * sizeof(task_arg) + args_value_size;
  task_spec *task = malloc(size);
  memset(task, 0, size);
  task->func_id = func_id;
  task->num_args = num_args;
  task->arg_index = 0;
  task->num_returns = num_returns;
  task->args_value_size = args_value_size;
  return task;
}

int64_t task_num_args(task_spec *spec) {
  return spec->num_args;
}

int64_t task_num_returns(task_spec *spec) {
  return spec->num_returns;
}

int8_t task_arg_type(task_spec *spec, int64_t arg_index) {
  CHECK(0 <= arg_index && arg_index < spec->num_args);
  return spec->args_and_returns[arg_index].type;
}

object_id *task_arg_id(task_spec *spec, int64_t arg_index) {
  CHECK(0 <= arg_index && arg_index < spec->num_args);
  task_arg *arg = &spec->args_and_returns[arg_index];
  CHECK(arg->type == ARG_BY_REF)
  return &arg->obj_id;
}

uint8_t *task_arg_val(task_spec *spec, int64_t arg_index) {
  CHECK(0 <= arg_index && arg_index < spec->num_args);
  task_arg *arg = &spec->args_and_returns[arg_index];
  CHECK(arg->type == ARG_BY_VAL);
  uint8_t *data = (uint8_t *) &spec->args_and_returns[0];
  data += (spec->num_args + spec->num_returns) * sizeof(task_arg);
  return data + arg->value.offset;
}

int64_t task_arg_length(task_spec *spec, int64_t arg_index) {
  CHECK(0 <= arg_index && arg_index < spec->num_args);
  task_arg *arg = &spec->args_and_returns[arg_index];
  CHECK(arg->type == ARG_BY_VAL);
  return arg->value.length;
}

int64_t task_args_add_ref(task_spec *spec, object_id obj_id) {
  task_arg *arg = &spec->args_and_returns[spec->arg_index];
  arg->type = ARG_BY_REF;
  arg->obj_id = obj_id;
  return spec->arg_index++;
}

int64_t task_args_add_val(task_spec *spec, uint8_t *data, int64_t length) {
  task_arg *arg = &spec->args_and_returns[spec->arg_index];
  arg->type = ARG_BY_VAL;
  arg->value.offset = spec->args_value_offset;
  arg->value.length = length;
  uint8_t *addr = task_arg_val(spec, spec->arg_index);
  CHECK(spec->args_value_offset + length <= spec->args_value_size);
  CHECK(spec->arg_index != spec->num_args - 1 ||
        spec->args_value_offset + length == spec->args_value_size);
  memcpy(addr, data, length);
  spec->args_value_offset += length;
  return spec->arg_index++;
}

object_id *task_return(task_spec *spec, int64_t ret_index) {
  CHECK(0 <= ret_index && ret_index < spec->num_returns);
  task_arg *ret = &spec->args_and_returns[spec->num_args + ret_index];
  CHECK(ret->type == ARG_BY_REF); /* No memory corruption. */
  return &ret->obj_id;
}

void free_task_spec(task_spec *spec) {
  CHECK(spec->arg_index == spec->num_args); /* Task was fully constructed */
  free(spec);
}
