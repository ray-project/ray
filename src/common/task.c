#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "utarray.h"

#include "task.h"
#include "common.h"
#include "io.h"

/* TASK SPECIFICATIONS */

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
  /* Function ID of the task. */
  function_id function_id;
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

/* The size of a task specification is given by the following expression. */
#define TASK_SPEC_SIZE(NUM_ARGS, NUM_RETURNS, ARGS_VALUE_SIZE)           \
  (sizeof(task_spec) + ((NUM_ARGS) + (NUM_RETURNS)) * sizeof(task_arg) + \
   (ARGS_VALUE_SIZE))

task_spec *alloc_task_spec(function_id function_id,
                           int64_t num_args,
                           int64_t num_returns,
                           int64_t args_value_size) {
  int64_t size = TASK_SPEC_SIZE(num_args, num_returns, args_value_size);
  task_spec *task = malloc(size);
  memset(task, 0, size);
  task->function_id = function_id;
  task->num_args = num_args;
  task->arg_index = 0;
  task->num_returns = num_returns;
  task->args_value_size = args_value_size;
  return task;
}

int64_t task_size(task_spec *spec) {
  return TASK_SPEC_SIZE(spec->num_args, spec->num_returns,
                        spec->args_value_size);
}

unique_id *task_function(task_spec *spec) {
  return &spec->function_id;
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

void print_task(task_spec *spec, UT_string *output) {
  /* For converting an id to hex, which has double the number
   * of bytes compared to the id (+ 1 byte for '\0'). */
  static char hex[2 * UNIQUE_ID_SIZE + 1];
  /* Print function id. */
  sha1_to_hex(&task_function(spec)->id[0], &hex[0]);
  utstring_printf(output, "fun %s ", &hex[0]);
  /* Print arguments. */
  for (int i = 0; i < task_num_args(spec); ++i) {
    sha1_to_hex(&task_arg_id(spec, i)->id[0], &hex[0]);
    utstring_printf(output, " id:%d %s", i, &hex[0]);
  }
  /* Print return ids. */
  for (int i = 0; i < task_num_returns(spec); ++i) {
    object_id *object_id = task_return(spec, i);
    sha1_to_hex(&object_id->id[0], &hex[0]);
    utstring_printf(output, " ret:%d %s", i, &hex[0]);
  }
}

/* TASK INSTANCES */

struct task_instance_impl {
  task_iid iid;
  int32_t state;
  node_id node;
  task_spec spec;
};

task_instance *make_task_instance(task_iid task_iid,
                                  task_spec *spec,
                                  int32_t state,
                                  node_id node) {
  int64_t size = sizeof(task_instance) - sizeof(task_spec) + task_size(spec);
  task_instance *result = malloc(size);
  memset(result, 0, size);
  result->iid = task_iid;
  result->state = state;
  result->node = node;
  memcpy(&result->spec, spec, task_size(spec));
  return result;
}

int64_t task_instance_size(task_instance *instance) {
  return sizeof(task_instance) - sizeof(task_spec) + task_size(&instance->spec);
}

task_iid *task_instance_id(task_instance *instance) {
  return &instance->iid;
}

int32_t *task_instance_state(task_instance *instance) {
  return &instance->state;
}

node_id *task_instance_node(task_instance *instance) {
  return &instance->node;
}

task_spec *task_instance_task_spec(task_instance *instance) {
  return &instance->spec;
}

void task_instance_free(task_instance *instance) {
  free(instance);
}
