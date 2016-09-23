#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "utarray.h"

#include "task.h"
#include "common.h"
#include "io.h"

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

void write_task(int fd, task_spec *spec) {
  write_bytes(fd, (uint8_t *) spec, task_size(spec));
}

task_spec *read_task(int fd) {
  uint8_t *bytes;
  int64_t length;
  read_bytes(fd, &bytes, &length);
  task_spec *spec = (task_spec *) bytes;
  CHECK(task_size(spec) == length);
  return spec;
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

UT_icd unique_id_icd = {sizeof(unique_id), NULL, NULL, NULL};

task_spec *parse_task(char *task_string, int64_t task_length) {
  /* We make one pass through task_string to store all the argument ids
   * in "args" and all the return ids in "returns". */
  UT_array *args;
  utarray_new(args, &unique_id_icd);
  UT_array *returns;
  utarray_new(returns, &unique_id_icd);
  function_id function_id;
  char *cursor = strtok(task_string, " ");
  int index = 0;
  while (cursor != NULL) {
    /* This will be equal to "args" or "returns" depending on whether we
     * are processing an argument id or a return id. */
    UT_array *target = NULL;
    if (strncmp("fun", cursor, 3) == 0) {
      /* Parse function id. */
      CHECK(cursor + 2 * UNIQUE_ID_SIZE + 1 <= task_string + task_length);
      cursor = strtok(NULL, " ");
      hex_to_sha1(cursor, &function_id.id[0]);
      cursor = strtok(NULL, " ");
      CHECK(cursor);
      continue;
    } else if (strncmp("id:", cursor, 3) == 0) {
      /* Parse pass by reference argument. */
      sscanf(cursor, "id:%d", &index);
      target = args;
    } else if (strncmp("val:", cursor, 4) == 0) {
      /* Parse pass by value argument. */
      sscanf(cursor, "val:%d", &index);
      CHECK(0); /* Not implemented yet */
    } else if (strncmp("ret:", cursor, 4) == 0) {
      /* Parse return object reference. */
      sscanf(cursor, "ret:%d", &index);
      target = returns;
    }
    cursor = strtok(NULL, " ");
    CHECK(cursor);
    if (index >= utarray_len(target)) {
      utarray_resize(target, index + 1);
    }
    object_id *id = (object_id *) utarray_eltptr(target, index);
    hex_to_sha1(cursor, &id->id[0]);
    cursor = strtok(NULL, " ");
  }
  /* TODO(pcm): Implement pass by value. */
  /* Now assemble the task specification. */
  task_spec *spec =
      alloc_task_spec(function_id, utarray_len(args), utarray_len(returns), 0);
  for (int i = 0; i < utarray_len(args); ++i) {
    object_id *id = (object_id *) utarray_eltptr(args, i);
    task_args_add_ref(spec, *id);
  }
  for (int i = 0; i < utarray_len(returns); ++i) {
    object_id *id = (object_id *) utarray_eltptr(returns, i);
    *task_return(spec, i) = *id;
  }
  utarray_free(args);
  utarray_free(returns);
  return spec;
}
