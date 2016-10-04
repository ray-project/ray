#ifndef TASK_H
#define TASK_H

/* This API specifies the task data structures. It is in C so we can
 * easily construct tasks from other languages like Python. The datastructures
 * are also defined in such a way that memory is contiguous and all pointers
 * are relative, so that we can memcpy the datastructure and ship it over the
 * network without serialization and deserialization. */

#include <stddef.h>
#include <stdint.h>
#include "common.h"
#include "utstring.h"

typedef unique_id function_id;

/* The task ID is a deterministic hash of the function ID that
 * the task executes and the argument IDs or argument values */
typedef unique_id task_id;

/* The task instance ID is a globally unique ID generated which
 * identifies this particular execution of the task */
typedef unique_id task_iid;

/* The node id is an identifier for the node the task is
 * scheduled on */
typedef unique_id node_id;

/*
 * TASK SPECIFICATIONS: Contain all the information neccessary
 * to execute the task (function id, arguments, return object ids).
 *
 */

typedef struct task_spec_impl task_spec;

/* If argument is passed by value or reference. */
enum arg_type { ARG_BY_REF, ARG_BY_VAL };

/* Construct and modify task specifications. */

/* Allocating and initializing a task. */
task_spec *alloc_task_spec(function_id function_id,
                           int64_t num_args,
                           int64_t num_returns,
                           int64_t args_value_size);

/* Size of the task in bytes. */
int64_t task_size(task_spec *spec);

/* Return the function ID of the task. */
unique_id *task_function(task_spec *spec);

/* Getting the number of arguments and returns. */
int64_t task_num_args(task_spec *spec);
int64_t task_num_returns(task_spec *spec);

/* Getting task arguments. */
int8_t task_arg_type(task_spec *spec, int64_t arg_index);
unique_id *task_arg_id(task_spec *spec, int64_t arg_index);
uint8_t *task_arg_val(task_spec *spec, int64_t arg_index);
int64_t task_arg_length(task_spec *spec, int64_t arg_index);

/* Setting task arguments. Note that this API only allows you to set the
 * arguments in their order of appearance. */
int64_t task_args_add_ref(task_spec *spec, object_id obj_id);
int64_t task_args_add_val(task_spec *spec, uint8_t *data, int64_t length);

/* Getting and setting return arguments. Tasks return by reference for now. */
unique_id *task_return(task_spec *spec, int64_t ret_index);

/* Freeing the task datastructure. */
void free_task_spec(task_spec *spec);

/* Write the task specification to a file or socket. */
void write_task(int fd, task_spec *spec);

/* Read the task specification from a file or socket. It is the user's
 * responsibility to free the task after it has been used. */
task_spec *read_task(int fd);

/* Print task as a humanly readable string. */
void print_task(task_spec *spec, UT_string *output);

/*
 * SCHEDULED TASK: Contains information about a scheduled task:
 * the task iid, the task specification and the task status
 * (WAITING, SCHEDULED, RUNNING, DONE) and which node the
 * task is scheduled on.
 *
 */

/* The scheduling_state can be used as a flag when we are listening
 * for an event, for example TASK_WAITING | TASK_SCHEDULED. */
enum scheduling_state {
  TASK_STATUS_WAITING = 1,
  TASK_STATUS_SCHEDULED = 2,
  TASK_STATUS_RUNNING = 4,
  TASK_STATUS_DONE = 8
};

/* A task instance is one execution of a task specification.
 * It has a unique instance id, a state of execution (see scheduling_state)
 * and a node it is scheduled on or running on. */
typedef struct task_instance_impl task_instance;

/* Allocate and initialize a new task instance. Must be freed with
 * scheduled_task_free after use. */
task_instance *make_task_instance(task_iid task_iid,
                                  task_spec *task,
                                  int32_t state,
                                  node_id node);

/* Size of task instance structure in bytes. */
int64_t task_instance_size(task_instance *instance);

/* Instance ID of the task instance. */
task_iid *task_instance_id(task_instance *instance);

/* The scheduling state of the task instance. */
int32_t *task_instance_state(task_instance *instance);

/* Node this task instance has been assigned to or is running on. */
node_id *task_instance_node(task_instance *instance);

/* Task specification of this task instance. */
task_spec *task_instance_task_spec(task_instance *instance);

/* Free this task instance datastructure. */
void task_instance_free(task_instance *instance);

#endif
