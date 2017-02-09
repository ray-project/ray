#ifndef PHOTON_H
#define PHOTON_H

#include "common/task.h"
#include "common/state/table.h"
#include "common/state/db.h"
#include "plasma_client.h"
#include "utarray.h"
#include "uthash.h"

/* Retry values for state table operations. For now, only try each command once
 * and give it one second to succeed. */
/* TODO(swang): We should set retry values in a config file somewhere. */
static const retry_info photon_retry = {.num_retries = 0,
                                        .timeout = 1000,
                                        .fail_callback = NULL};

enum photon_message_type {
  /** Notify the local scheduler that a task has finished. */
  TASK_DONE = 64,
  /** Get a new task from the local scheduler. */
  GET_TASK,
  /** This is sent from the local scheduler to a worker to tell the worker to
   *  execute a task. */
  EXECUTE_TASK,
  /** Reconstruct a possibly lost object. */
  RECONSTRUCT_OBJECT,
  /** Log a message to the event table. */
  EVENT_LOG_MESSAGE,
};

// clang-format off
/** Contains all information that is associated to a worker. */
typedef struct {
  int sock;
  /** A pointer to a task object, to update the task table. */
  task *task_in_progress;
} worker;
// clang-format on

/* These are needed to define the UT_arrays. */
UT_icd task_ptr_icd;
UT_icd worker_icd;

/** Internal state of the scheduling algorithm. */
typedef struct scheduling_algorithm_state scheduling_algorithm_state;

/** A struct storing the configuration state of the local scheduler. This should
 *  consist of values that don't change over the lifetime of the local
 *  scheduler. */
typedef struct {
  /** The script to use when starting a new worker. */
  char *start_worker_command;
  /** Whether there is a global scheduler. */
  bool global_scheduler_exists;
} local_scheduler_config;

/** The state of the local scheduler. */
typedef struct {
  /** The configuration for the local scheduler. */
  local_scheduler_config config;
  /** The local scheduler event loop. */
  event_loop *loop;
  /** List of workers available to this node. This is used to free the worker
   *  structs when we free the scheduler state and also to access the worker
   *  structs in the tests. */
  UT_array *workers;
  /** The handle to the database. */
  db_handle *db;
  /** The Plasma client. */
  plasma_connection *plasma_conn;
  /** State for the scheduling algorithm. */
  scheduling_algorithm_state *algorithm_state;
  /** Input buffer, used for reading input in process_message to avoid
   *  allocation for each call to process_message. */
  UT_array *input_buffer;
  /** Vector of static attributes associated with the node owned by this local
   *  scheduler. */
  double static_resources[MAX_RESOURCE_INDEX];
  /** Vector of dynamic attributes associated with the node owned by this local
   *  scheduler. */
  double dynamic_resources[MAX_RESOURCE_INDEX];
} local_scheduler_state;

/** Contains all information associated with a local scheduler client. */
typedef struct {
  /** The socket used to communicate with the client. */
  int sock;
  /** A pointer to the task object that is currently running on this client. If
   *  no task is running on the worker, this will be NULL. This is used to
   *  update the task table. */
  task *task_in_progress;
  /** A pointer to the local scheduler state. */
  local_scheduler_state *local_scheduler_state;
} local_scheduler_client;

#endif /* PHOTON_H */
