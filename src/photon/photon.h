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

/** Association between the socket fd of a worker and its worker_index. */
typedef struct {
  /** The socket fd of a worker. */
  int sock;
  /** The index of the worker in scheduler_info->workers. */
  int64_t worker_index;
  /** Handle for the hash table. */
  UT_hash_handle hh;
} worker_index;

/** Internal state of the scheduling algorithm. */
typedef struct scheduling_algorithm_state scheduling_algorithm_state;

/** The state of the local scheduler. */
typedef struct {
  /** The local scheduler event loop. */
  event_loop *loop;
  /** Association between client socket and worker index. */
  worker_index *worker_index;
  /** List of workers available to this node. The index into this array
   *  is the worker_index and is used to identify workers throughout
   *  the program. */
  UT_array *workers;
  /** The handle to the database. */
  db_handle *db;
  /** Whether there is a global scheduler. */
  bool global_scheduler_exists;
  /** The Plasma client. */
  plasma_connection *plasma_conn;
  /** State for the scheduling algorithm. */
  scheduling_algorithm_state *algorithm_state;
  /** Input buffer, used for reading input in process_message to avoid
   *  allocation for each call to process_message. */
  UT_array *input_buffer;
  /* Vector of static attributes associated with the node owned by this local
   * scheduler. */
  double static_resources[MAX_RESOURCE_INDEX];
  /* Vector of dynamic attributes associated with the node owned by this local
   * scheduler. */
  double dynamic_resources[MAX_RESOURCE_INDEX];
} local_scheduler_state;

#endif /* PHOTON_H */
