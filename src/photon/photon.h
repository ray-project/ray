#ifndef PHOTON_H
#define PHOTON_H

#include "common/task.h"
#include "common/state/db.h"
#include "plasma_client.h"
#include "utarray.h"
#include "uthash.h"

enum photon_message_type {
  /** Notify the local scheduler that a task has finished. */
  TASK_DONE = 64,
  /** Get a new task from the local scheduler. */
  GET_TASK,
  /** This is sent from the local scheduler to a worker to tell the worker to
   *  execute a task. */
  EXECUTE_TASK,
};

// clang-format off
/** Contains all information that is associated to a worker. */
typedef struct {
  int sock;
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
  /** The Plasma client. */
  plasma_connection *plasma_conn;
  /** State for the scheduling algorithm. */
  scheduling_algorithm_state *algorithm_state;
  /** Input buffer, used for reading input in process_message to avoid
   *  allocation for each call to process_message. */
  UT_array *input_buffer;
} local_scheduler_state;

#endif /* PHOTON_H */
