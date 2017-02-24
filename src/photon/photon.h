#ifndef PHOTON_H
#define PHOTON_H

#include "common/task.h"
#include "common/state/table.h"
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
  /** Reconstruct a possibly lost object. */
  RECONSTRUCT_OBJECT,
  /** Log a message to the event table. */
  EVENT_LOG_MESSAGE,
  /** Send an initial connection message to the local scheduler.
   *  This contains the worker's process ID and actor ID. */
  REGISTER_WORKER_INFO,
  /** For a worker that was blocked on some object(s), tell the local scheduler
   *  that the worker is now unblocked. */
  NOTIFY_UNBLOCKED,
};

/* These are needed to define the UT_arrays. */
UT_icd task_ptr_icd;
UT_icd workers_icd;
UT_icd pid_t_icd;

/** This struct is used to register a new worker with the local scheduler.
 *  It is shipped as part of photon_connect */
typedef struct {
  /** The ID of the actor. This is NIL_ACTOR_ID if the worker is not an actor.
   */
  actor_id actor_id;
  /** The process ID of this worker. */
  pid_t worker_pid;
} register_worker_info;

/** This struct is used to maintain a mapping from actor IDs to the ID of the
 *  local scheduler that is responsible for the actor. */
typedef struct {
  /** The ID of the actor. This is used as a key in the hash table. */
  actor_id actor_id;
  /** The ID of the local scheduler that is responsible for the actor. */
  db_client_id local_scheduler_id;
  /** Handle fo the hash table. */
  UT_hash_handle hh;
} actor_map_entry;

/** Internal state of the scheduling algorithm. */
typedef struct scheduling_algorithm_state scheduling_algorithm_state;

/** A struct storing the configuration state of the local scheduler. This should
 *  consist of values that don't change over the lifetime of the local
 *  scheduler. */
typedef struct {
  /** The script to use when starting a new worker. */
  const char **start_worker_command;
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
  /** List of the process IDs for child processes (workers) started by the
   *  local scheduler that have not sent a REGISTER_PID message yet. */
  UT_array *child_pids;
  /** A hash table mapping actor IDs to the db_client_id of the local scheduler
   *  that is responsible for the actor. */
  actor_map_entry *actor_mapping;
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
  /** A flag to indicate whether this worker is currently blocking on an
   *  object(s) that isn't available locally yet. */
  bool is_blocked;
  /** The process ID of the client. If this is set to zero, the client has not
   *  yet registered a process ID. */
  pid_t pid;
  /** Whether the client is a child process of the local scheduler. */
  bool is_child;
  /** The ID of the actor on this worker. If there is no actor running on this
   *  worker, this should be NIL_ACTOR_ID. */
  actor_id actor_id;
  /** A pointer to the local scheduler state. */
  local_scheduler_state *local_scheduler_state;
} local_scheduler_client;

#endif /* PHOTON_H */
