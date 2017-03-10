#ifndef LOCAL_SCHEDULER_SHARED_H
#define LOCAL_SCHEDULER_SHARED_H

#include "common/task.h"
#include "common/state/table.h"
#include "common/state/db.h"
#include "plasma_client.h"
#include "utarray.h"
#include "uthash.h"

/* These are needed to define the UT_arrays. */
extern UT_icd task_ptr_icd;
extern UT_icd workers_icd;
extern UT_icd pid_t_icd;

/** This struct is used to maintain a mapping from actor IDs to the ID of the
 *  local scheduler that is responsible for the actor. */
typedef struct {
  /** The ID of the actor. This is used as a key in the hash table. */
  ActorID actor_id;
  /** The ID of the local scheduler that is responsible for the actor. */
  DBClientID local_scheduler_id;
  /** Handle fo the hash table. */
  UT_hash_handle hh;
} actor_map_entry;

/** Internal state of the scheduling algorithm. */
typedef struct SchedulingAlgorithmState SchedulingAlgorithmState;

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
  DBHandle *db;
  /** The Plasma client. */
  PlasmaConnection *plasma_conn;
  /** State for the scheduling algorithm. */
  SchedulingAlgorithmState *algorithm_state;
  /** Input buffer, used for reading input in process_message to avoid
   *  allocation for each call to process_message. */
  UT_array *input_buffer;
  /** Vector of static attributes associated with the node owned by this local
   *  scheduler. */
  double static_resources[ResourceIndex_MAX];
  /** Vector of dynamic attributes associated with the node owned by this local
   *  scheduler. */
  double dynamic_resources[ResourceIndex_MAX];
} LocalSchedulerState;

/** Contains all information associated with a local scheduler client. */
typedef struct {
  /** The socket used to communicate with the client. */
  int sock;
  /** A pointer to the task object that is currently running on this client. If
   *  no task is running on the worker, this will be NULL. This is used to
   *  update the task table. */
  Task *task_in_progress;
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
  ActorID actor_id;
  /** A pointer to the local scheduler state. */
  LocalSchedulerState *local_scheduler_state;
} LocalSchedulerClient;

#endif /* LOCAL_SCHEDULER_SHARED_H */
