#ifndef LOCAL_SCHEDULER_SHARED_H
#define LOCAL_SCHEDULER_SHARED_H

#include "common/task.h"
#include "common/state/table.h"
#include "common/state/db.h"
#include "plasma/client.h"
#include "ray/gcs/client.h"

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/** This struct is used to maintain a mapping from actor IDs to the ID of the
 *  local scheduler that is responsible for the actor. */
struct ActorMapEntry {
  /** The ID of the driver that created the actor. */
  WorkerID driver_id;
  /** The ID of the local scheduler that is responsible for the actor. */
  DBClientID local_scheduler_id;
};

/** Internal state of the scheduling algorithm. */
typedef struct SchedulingAlgorithmState SchedulingAlgorithmState;

struct LocalSchedulerClient;

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
struct LocalSchedulerState {
  /** The configuration for the local scheduler. */
  local_scheduler_config config;
  /** The local scheduler event loop. */
  event_loop *loop;
  /** List of workers available to this node. This is used to free the worker
   *  structs when we free the scheduler state and also to access the worker
   *  structs in the tests. */
  std::list<LocalSchedulerClient *> workers;
  /** A set of driver IDs corresponding to drivers that have been removed. This
   *  is used to make sure we don't execute any tasks belong to dead drivers. */
  std::unordered_set<WorkerID> removed_drivers;
  /** A set of actors IDs corresponding to local actors that have been removed.
   * This ensures we can reject any tasks destined for dead actors. */
  std::unordered_set<ActorID> removed_actors;
  /** List of the process IDs for child processes (workers) started by the
   *  local scheduler that have not sent a REGISTER_PID message yet. */
  std::vector<pid_t> child_pids;
  /** A hash table mapping actor IDs to the db_client_id of the local scheduler
   *  that is responsible for the actor. */
  std::unordered_map<ActorID, ActorMapEntry> actor_mapping;
  /** The handle to the database. */
  DBHandle *db;
  /** The Plasma client. */
  plasma::PlasmaClient *plasma_conn;
  /** State for the scheduling algorithm. */
  SchedulingAlgorithmState *algorithm_state;
  /** Input buffer, used for reading input in process_message to avoid
   *  allocation for each call to process_message. */
  std::vector<uint8_t> input_buffer;
  /** Vector of static attributes associated with the node owned by this local
   *  scheduler. */
  std::unordered_map<std::string, double> static_resources;
  /** Vector of dynamic attributes associated with the node owned by this local
   *  scheduler. */
  std::unordered_map<std::string, double> dynamic_resources;
  /** The IDs of the available GPUs. There is redundancy here in that
   *  available_gpus.size() == dynamic_resources[ResourceIndex_GPU] should
   *  always be true. */
  std::vector<int> available_gpus;
  /** The time (in milliseconds since the Unix epoch) when the most recent
   *  heartbeat was sent. */
  int64_t previous_heartbeat_time;
};

/** Contains all information associated with a local scheduler client. */
struct LocalSchedulerClient {
  /** The socket used to communicate with the client. */
  int sock;
  /** True if the client has registered and false otherwise. */
  bool registered;
  /** True if the client has sent a disconnect message to the local scheduler
   *  and false otherwise. If this is true, then the local scheduler will not
   *  propagate an error message to the driver when the client exits. */
  bool disconnected;
  /** True if the client is a worker and false if it is a driver. */
  bool is_worker;
  /** The worker ID if the client is a worker and the driver ID if the client is
   *  a driver. */
  WorkerID client_id;
  /** A pointer to the task object that is currently running on this client. If
   *  no task is running on the worker, this will be NULL. This is used to
   *  update the task table. */
  Task *task_in_progress;
  /** An array of resource counts currently in use by the worker.  */
  std::unordered_map<std::string, double> resources_in_use;
  /** A vector of the IDs of the GPUs that the worker is currently using. If the
   *  worker is an actor, this will be constant throughout the lifetime of the
   *  actor (and will be equal to the number of GPUs requested by the actor). If
   *  the worker is not an actor, this will be constant for the duration of a
   *  task and will have length equal to the number of GPUs requested by the
   *  task (in particular it will not change if the task blocks). */
  std::vector<int> gpus_in_use;
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
};

/**
 * Free the local scheduler state. This disconnects all clients and notifies
 * the global scheduler of the local scheduler's exit.
 *
 * @param state The state to free.
 * @return Void
 */
void LocalSchedulerState_free(LocalSchedulerState *state);

#endif /* LOCAL_SCHEDULER_SHARED_H */
