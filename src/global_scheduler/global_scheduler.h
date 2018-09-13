#ifndef GLOBAL_SCHEDULER_H
#define GLOBAL_SCHEDULER_H

#include "task.h"

#include <unordered_map>

#include "ray/gcs/client.h"
#include "state/db.h"
#include "state/local_scheduler_table.h"

/* The frequency with which the global scheduler checks if there are any tasks
 * that haven't been scheduled yet. */
#define GLOBAL_SCHEDULER_TASK_CLEANUP_MILLISECONDS 100

/** Contains all information that is associated with a local scheduler. */
typedef struct {
  /** The ID of the local scheduler in Redis. */
  DBClientID id;
  /** The number of heartbeat intervals that have passed since we last heard
   *  from this local scheduler. */
  int64_t num_heartbeats_missed;
  /** The number of tasks sent from the global scheduler to this local
   *  scheduler. */
  int64_t num_tasks_sent;
  /** The number of tasks sent from the global scheduler to this local scheduler
   *  since the last heartbeat arrived. */
  int64_t num_recent_tasks_sent;
  /** The latest information about the local scheduler capacity. This is updated
   *  every time a new local scheduler heartbeat arrives. */
  LocalSchedulerInfo info;
} LocalScheduler;

typedef class GlobalSchedulerPolicyState GlobalSchedulerPolicyState;

/**
 * This defines a hash table used to cache information about different objects.
 */
typedef struct {
  /** The size in bytes of the object. */
  int64_t data_size;
  /** A vector of object locations for this object. */
  std::vector<std::string> object_locations;
} SchedulerObjectInfo;

/**
 * Global scheduler state structure.
 */
typedef struct {
  /** The global scheduler event loop. */
  event_loop *loop;
  /** The global state store database. */
  DBHandle *db;
  /** A hash table mapping local scheduler ID to the local schedulers that are
   *  connected to Redis. */
  std::unordered_map<DBClientID, LocalScheduler> local_schedulers;
  /** The state managed by the scheduling policy. */
  GlobalSchedulerPolicyState *policy_state;
  /** The plasma_manager ip:port -> local_scheduler_db_client_id association. */
  std::unordered_map<std::string, DBClientID> plasma_local_scheduler_map;
  /** The local_scheduler_db_client_id -> plasma_manager ip:port association. */
  std::unordered_map<DBClientID, std::string> local_scheduler_plasma_map;
  /** Objects cached by this global scheduler instance. */
  std::unordered_map<ObjectID, SchedulerObjectInfo> scheduler_object_info_table;
  /** An array of tasks that haven't been scheduled yet. */
  std::vector<Task *> pending_tasks;
} GlobalSchedulerState;

/**
 * This is a helper method to look up the local scheduler struct that
 * corresponds to a particular local_scheduler_id.
 *
 * @param state The state of the global scheduler.
 * @param The local_scheduler_id of the local scheduler.
 * @return The corresponding local scheduler struct. If the global scheduler is
 *         not aware of the local scheduler, then this will be NULL.
 */
LocalScheduler *get_local_scheduler(GlobalSchedulerState *state,
                                    DBClientID local_scheduler_id);

/**
 * Assign the given task to the local scheduler, update Redis and scheduler data
 * structures.
 *
 * @param state Global scheduler state.
 * @param task Task to be assigned to the local scheduler.
 * @param local_scheduler_id DB client ID for the local scheduler.
 * @return Void.
 */
void assign_task_to_local_scheduler(GlobalSchedulerState *state,
                                    Task *task,
                                    DBClientID local_scheduler_id);

#endif /* GLOBAL_SCHEDULER_H */
