#ifndef GLOBAL_SCHEDULER_H
#define GLOBAL_SCHEDULER_H

#include "task.h"

#include "state/db.h"
#include "utarray.h"
#include "uthash.h"

/** Contains all information that is associated with a local scheduler. */
typedef struct {
  /** The ID of the local scheduler in Redis. */
  db_client_id id;
  /** The number of tasks sent from the global scheduler to this local
   *  scheduler. */
  int64_t num_tasks_sent;
} local_scheduler;

typedef struct global_scheduler_policy_state global_scheduler_policy_state;

/**
 * This defines a hash table used to cache information about different objects.
 */
typedef struct {
  /** The object ID in question. */
  object_id object_id;
  /** The size in bytes of the object. */
  int64_t data_size;
  /** An array of object locations for this object. */
  UT_array *object_locations;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} scheduler_object_info;

typedef struct {
  char *aux_address; /* Key */
  db_client_id photon_db_client_id;
  UT_hash_handle hh;
} aux_address_entry;

typedef struct {
  /** The global scheduler event loop. */
  event_loop *loop;
  /** The global state store database. */
  db_handle *db;
  /** The local schedulers that are connected to Redis. */
  UT_array *local_schedulers;
  /** The state managed by the scheduling policy. */
  global_scheduler_policy_state *policy_state;
  aux_address_entry *plasma_photon_map;
  /** Objects cached by this global scheduler instance. */
  scheduler_object_info *scheduler_object_info_table;
} global_scheduler_state;

void assign_task_to_local_scheduler(global_scheduler_state *state,
                                    task *task,
                                    node_id node_id);

#endif /* GLOBAL_SCHEDULER_H */
