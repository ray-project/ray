#ifndef GLOBAL_SCHEDULER_H
#define GLOBAL_SCHEDULER_H

#include "task.h"

#include "state/db.h"
#include "utarray.h"

/** Contains all information that is associated with a local scheduler. */
typedef struct {
  /** The ID of the local scheduler in Redis. */
  db_client_id id;
} local_scheduler;

typedef struct {
  /** The global scheduler event loop. */
  event_loop *loop;
  /** The global state store database. */
  db_handle *db;
  /** The local schedulers that are connected to Redis. */
  UT_array *local_schedulers;
} global_scheduler_state;

void assign_task_to_local_scheduler(global_scheduler_state *state,
                                    task *original_task,
                                    node_id node_id);

#endif /* GLOBAL_SCHEDULER_H */
