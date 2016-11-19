#ifndef GLOBAL_SCHEDULER_ALGORITHM_H
#define GLOBAL_SCHEDULER_ALGORITHM_H

#include "common.h"
#include "global_scheduler.h"
#include "task.h"

/* ==== The scheduling algorithm ====
 *
 * This file contains declaration for all functions and data structures that
 * need to be provided if you want to implement a new algorithm for the global
 * scheduler.
 *
 */

/**
 * Assign the task to a local scheduler. At the moment, this simply assigns the
 * task to the first local scheduler and if there are no local schedulers it
 * fails.
 *
 * @param state The global scheduler state.
 * @param task The task that is waiting to be scheduled.
 * @return Void.
 */
void handle_task_waiting(global_scheduler_state *state, task *task);

/**
 * Handle the fact that a new object is available.
 *
 * @param state The global scheduler state.
 * @param object_id The ID of the object that is now available.
 * @return Void.
 */
void handle_object_available(global_scheduler_state *state,
                             object_id object_id);

/**
 * Handle a heartbeat message from a local scheduler. TODO(rkn): this is a
 * placeholder for now.
 *
 * @param state The global scheduler state.
 * @return Void.
 */
void handle_local_scheduler_heartbeat(global_scheduler_state *state);

/**
 * Handle the presence of a new local scheduler. Currently, this just adds the
 * local scheduler to a queue of local schedulers.
 *
 * @param state The global scheduler state.
 * @param The db client ID of the new local scheduler.
 * @return Void.
 */
void handle_new_local_scheduler(global_scheduler_state *state,
                                db_client_id db_client_id);

#endif /* GLOBAL_SCHEDULER_ALGORITHM_H */
