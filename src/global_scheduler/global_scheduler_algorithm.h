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

void handle_task_waiting(global_scheduler_state *state, task *original_task);

void handle_object_available(global_scheduler_state *state, object_id obj_id);

void handle_object_unavailable(global_scheduler_state *state, object_id obj_id);

void handle_local_scheduler_heartbeat(global_scheduler_state *state);

void handle_new_local_scheduler(global_scheduler_state *state,
                                db_client_id db_client_id);

#endif /* GLOBAL_SCHEDULER_ALGORITHM_H */
