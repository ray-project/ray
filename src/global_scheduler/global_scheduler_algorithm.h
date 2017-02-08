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

typedef enum {
  SCHED_ALGORITHM_ROUND_ROBIN = 1,
  SCHED_ALGORITHM_TRANSFER_AWARE = 2,
  SCHED_ALGORITHM_MAX
} global_scheduler_algorithm;

/** The state managed by the global scheduling policy. */
struct global_scheduler_policy_state {
  /** The index of the next local scheduler to assign a task to. */
  int64_t round_robin_index;
  double resource_attribute_weight[MAX_RESOURCE_INDEX + 1];
};

typedef struct {
  const char *object_location;
  int64_t total_object_size;
  UT_hash_handle hh;
} object_size_entry;

/**
 * Create the state of the global scheduler policy. This state must be freed by
 * the caller.
 *
 * @return The state of the scheduling policy.
 */
global_scheduler_policy_state *init_global_scheduler_policy(void);

/**
 * Free the global scheduler policy state.
 *
 * @param policy_state The policy state to free.
 * @return Void.
 */
void destroy_global_scheduler_policy(
    global_scheduler_policy_state *policy_state);

/**
 * Main new task handling function in the global scheduler.
 *
 * @param state Global scheduler state.
 * @param policy_state State specific to the scheduling policy.
 * @param task New task to be scheduled.
 * @return  Void.
 */
void handle_task_waiting(global_scheduler_state *state,
                         global_scheduler_policy_state *policy_state,
                         task *task);

/**
 * Handle the fact that a new object is available.
 *
 * @param state The global scheduler state.
 * @param policy_state The state managed by the scheduling policy.
 * @param object_id The ID of the object that is now available.
 * @return Void.
 */
void handle_object_available(global_scheduler_state *state,
                             global_scheduler_policy_state *policy_state,
                             object_id object_id);

/**
 * Handle a heartbeat message from a local scheduler. TODO(rkn): this is a
 * placeholder for now.
 *
 * @param state The global scheduler state.
 * @param policy_state The state managed by the scheduling policy.
 * @return Void.
 */
void handle_local_scheduler_heartbeat(
    global_scheduler_state *state,
    global_scheduler_policy_state *policy_state);

/**
 * Handle the presence of a new local scheduler. Currently, this just adds the
 * local scheduler to a queue of local schedulers.
 *
 * @param state The global scheduler state.
 * @param policy_state The state managed by the scheduling policy.
 * @param The db client ID of the new local scheduler.
 * @return Void.
 */
void handle_new_local_scheduler(global_scheduler_state *state,
                                global_scheduler_policy_state *policy_state,
                                db_client_id db_client_id);

#endif /* GLOBAL_SCHEDULER_ALGORITHM_H */
