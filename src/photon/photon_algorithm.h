#ifndef PHOTON_ALGORITHM_H
#define PHOTON_ALGORITHM_H

#include "photon.h"
#include "common/task.h"

/* The duration that the local scheduler will wait before reinitiating a fetch
 * request for a missing task dependency. TODO(rkn): We may want this to be
 * adaptable based on the load on the local scheduler. */
#define LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS 1000

/* ==== The scheduling algorithm ====
 *
 * This file contains declaration for all functions and data structures
 * that need to be provided if you want to implement a new algorithms
 * for the local scheduler.
 *
 */

/**
 * Initialize the scheduler state.
 *
 * @return State managed by the scheduling algorithm.
 */
scheduling_algorithm_state *make_scheduling_algorithm_state(void);

/**
 * Free the scheduler state.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @return Void.
 */
void free_scheduling_algorithm_state(
    scheduling_algorithm_state *algorithm_state);

/**
 * This function will be called when a new task is submitted by a worker for
 * execution.
 *
 * @param task Task that is submitted by the worker.
 * @return Void.
 */
void handle_task_submitted(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_spec *spec);

/**
 * This function will be called when a task is assigned by the global scheduler
 * for execution on this local scheduler.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param task Task that is assigned by the global scheduler.
 * @return Void.
 */
void handle_task_scheduled(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_spec *spec);

/**
 * This function is called if a new object becomes available in the local
 * plasma store.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param object_id ID of the object that became available.
 * @return Void.
 */
void handle_object_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             object_id object_id);

/**
 * This function is called if an object is removed from the local plasma store.
 *
 * @param state The state of the local scheduler.
 * @param object_id ID of the object that was removed.
 * @return Void.
 */
void handle_object_removed(local_scheduler_state *state, object_id object_id);

/**
 * This function is called when a new worker becomes available
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker_index The index of the worker that becomes available.
 * @return Void.
 */
void handle_worker_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             int worker_index);

/** The following methods are for testing purposes only. */
#ifdef PHOTON_TEST
/**
 * Get the number of tasks currently queued locally.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @return The number of tasks queued locally.
 */
int num_tasks_in_queue(scheduling_algorithm_state *algorithm_state);
#endif

#endif /* PHOTON_ALGORITHM_H */
