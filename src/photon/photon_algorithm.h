#ifndef PHOTON_ALGORITHM_H
#define PHOTON_ALGORITHM_H

#include "photon.h"
#include "common/task.h"

/* ==== The scheduling algorithm ====
 *
 * This file contains declaration for all functions and data structures
 * that need to be provided if you want to implement a new algorithms
 * for the local scheduler.
 *
 */

/** Internal state of the scheduling algorithm. */
typedef struct scheduler_state scheduler_state;

/**
 * Initialize the scheduler state.
 *
 * @return Internal state of the scheduling algorithm.
 */
scheduler_state *make_scheduler_state(void);

/**
 * Free the scheduler state.
 *
 * @param state Internal state of the scheduling algorithm.
 * @return Void.
 */
void free_scheduler_state(scheduler_state *state);

/**
 * This function will be called when a new task is submitted by a worker for
 * execution.
 *
 * @param info Info about resources exposed by photon to the scheduling
 *        algorithm.
 * @param state State of the scheduling algorithm.
 * @param task Task that is submitted by the worker.
 * @return Void.
 */
void handle_task_submitted(scheduler_info *info,
                           scheduler_state *state,
                           task_spec *spec);

/**
 * This function will be called when a task is assigned by the global scheduler
 * for execution on this local scheduler.
 *
 * @param info Info about resources exposed by photon to the scheduling
 *        algorithm.
 * @param state State of the scheduling algorithm.
 * @param task Task that is assigned by the global scheduler.
 * @return Void.
 */
void handle_task_scheduled(scheduler_info *info,
                           scheduler_state *state,
                           task_spec *spec);

/**
 * This function is called if a new object becomes available in the local
 * plasma store.
 *
 * @param info Info about resources exposed by photon to the scheduling
 *        algorithm.
 * @param state State of the scheduling algorithm.
 * @param object_id ID of the object that became available.
 * @return Void.
 */
void handle_object_available(scheduler_info *info,
                             scheduler_state *state,
                             object_id object_id);

/**
 * This function is called when a new worker becomes available
 *
 * @param info Info about resources exposed by photon to the scheduling
 *        algorithm.
 * @param state State of the scheduling algorithm.
 * @param worker_index The index of the worker that becomes available.
 * @return Void.
 */
void handle_worker_available(scheduler_info *info,
                             scheduler_state *state,
                             int worker_index);

#endif /* PHOTON_ALGORITHM_H */
