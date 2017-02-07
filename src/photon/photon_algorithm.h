#ifndef PHOTON_ALGORITHM_H
#define PHOTON_ALGORITHM_H

#include "photon.h"
#include "common/task.h"
#include "state/local_scheduler_table.h"

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
 *
 */
void provide_scheduler_info(local_scheduler_state *state,
                            scheduling_algorithm_state *algorithm_state,
                            local_scheduler_info *info);

/**
 * This function will be called when a new task is submitted by a worker for
 * execution. The task will either be:
 * 1. Put into the waiting queue, where it will wait for its dependencies to
 *    become available.
 * 2. Put into the dispatch queue, where it will wait for an available worker.
 * 3. Given to the global scheduler to be scheduled.
 *
 * Currently, the local scheduler policy is to keep the task if its
 * dependencies are ready and there is an available worker.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param task Task that is submitted by the worker.
 * @return Void.
 */
void handle_task_submitted(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_spec *spec);

/**
 * This version of handle_task_submitted is used when the task being submitted
 * is a method of an actor.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param task Task that is submitted by the worker.
 * @return Void.
 */
void handle_actor_task_submitted(local_scheduler_state *state,
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
 * This function will be called when an actor task is assigned by the global
 * scheduler or by another local scheduler for execution on this local
 * scheduler.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param task Task that is assigned by the global scheduler.
 * @return Void.
 */
void handle_actor_task_scheduled(local_scheduler_state *state,
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
 * @param worker The worker that is available.
 * @return Void.
 */
void handle_worker_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             local_scheduler_client *worker);

/**
 * This version of handle_worker_available is called whenever the worker that is
 * available is running an actor.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param wi Information about the worker that is available.
 * @return Void.
 */
void handle_actor_worker_available(local_scheduler_state *state,
                                   scheduling_algorithm_state *algorithm_state,
                                   local_scheduler_client *worker);

/**
 * Handle the fact that a new worker is available for running an actor.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param actor_id The ID of the actor running on the worker.
 * @param worker The worker that was connected.
 * @return Void.
 */
void handle_actor_worker_connect(local_scheduler_state *state,
                                 scheduling_algorithm_state *algorithm_state,
                                 actor_id actor_id,
                                 local_scheduler_client *worker);

/**
 * Handle the fact that a worker running an actor has disconnected.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param actor_id The ID of the actor running on the worker.
 * @return Void.
 */
void handle_actor_worker_disconnect(local_scheduler_state *state,
                                    scheduling_algorithm_state *algorithm_state,
                                    actor_id actor_id);

/**
 * This function fetches queued task's missing object dependencies. It is
 * called every LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS.
 *
 * @param loop The local scheduler's event loop.
 * @param id The ID of the timer that triggers this function.
 * @param context The function's context.
 * @return An integer representing the time interval in seconds before the
 *         next invocation of the function.
 */
int fetch_object_timeout_handler(event_loop *loop, timer_id id, void *context);

/** The following methods are for testing purposes only. */
#ifdef PHOTON_TEST
/**
 * Get the number of tasks currently waiting for object dependencies to become
 * available locally.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @return The number of tasks queued.
 */
int num_waiting_tasks(scheduling_algorithm_state *algorithm_state);

/**
 * Get the number of tasks currently waiting for a worker to become available.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @return The number of tasks queued.
 */
int num_dispatch_tasks(scheduling_algorithm_state *algorithm_state);
#endif

#endif /* PHOTON_ALGORITHM_H */
