#ifndef LOCAL_SCHEDULER_ALGORITHM_H
#define LOCAL_SCHEDULER_ALGORITHM_H

#include "local_scheduler_shared.h"
#include "common/task.h"
#include "state/local_scheduler_table.h"

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
SchedulingAlgorithmState *SchedulingAlgorithmState_init(void);

/**
 * Free the scheduler state.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @return Void.
 */
void SchedulingAlgorithmState_free(SchedulingAlgorithmState *algorithm_state);

/**
 *
 */
void provide_scheduler_info(LocalSchedulerState *state,
                            SchedulingAlgorithmState *algorithm_state,
                            LocalSchedulerInfo *info);

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
void handle_task_submitted(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           TaskExecutionSpec &execution_spec);

/**
 * This version of handle_task_submitted is used when the task being submitted
 * is a method of an actor.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param task Task that is submitted by the worker.
 * @return Void.
 */
void handle_actor_task_submitted(LocalSchedulerState *state,
                                 SchedulingAlgorithmState *algorithm_state,
                                 TaskExecutionSpec &execution_spec);

/**
 * This function will be called when the local scheduler receives a notification
 * about the creation of a new actor. This can be used by the scheduling
 * algorithm to resubmit cached actor tasks.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param actor_id The ID of the actor being created.
 * @return Void.
 */
void handle_actor_creation_notification(
    LocalSchedulerState *state,
    SchedulingAlgorithmState *algorithm_state,
    ActorID actor_id);

/**
 * This function will be called when a task is assigned by the global scheduler
 * for execution on this local scheduler.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param task Task that is assigned by the global scheduler.
 * @return Void.
 */
void handle_task_scheduled(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           TaskExecutionSpec &execution_spec);

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
void handle_actor_task_scheduled(LocalSchedulerState *state,
                                 SchedulingAlgorithmState *algorithm_state,
                                 TaskExecutionSpec &execution_spec);

/**
 * This function is called if a new object becomes available in the local
 * plasma store.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param object_id ID of the object that became available.
 * @return Void.
 */
void handle_object_available(LocalSchedulerState *state,
                             SchedulingAlgorithmState *algorithm_state,
                             ObjectID object_id);

/**
 * This function is called if an object is removed from the local plasma store.
 *
 * @param state The state of the local scheduler.
 * @param object_id ID of the object that was removed.
 * @return Void.
 */
void handle_object_removed(LocalSchedulerState *state, ObjectID object_id);

/**
 * This function is called when a new worker becomes available.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker The worker that is available.
 * @return Void.
 */
void handle_worker_available(LocalSchedulerState *state,
                             SchedulingAlgorithmState *algorithm_state,
                             LocalSchedulerClient *worker);

/**
 * This function is called when a worker is removed.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker The worker that is removed.
 * @return Void.
 */
void handle_worker_removed(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           LocalSchedulerClient *worker);

/**
 * This version of handle_worker_available is called whenever the worker that is
 * available is running an actor.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker The worker that is available.
 * @return Void.
 */
void handle_actor_worker_available(LocalSchedulerState *state,
                                   SchedulingAlgorithmState *algorithm_state,
                                   LocalSchedulerClient *worker);

/**
 * Handle the fact that a new worker is available for running an actor.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param actor_id The ID of the actor running on the worker.
 * @param initial_execution_dependency The dummy object ID of the actor
 *        creation task.
 * @param worker The worker that was converted to an actor.
 * @return Void.
 */
void handle_convert_worker_to_actor(
    LocalSchedulerState *state,
    SchedulingAlgorithmState *algorithm_state,
    const ActorID &actor_id,
    const ObjectID &initial_execution_dependency,
    LocalSchedulerClient *worker);

/**
 * Handle the fact that a worker running an actor has disconnected.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker The worker that was disconnected.
 * @param cleanup Whether the disconnect was during cleanup.
 * @return Void.
 */
void handle_actor_worker_disconnect(LocalSchedulerState *state,
                                    SchedulingAlgorithmState *algorithm_state,
                                    LocalSchedulerClient *worker,
                                    bool cleanup);

/**
 * This function is called when a worker that was executing a task becomes
 * blocked on an object that isn't available locally yet.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker The worker that is blocked.
 * @return Void.
 */
void handle_worker_blocked(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           LocalSchedulerClient *worker);

/**
 * This function is called when an actor that was executing a task becomes
 * blocked on an object that isn't available locally yet.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker The worker that is blocked.
 * @return Void.
 */
void handle_actor_worker_blocked(LocalSchedulerState *state,
                                 SchedulingAlgorithmState *algorithm_state,
                                 LocalSchedulerClient *worker);

/**
 * This function is called when a worker that was blocked on an object that
 * wasn't available locally yet becomes unblocked.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker The worker that is now unblocked.
 * @return Void.
 */
void handle_worker_unblocked(LocalSchedulerState *state,
                             SchedulingAlgorithmState *algorithm_state,
                             LocalSchedulerClient *worker);

/**
 * This function is called when an actor that was blocked on an object that
 * wasn't available locally yet becomes unblocked.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param worker The worker that is now unblocked.
 * @return Void.
 */
void handle_actor_worker_unblocked(LocalSchedulerState *state,
                                   SchedulingAlgorithmState *algorithm_state,
                                   LocalSchedulerClient *worker);

/**
 * Process the fact that a driver has been removed. This will remove all of the
 * tasks for that driver from the scheduling algorithm's internal data
 * structures.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param driver_id The ID of the driver that was removed.
 * @return Void.
 */
void handle_driver_removed(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           WorkerID driver_id);

/**
 * This function fetches queued task's missing object dependencies. It is
 * called every local_scheduler_fetch_timeout_milliseconds.
 *
 * @param loop The local scheduler's event loop.
 * @param id The ID of the timer that triggers this function.
 * @param context The function's context.
 * @return An integer representing the time interval in seconds before the
 *         next invocation of the function.
 */
int fetch_object_timeout_handler(event_loop *loop, timer_id id, void *context);

/**
 * This function initiates reconstruction for task's missing object
 * dependencies. It is called every
 * local_scheduler_reconstruction_timeout_milliseconds, but it may not initiate
 * reconstruction for every missing object.
 *
 * @param loop The local scheduler's event loop.
 * @param id The ID of the timer that triggers this function.
 * @param context The function's context.
 * @return An integer representing the time interval in seconds before the
 *         next invocation of the function.
 */
int reconstruct_object_timeout_handler(event_loop *loop,
                                       timer_id id,
                                       void *context);

/// This function initiates reconstruction for the actor creation tasks
/// corresponding to the actor tasks cached in the local scheduler.
///
/// \param loop The local scheduler's event loop.
/// \param id The ID of the timer that triggers this function.
/// \param context The function's context.
/// \return An integer representing the time interval in seconds before the
/// next invocation of the function.
int rerun_actor_creation_tasks_timeout_handler(event_loop *loop,
                                               timer_id id,
                                               void *context);

/**
 * Check whether an object, including actor dummy objects, is locally
 * available.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param object_id The ID of the object to check for.
 * @return A bool representing whether the object is locally available.
 */
bool object_locally_available(SchedulingAlgorithmState *algorithm_state,
                              ObjectID object_id);

/// Spill some tasks back to the global scheduler. This function implements the
/// spillback policy.
///
/// @param state The scheduler state.
/// @return Void.
void spillback_tasks_handler(LocalSchedulerState *state);

/**
 * A helper function to print debug information about the current state and
 * number of workers.
 *
 * @param message A message to identify the log message.
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @return Void.
 */
void print_worker_info(const char *message,
                       SchedulingAlgorithmState *algorithm_state);

/*
 * The actor frontier consists of the number of tasks executed so far and the
 * execution dependencies required by the current runnable tasks, according to
 * the actor's local scheduler. Since an actor may have multiple handles, the
 * tasks submitted to the actor form a DAG, where nodes are tasks and edges are
 * execution dependencies. The frontier is a cut across this DAG. The number of
 * tasks so far is the number of nodes included in the DAG root's partition.
 *
 * The actor gets the current frontier of tasks from the local scheduler during
 * a checkpoint save, so that it can save the point in the actor's lifetime at
 * which the checkpoint was taken. If the actor later resumes from that
 * checkpoint, the actor can set the current frontier of tasks in the local
 * scheduler so that the same frontier of tasks can be made runnable again
 * during reconstruction, and so that we do not duplicate execution of tasks
 * that already executed before the checkpoint.
 */

/**
 * Get the number of tasks, per actor handle, that have been executed on an
 * actor so far.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param actor_id The ID of the actor whose task counters are returned.
 * @return A map from handle ID to the number of tasks submitted by that handle
 *         that have executed so far.
 */
std::unordered_map<ActorHandleID, int64_t, UniqueIDHasher>
get_actor_task_counters(SchedulingAlgorithmState *algorithm_state,
                        ActorID actor_id);

/**
 * Set the number of tasks, per actor handle, that have been executed on an
 * actor so far. All previous counts will be overwritten. Tasks that are
 * waiting or runnable on the local scheduler that have a lower task count will
 * be discarded, so that we don't duplicate execution.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param actor_id The ID of the actor whose task counters are returned.
 * @param task_counters A map from handle ID to the number of tasks submitted
 *        by that handle that have executed so far.
 * @return Void.
 */
void set_actor_task_counters(
    SchedulingAlgorithmState *algorithm_state,
    ActorID actor_id,
    const std::unordered_map<ActorHandleID, int64_t, UniqueIDHasher>
        &task_counters);

/**
 * Get the actor's frontier of task dependencies.
 * NOTE(swang): The returned frontier only includes handles known by the local
 * scheduler. It does not include handles for which the local scheduler has not
 * seen a runnable task yet.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param actor_id The ID of the actor whose task counters are returned.
 * @return A map from handle ID to execution dependency for the earliest
 *         runnable task submitted through that handle.
 */
std::unordered_map<ActorHandleID, ObjectID, UniqueIDHasher> get_actor_frontier(
    SchedulingAlgorithmState *algorithm_state,
    ActorID actor_id);

/**
 * Set the actor's frontier of task dependencies. The previous frontier will be
 * overwritten. Any tasks that have an execution dependency on the new frontier
 * (and that have all other dependencies fulfilled) will become runnable.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @param actor_id The ID of the actor whose task counters are returned.
 * @param frontier_dependencies A map from handle ID to execution dependency
 *        for the earliest runnable task submitted through that handle.
 * @return Void.
 */
void set_actor_frontier(
    LocalSchedulerState *state,
    SchedulingAlgorithmState *algorithm_state,
    ActorID actor_id,
    const std::unordered_map<ActorHandleID, ObjectID, UniqueIDHasher>
        &frontier_dependencies);

/** The following methods are for testing purposes only. */
#ifdef LOCAL_SCHEDULER_TEST
/**
 * Get the number of tasks currently waiting for object dependencies to become
 * available locally.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @return The number of tasks queued.
 */
int num_waiting_tasks(SchedulingAlgorithmState *algorithm_state);

/**
 * Get the number of tasks currently waiting for a worker to become available.
 *
 * @param algorithm_state State maintained by the scheduling algorithm.
 * @return The number of tasks queued.
 */
int num_dispatch_tasks(SchedulingAlgorithmState *algorithm_state);
#endif

#endif /* LOCAL_SCHEDULER_ALGORITHM_H */
