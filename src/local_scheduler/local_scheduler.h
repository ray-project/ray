#ifndef LOCAL_SCHEDULER_H
#define LOCAL_SCHEDULER_H

#include "task.h"
#include "event_loop.h"

/**
 * Establish a connection to a new client.
 *
 * @param loop Event loop of the local scheduler.
 * @param listener_socket Socket the local scheduler is listening on for new
 *        client requests.
 * @param context State of the local scheduler.
 * @param events Flag for events that are available on the listener socket.
 * @return Void.
 */
void new_client_connection(event_loop *loop,
                           int listener_sock,
                           void *context,
                           int events);

/**
 * Check if a driver is still alive.
 *
 * @param driver_id The ID of the driver.
 * @return True if the driver is still alive and false otherwise.
 */
bool is_driver_alive(WorkerID driver_id);

/**
 * This function can be called by the scheduling algorithm to assign a task
 * to a worker.
 *
 * @param info
 * @param task The task that is submitted to the worker.
 * @param worker The worker to assign the task to.
 * @return Void.
 */
void assign_task_to_worker(LocalSchedulerState *state,
                           TaskSpec *task,
                           int64_t task_spec_size,
                           LocalSchedulerClient *worker);

/*
 * This function is called whenever a task has finished on one of the workers.
 * It updates the resource accounting and the global state store.
 *
 * @param state The local scheduler state.
 * @param worker The worker that finished the task.
 * @param actor_checkpoint_failed If the last task assigned was a checkpoint
 *        task that failed.
 * @return Void.
 */
void finish_task(LocalSchedulerState *state,
                 LocalSchedulerClient *worker,
                 bool actor_checkpoint_failed);

/**
 * This is the callback that is used to process a notification from the Plasma
 * store that an object has been sealed.
 *
 * @param loop The local scheduler's event loop.
 * @param client_sock The file descriptor to read the notification from.
 * @param context The local scheduler state.
 * @param events
 * @return Void.
 */
void process_plasma_notification(event_loop *loop,
                                 int client_sock,
                                 void *context,
                                 int events);

/**
 * Reconstruct an object. If the object does not exist on any nodes, according
 * to the state tables, and if the object is not already being reconstructed,
 * this triggers a single reexecution of the task that originally created the
 * object.
 *
 * @param state The local scheduler state.
 * @param object_id The ID of the object to reconstruct.
 * @return Void.
 */
void reconstruct_object(LocalSchedulerState *state, ObjectID object_id);

void print_resource_info(const LocalSchedulerState *s, const TaskSpec *spec);

/**
 * Kill a worker, if it is a child process, and clean up all of its associated
 * state. Note that this function is also called on drivers, but it should not
 * actually send a kill signal to drivers.
 *
 * @param state The local scheduler state.
 * @param worker The local scheduler client to kill.
 * @param wait A boolean representing whether to wait for the killed worker to
 *        exit.
 * @param suppress_warning A bool that is true if we should not warn the driver,
 *        and false otherwise. This should only be true when a driver is
 *        removed.
 * @return Void.
 */
void kill_worker(LocalSchedulerState *state,
                 LocalSchedulerClient *worker,
                 bool wait,
                 bool suppress_warning);

/**
 * Start a worker. This forks a new worker process that can be added to the
 * pool of available workers, pending registration of its PID with the local
 * scheduler.
 *
 * @param state The local scheduler state.
 * @param actor_id The ID of the actor for this worker. If this worker is not an
 *        actor, then NIL_ACTOR_ID should be used.
 * @param reconstruct True if the worker is an actor and is being started in
 *        reconstruct mode.
 * @param Void.
 */
void start_worker(LocalSchedulerState *state,
                  ActorID actor_id,
                  bool reconstruct);

/**
 * Check if a certain quantity of dynamic resources are available. If num_cpus
 * is 0, we ignore the dynamic number of available CPUs (which may be negative).
 *
 * @param state The state of the local scheduler.
 * @param num_cpus Check if this many CPUs are available.
 * @param num_gpus Check if this many GPUs are available.
 * @return True if there are enough CPUs and GPUs and false otherwise.
 */
bool check_dynamic_resources(LocalSchedulerState *state,
                             double num_cpus,
                             double num_gpus,
                             double num_custom_resource);

/**
 * Acquire additional resources (CPUs and GPUs) for a worker.
 *
 * @param state The local scheduler state.
 * @param worker The worker who is acquiring resources.
 * @param num_cpus The number of CPU resources to acquire.
 * @param num_gpus The number of GPU resources to acquire.
 * @return Void.
 */
void acquire_resources(LocalSchedulerState *state,
                       LocalSchedulerClient *worker,
                       double num_cpus,
                       double num_gpus,
                       double num_custom_resource);

/**
 * Return resources (CPUs and GPUs) being used by a worker to the local
 * scheduler.
 *
 * @param state The local scheduler state.
 * @param worker The worker who is returning resources.
 * @param num_cpus The number of CPU resources to return.
 * @param num_gpus The number of GPU resources to return.
 * @return Void.
 */
void release_resources(LocalSchedulerState *state,
                       LocalSchedulerClient *worker,
                       double num_cpus,
                       double num_gpus,
                       double num_custom_resource);

/** The following methods are for testing purposes only. */
#ifdef LOCAL_SCHEDULER_TEST
LocalSchedulerState *LocalSchedulerState_init(
    const char *node_ip_address,
    event_loop *loop,
    const char *redis_addr,
    int redis_port,
    const char *local_scheduler_socket_name,
    const char *plasma_manager_socket_name,
    const char *plasma_store_socket_name,
    const char *plasma_manager_address,
    bool global_scheduler_exists,
    const double static_resource_vector[],
    const char *worker_path,
    int num_workers);

SchedulingAlgorithmState *get_algorithm_state(LocalSchedulerState *state);

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events);

#endif

#endif /* LOCAL_SCHEDULER_H */
