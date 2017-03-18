#ifndef LOCAL_SCHEDULER_H
#define LOCAL_SCHEDULER_H

#include "task.h"
#include "event_loop.h"

/* The duration that we wait after sending a worker SIGTERM before sending the
 * worker SIGKILL. */
#define KILL_WORKER_TIMEOUT_MILLISECONDS 100

#define DEFAULT_NUM_CPUS INT16_MAX
#define DEFAULT_NUM_GPUS 0

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
 * Kill a worker.
 *
 * @param worker The local scheduler client to kill.
 * @param wait A boolean representing whether to wait for the killed worker to
 *        exit.
 * @param Void.
 */
void kill_worker(LocalSchedulerClient *worker, bool wait);

/**
 * Start a worker. This forks a new worker process that can be added to the
 * pool of available workers, pending registration of its PID with the local
 * scheduler.
 *
 * @param state The local scheduler state.
 * @param actor_id The ID of the actor for this worker. If this worker is not an
 *        actor, then NIL_ACTOR_ID should be used.
 * @param Void.
 */
void start_worker(LocalSchedulerState *state, ActorID actor_id);

/**
 * Update our accounting for the current resources being used, according to
 * some task that is starting or finishing execution.
 *
 * @param state The local scheduler state.
 * @param spec The specification for the task that is or was using resources.
 * @param return_resources A boolean representing whether the task is starting
 *        or finishing execution. If true, then the task is finishing execution
 *        (possibly temporarily), so it will add to the dynamic resources
 *        available. Else, it will take from the dynamic resources available.
 * @return Void.
 */
void update_dynamic_resources(LocalSchedulerState *state,
                              TaskSpec *spec,
                              bool return_resources);

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

void LocalSchedulerState_free(LocalSchedulerState *state);

SchedulingAlgorithmState *get_algorithm_state(LocalSchedulerState *state);

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events);

#endif

#endif /* LOCAL_SCHEDULER_H */
