#ifndef PHOTON_SCHEDULER_H
#define PHOTON_SCHEDULER_H

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
 * This function can be called by the scheduling algorithm to assign a task
 * to a worker.
 *
 * @param info
 * @param task The task that is submitted to the worker.
 * @param worker_index The index of the worker the task is submitted to.
 * @param from_global_scheduler True if the task was assigned to the local
 *        scheduler by the global scheduler and false otherwise.
 * @return Void.
 */
void assign_task_to_worker(local_scheduler_state *state,
                           task_spec *task,
                           int worker_index,
                           bool from_global_scheduler);

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
void reconstruct_object(local_scheduler_state *state, object_id object_id);

/** The following methods are for testing purposes only. */
#ifdef PHOTON_TEST
local_scheduler_state *init_local_scheduler(
    event_loop *loop,
    const char *redis_addr,
    int redis_port,
    const char *plasma_manager_socket_name,
    const char *plasma_store_socket_name,
    bool global_scheduler_exists);

void free_local_scheduler(local_scheduler_state *state);

scheduling_algorithm_state *get_algorithm_state(local_scheduler_state *state);

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events);
#endif

#endif /* PHOTON_SCHEDULER_H */
