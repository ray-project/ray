#ifndef PHOTON_SCHEDULER_H
#define PHOTON_SCHEDULER_H

#include "task.h"
#include "event_loop.h"

typedef struct local_scheduler_state local_scheduler_state;

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
 * @return Void.
 */
void assign_task_to_worker(scheduler_info *info,
                           task_spec *task,
                           int worker_index);

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

#endif /* PHOTON_SCHEDULER_H */
