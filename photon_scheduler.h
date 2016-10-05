#ifndef PHOTON_SCHEDULER_H
#define PHOTON_SCHEDULER_H

#include "task.h"

typedef struct local_scheduler_state local_scheduler_state;

/**
 * Establish a connection to a new client.
 *
 * @param loop Event loop of the local scheduler.
 * @param listener_socket Socket the local scheduler is listening on for new
 *                        client requests.
 * @param context State of the local scheduler.
 * @param events Flag for events that are available on the listener socket.
 */
void new_client_connection(event_loop *loop, int listener_sock, void *context,
                           int events);

/**
 * Assign a task to a worker.
 *
 * @param s State of the local scheduler.
 * @param client_sock Socket by which the worker is connected.
 */
void handle_get_task(local_scheduler_state *s, int client_sock);

/**
 * Handle incoming submit request by a worker.
 *
 * @param s State of the local scheduler.
 * @param task Task specification of the task to be submitted.
 */
void handle_submit_task(local_scheduler_state *s, task_spec *task);

#endif /* PHOTON_SCHEDULER_H */
