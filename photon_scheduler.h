#ifndef PHOTON_SCHEDULER_H
#define PHOTON_SCHEDULER_H

#include "task.h"

typedef struct local_scheduler_state local_scheduler_state;

/* Establish a connection to a new client. */
void new_client_connection(event_loop *loop, int listener_sock, void *context,
                           int events);

/* Assign a task to a worker. */
void handle_get_task(local_scheduler_state *s, int client_sock);

/* Handle incoming submit request by a worker. */
void handle_submit_task(local_scheduler_state *s, task_spec *task);

#endif /* PHOTON_SCHEDULER_H */
