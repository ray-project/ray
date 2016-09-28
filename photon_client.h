#ifndef PHOTON_CLIENT_H
#define PHOTON_CLIENT_H

#include "common/task.h"
#include "photon.h"

typedef struct photon_conn_impl photon_conn;

/* Connect to the local scheduler. */
photon_conn *photon_connect(const char *photon_socket);

/* Submit a task to the local scheduler. */
void photon_submit(photon_conn *conn, task_spec *task);

/* Get next task for this client. */
task_spec *photon_get_task(photon_conn *conn);

/* Tell the local scheduler that the client has finished executing a task. */
void photon_task_done(photon_conn *conn);

/* Disconnect from the local scheduler. */
void photon_disconnect(photon_conn *conn);

/* Send a log message to the local scheduler. */
void photon_log_message(photon_conn *conn);

#endif
