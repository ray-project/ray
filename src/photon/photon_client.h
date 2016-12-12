#ifndef PHOTON_CLIENT_H
#define PHOTON_CLIENT_H

#include "common/task.h"
#include "photon.h"

typedef struct {
  /* File descriptor of the Unix domain socket that connects to photon. */
  int conn;
} photon_conn;

/**
 * Connect to the local scheduler.
 *
 * @param photon_socket The name of the socket to use to connect to the local
 *        scheduler.
 * @return The connection information.
 */
photon_conn *photon_connect(const char *photon_socket);

/**
 * Disconnect from the local scheduler.
 *
 * @param conn Photon connection information returned by photon_connect.
 *
 * @return Void.
 */
void photon_disconnect(photon_conn *conn);

/**
 * Submit a task to the local scheduler.
 *
 * @param conn The connection information.
 * @param task The address of the task to submit.
 * @return Void.
 */
void photon_submit(photon_conn *conn, task_spec *task);

/**
 * Get next task for this client. This will block until the scheduler assigns
 * a task to this worker. This allocates and returns a task, and so the task
 * must be freed by the caller.
 *
 * @todo When does this actually get freed?
 *
 * @param conn The connection information.
 * @return The address of the assigned task.
 */
task_spec *photon_get_task(photon_conn *conn);

/**
 * Tell the local scheduler that the client has finished executing a task.
 *
 * @param conn The connection information.
 * @return Void.
 */
void photon_task_done(photon_conn *conn);

/**
 * Tell the local scheduler to reconstruct an object.
 *
 * @param conn The connection information.
 * @param object_id The ID of the object to reconstruct.
 * @return Void.
 */
void photon_reconstruct_object(photon_conn *conn, object_id object_id);

/**
 * Send a log message to the local scheduler.
 *
 * @param conn The connection information.
 * @return Void.
 */
void photon_log_message(photon_conn *conn);

#endif
