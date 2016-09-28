#include "photon_client.h"

#include "common/io.h"
#include "common/task.h"
#include <stdlib.h>

photon_conn *photon_connect(const char *photon_socket) {
  photon_conn *result = malloc(sizeof(photon_conn));
  result->conn = connect_ipc_sock(photon_socket);
  return result;
}

void photon_submit(photon_conn *conn, task_spec *task) {
  write_message(conn->conn, SUBMIT_TASK, task_size(task), (uint8_t *)task);
}

void photon_task_done(photon_conn *conn) {
  write_message(conn->conn, TASK_DONE, 0, NULL);
}

void photon_disconnect(photon_conn *conn) {
  write_message(conn->conn, DISCONNECT_CLIENT, 0, NULL);
}

void photon_log_message(photon_conn *conn) {
  write_message(conn->conn, LOG_MESSAGE, 0, NULL);
}
