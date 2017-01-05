#include "photon_client.h"

#include "common/io.h"
#include "common/task.h"
#include <stdlib.h>

photon_conn *photon_connect(const char *photon_socket) {
  photon_conn *result = malloc(sizeof(photon_conn));
  result->conn = connect_ipc_sock(photon_socket);
  return result;
}

void photon_disconnect(photon_conn *conn) {
  close(conn->conn);
  free(conn);
}

void photon_log_event(photon_conn *conn,
                      uint8_t *key_name,
                      int64_t key_name_length,
                      uint8_t *payload,
                      int64_t payload_length) {
  int64_t message_length = sizeof(key_name_length) + sizeof(payload_length) +
                           key_name_length + payload_length;
  uint8_t *message = malloc(message_length);
  int64_t offset = 0;
  memcpy(&message[offset], &key_name_length, sizeof(key_name_length));
  offset += sizeof(key_name_length);
  memcpy(&message[offset], &payload_length, sizeof(payload_length));
  offset += sizeof(payload_length);
  memcpy(&message[offset], key_name, key_name_length);
  offset += key_name_length;
  memcpy(&message[offset], payload, payload_length);
  offset += payload_length;
  CHECK(offset == message_length);
  write_message(conn->conn, EVENT_LOG_MESSAGE, message_length, message);
  free(message);
}

void photon_submit(photon_conn *conn, task_spec *task) {
  write_message(conn->conn, SUBMIT_TASK, task_spec_size(task),
                (uint8_t *) task);
}

task_spec *photon_get_task(photon_conn *conn) {
  write_message(conn->conn, GET_TASK, 0, NULL);
  int64_t type;
  int64_t length;
  uint8_t *message;
  /* Receive a task from the local scheduler. This will block until the local
   * scheduler gives this client a task. */
  read_message(conn->conn, &type, &length, &message);
  CHECK(type == EXECUTE_TASK);
  task_spec *task = (task_spec *) message;
  CHECK(length == task_spec_size(task));
  return task;
}

void photon_task_done(photon_conn *conn) {
  write_message(conn->conn, TASK_DONE, 0, NULL);
}

void photon_reconstruct_object(photon_conn *conn, object_id object_id) {
  write_message(conn->conn, RECONSTRUCT_OBJECT, sizeof(object_id),
                (uint8_t *) &object_id);
}

void photon_log_message(photon_conn *conn) {
  write_message(conn->conn, LOG_MESSAGE, 0, NULL);
}
