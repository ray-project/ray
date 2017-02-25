#include "photon_client.h"

#include "common/io.h"
#include "common/task.h"
#include <stdlib.h>

photon_conn *photon_connect(const char *photon_socket, ActorID actor_id) {
  photon_conn *result = malloc(sizeof(photon_conn));
  result->conn = connect_ipc_sock_retry(photon_socket, -1, -1);
  register_worker_info info;
  memset(&info, 0, sizeof(info));
  /* Register the process ID with the local scheduler. */
  info.worker_pid = getpid();
  info.actor_id = actor_id;
  int success = write_message(result->conn, REGISTER_WORKER_INFO, sizeof(info),
                              (uint8_t *) &info);
  CHECKM(success == 0, "Unable to register worker with local scheduler");
  return result;
}

void photon_disconnect(photon_conn *conn) {
  close(conn->conn);
  free(conn);
}

void photon_log_event(photon_conn *conn,
                      uint8_t *key,
                      int64_t key_length,
                      uint8_t *value,
                      int64_t value_length) {
  int64_t message_length =
      sizeof(key_length) + sizeof(value_length) + key_length + value_length;
  uint8_t *message = malloc(message_length);
  int64_t offset = 0;
  memcpy(&message[offset], &key_length, sizeof(key_length));
  offset += sizeof(key_length);
  memcpy(&message[offset], &value_length, sizeof(value_length));
  offset += sizeof(value_length);
  memcpy(&message[offset], key, key_length);
  offset += key_length;
  memcpy(&message[offset], value, value_length);
  offset += value_length;
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

void photon_reconstruct_object(photon_conn *conn, ObjectID object_id) {
  write_message(conn->conn, RECONSTRUCT_OBJECT, sizeof(object_id),
                (uint8_t *) &object_id);
}

void photon_log_message(photon_conn *conn) {
  write_message(conn->conn, LOG_MESSAGE, 0, NULL);
}

void photon_notify_unblocked(photon_conn *conn) {
  write_message(conn->conn, NOTIFY_UNBLOCKED, 0, NULL);
}
