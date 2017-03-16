#include "local_scheduler_client.h"

#include "common_protocol.h"
#include "format/local_scheduler_generated.h"

#include "common/io.h"
#include "common/task.h"
#include <stdlib.h>

LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket,
    ActorID actor_id,
    bool is_worker) {
  LocalSchedulerConnection *result =
      (LocalSchedulerConnection *) malloc(sizeof(LocalSchedulerConnection));
  result->conn = connect_ipc_sock_retry(local_scheduler_socket, -1, -1);

  if (is_worker) {
    /* If we are a worker, register with the local scheduler.
     * NOTE(swang): If the local scheduler exits and we are registered as a
     * worker, we will get killed. */
    flatbuffers::FlatBufferBuilder fbb;
    auto message =
        CreateRegisterWorkerInfo(fbb, to_flatbuf(fbb, actor_id), getpid());
    fbb.Finish(message);
    /* Register the process ID with the local scheduler. */
    int success = write_message(result->conn, MessageType_RegisterWorkerInfo,
                                fbb.GetSize(), fbb.GetBufferPointer());
    CHECKM(success == 0, "Unable to register worker with local scheduler");
  }

  return result;
}

void LocalSchedulerConnection_free(LocalSchedulerConnection *conn) {
  close(conn->conn);
  free(conn);
}

void local_scheduler_log_event(LocalSchedulerConnection *conn,
                               uint8_t *key,
                               int64_t key_length,
                               uint8_t *value,
                               int64_t value_length) {
  flatbuffers::FlatBufferBuilder fbb;
  auto key_string = fbb.CreateString((char *) key, key_length);
  auto value_string = fbb.CreateString((char *) value, value_length);
  auto message = CreateEventLogMessage(fbb, key_string, value_string);
  fbb.Finish(message);
  write_message(conn->conn, MessageType_EventLogMessage, fbb.GetSize(),
                fbb.GetBufferPointer());
}

void local_scheduler_submit(LocalSchedulerConnection *conn,
                            TaskSpec *task,
                            int64_t task_size) {
  write_message(conn->conn, MessageType_SubmitTask, task_size,
                (uint8_t *) task);
}

TaskSpec *local_scheduler_get_task(LocalSchedulerConnection *conn,
                                   int64_t *task_size) {
  write_message(conn->conn, MessageType_GetTask, 0, NULL);
  int64_t type;
  uint8_t *message;
  /* Receive a task from the local scheduler. This will block until the local
   * scheduler gives this client a task. */
  read_message(conn->conn, &type, task_size, &message);
  CHECK(type == MessageType_ExecuteTask);
  TaskSpec *task = (TaskSpec *) message;
  return task;
}

void local_scheduler_task_done(LocalSchedulerConnection *conn) {
  write_message(conn->conn, MessageType_TaskDone, 0, NULL);
}

void local_scheduler_reconstruct_object(LocalSchedulerConnection *conn,
                                        ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateReconstructObject(fbb, to_flatbuf(fbb, object_id));
  fbb.Finish(message);
  write_message(conn->conn, MessageType_ReconstructObject, fbb.GetSize(),
                fbb.GetBufferPointer());
  /* TODO(swang): Propagate the error. */
}

void local_scheduler_log_message(LocalSchedulerConnection *conn) {
  write_message(conn->conn, MessageType_EventLogMessage, 0, NULL);
}

void local_scheduler_notify_unblocked(LocalSchedulerConnection *conn) {
  write_message(conn->conn, MessageType_NotifyUnblocked, 0, NULL);
}
