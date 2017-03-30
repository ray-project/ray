#include "local_scheduler_client.h"

#include "common_protocol.h"
#include "format/local_scheduler_generated.h"

#include "common/io.h"
#include "common/task.h"
#include <stdlib.h>

LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket,
    ActorID actor_id,
    bool is_worker,
    int64_t num_gpus) {
  LocalSchedulerConnection *result = new LocalSchedulerConnection;
  result->conn = connect_ipc_sock_retry(local_scheduler_socket, -1, -1);
  result->actor_id = actor_id;

  if (is_worker) {
    /* If we are a worker, register with the local scheduler.
     * NOTE(swang): If the local scheduler exits and we are registered as a
     * worker, we will get killed. */
    flatbuffers::FlatBufferBuilder fbb;
    auto message = CreateRegisterWorkerInfo(
        fbb, to_flatbuf(fbb, result->actor_id), getpid(), num_gpus);
    fbb.Finish(message);
    /* Register the process ID with the local scheduler. */
    int success = write_message(result->conn, MessageType_RegisterWorkerInfo,
                                fbb.GetSize(), fbb.GetBufferPointer());
    CHECKM(success == 0, "Unable to register worker with local scheduler");

    /* Get the response from the local scheduler. */
    int64_t type;
    int64_t reply_size;
    uint8_t *reply;
    /* Receive a reply from the local scheduler. This will block until the local
     * scheduler replies to the client. If this worker is an actor that requires
     * some GPUs, this will block until that number of GPUs is available. */
    read_message(result->conn, &type, &reply_size, &reply);
    if (type == DISCONNECT_CLIENT) {
      LOG_WARN("Exiting because local scheduler closed connection.");
      exit(1);
    }
    CHECK(type == MessageType_RegisterWorkerReply);
    /* Parse the flatbuffer payload and get the GPU IDs allocated for this
     * worker. */
    auto reply_message = flatbuffers::GetRoot<RegisterWorkerReply>(reply);
    for (int i = 0; i < reply_message->gpu_ids()->size(); ++i) {
      result->gpu_ids.push_back(reply_message->gpu_ids()->Get(i));
    }
    /* If the worker is not an actor, there should not be any GPU IDs here. */
    if (ActorID_equal(actor_id, NIL_ACTOR_ID)) {
      CHECK(reply_message->gpu_ids()->size() == 0);
    }
  }
  /* Return the connection object. */
  return result;
}

void LocalSchedulerConnection_free(LocalSchedulerConnection *conn) {
  close(conn->conn);
  delete conn;
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
  uint8_t *task_reply;
  int64_t task_reply_size;
  /* Receive a task from the local scheduler. This will block until the local
   * scheduler gives this client a task. */
  read_message(conn->conn, &type, &task_reply_size, &task_reply);
  if (type == DISCONNECT_CLIENT) {
    LOG_WARN("Exiting because local scheduler closed connection.");
    exit(1);
  }
  CHECK(type == MessageType_ExecuteTask);

  /* Parse the task reply as a flatbuffer object. */
  auto message = flatbuffers::GetRoot<GetTaskReply>(task_reply);

  /* Set the GPU IDs for this task. We only do this for non-actor tasks because
   * for actors the GPUs are associated with the actor itself and not with the
   * actor methods. */
  if (ActorID_equal(conn->actor_id, NIL_ACTOR_ID)) {
    conn->gpu_ids.clear();
    for (int i = 0; i < message->gpu_ids()->size(); ++i) {
      conn->gpu_ids.push_back(message->gpu_ids()->Get(i));
    }
  }

  /* Parse the TaskSpec from the reply. */
  TaskSpec *task_spec = (TaskSpec *) message->task_spec()->data();
  *task_size = message->task_spec()->size();
  /* Copy the TaskSpec and give ownership of the caller. */
  TaskSpec *task_spec_copy = (TaskSpec *) malloc(*task_size);
  memcpy(task_spec_copy, task_spec, *task_size);

  /* Free the task reply that was allocated by local_scheduler_get_task. */
  free(task_reply);

  return task_spec_copy;
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

void local_scheduler_put_object(LocalSchedulerConnection *conn,
                                TaskID task_id,
                                ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePutObject(fbb, to_flatbuf(fbb, task_id),
                                 to_flatbuf(fbb, object_id));
  fbb.Finish(message);

  write_message(conn->conn, MessageType_PutObject, fbb.GetSize(),
                fbb.GetBufferPointer());
}
