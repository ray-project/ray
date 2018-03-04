#include "local_scheduler_client.h"

#include "common_protocol.h"
#include "format/local_scheduler_generated.h"

#include "common/io.h"
#include "common/task.h"
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket,
    UniqueID client_id,
    ActorID actor_id,
    bool is_worker,
    int64_t num_gpus) {
  LocalSchedulerConnection *result = new LocalSchedulerConnection();
  result->conn = connect_ipc_sock_retry(local_scheduler_socket, -1, -1);
  result->actor_id = actor_id;

  /* Register with the local scheduler.
   * NOTE(swang): If the local scheduler exits and we are registered as a
   * worker, we will get killed. */
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateRegisterClientRequest(
      fbb, is_worker, to_flatbuf(fbb, client_id),
      to_flatbuf(fbb, result->actor_id), getpid(), num_gpus);
  fbb.Finish(message);
  /* Register the process ID with the local scheduler. */
  int success = write_message(result->conn, MessageType_RegisterClientRequest,
                              fbb.GetSize(), fbb.GetBufferPointer());
  RAY_CHECK(success == 0) << "Unable to register worker with local scheduler";

  /* Wait for a confirmation from the local scheduler. */
  int64_t type;
  int64_t reply_size;
  uint8_t *reply;
  read_message(result->conn, &type, &reply_size, &reply);
  if (type == DISCONNECT_CLIENT) {
    RAY_LOG(DEBUG) << "Exiting because local scheduler closed connection.";
    exit(1);
  }
  RAY_CHECK(type == MessageType_RegisterClientReply);

  /* Parse the reply object. */
  auto reply_message = flatbuffers::GetRoot<RegisterClientReply>(reply);
  for (size_t i = 0; i < reply_message->gpu_ids()->size(); ++i) {
    result->gpu_ids.push_back(reply_message->gpu_ids()->Get(i));
  }
  /* If the worker is not an actor, there should not be any GPU IDs here. */
  if (ActorID_equal(result->actor_id, ActorID::nil())) {
    RAY_CHECK(reply_message->gpu_ids()->size() == 0);
  }

  free(reply);

  return result;
}

void LocalSchedulerConnection_free(LocalSchedulerConnection *conn) {
  close(conn->conn);
  delete conn;
}

void local_scheduler_disconnect_client(LocalSchedulerConnection *conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateDisconnectClient(fbb);
  fbb.Finish(message);
  write_message(conn->conn, MessageType_DisconnectClient, fbb.GetSize(),
                fbb.GetBufferPointer());
}

void local_scheduler_log_event(LocalSchedulerConnection *conn,
                               uint8_t *key,
                               int64_t key_length,
                               uint8_t *value,
                               int64_t value_length,
                               double timestamp) {
  flatbuffers::FlatBufferBuilder fbb;
  auto key_string = fbb.CreateString((char *) key, key_length);
  auto value_string = fbb.CreateString((char *) value, value_length);
  auto message =
      CreateEventLogMessage(fbb, key_string, value_string, timestamp);
  fbb.Finish(message);
  write_message(conn->conn, MessageType_EventLogMessage, fbb.GetSize(),
                fbb.GetBufferPointer());
}

void local_scheduler_submit(LocalSchedulerConnection *conn,
                            TaskExecutionSpec &execution_spec) {
  flatbuffers::FlatBufferBuilder fbb;
  auto execution_dependencies =
      to_flatbuf(fbb, execution_spec.ExecutionDependencies());
  auto task_spec = fbb.CreateString((char *) execution_spec.Spec(),
                                    execution_spec.SpecSize());
  auto message =
      CreateSubmitTaskRequest(fbb, execution_dependencies, task_spec);
  fbb.Finish(message);
  write_message(conn->conn, MessageType_SubmitTask, fbb.GetSize(),
                fbb.GetBufferPointer());
}

TaskSpec *local_scheduler_get_task(LocalSchedulerConnection *conn,
                                   int64_t *task_size) {
  write_message(conn->conn, MessageType_GetTask, 0, NULL);
  int64_t type;
  int64_t reply_size;
  uint8_t *reply;
  /* Receive a task from the local scheduler. This will block until the local
   * scheduler gives this client a task. */
  read_message(conn->conn, &type, &reply_size, &reply);
  if (type == DISCONNECT_CLIENT) {
    RAY_LOG(WARNING) << "Exiting because local scheduler closed connection.";
    exit(1);
  }
  RAY_CHECK(type == MessageType_ExecuteTask);

  /* Parse the flatbuffer object. */
  auto reply_message = flatbuffers::GetRoot<GetTaskReply>(reply);

  /* Set the GPU IDs for this task. We only do this for non-actor tasks because
   * for actors the GPUs are associated with the actor itself and not with the
   * actor methods. */
  if (ActorID_equal(conn->actor_id, ActorID::nil())) {
    conn->gpu_ids.clear();
    for (size_t i = 0; i < reply_message->gpu_ids()->size(); ++i) {
      conn->gpu_ids.push_back(reply_message->gpu_ids()->Get(i));
    }
  }

  /* Create a copy of the task spec so we can free the reply. */
  *task_size = reply_message->task_spec()->size();
  TaskSpec *data = (TaskSpec *) reply_message->task_spec()->data();
  TaskSpec *spec = TaskSpec_copy(data, *task_size);
  /* Free the original message from the local scheduler. */
  free(reply);
  /* Return the copy of the task spec and pass ownership to the caller. */
  return spec;
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

const std::vector<uint8_t> local_scheduler_get_actor_frontier(
    LocalSchedulerConnection *conn,
    ActorID actor_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateGetActorFrontierRequest(fbb, to_flatbuf(fbb, actor_id));
  fbb.Finish(message);
  write_message(conn->conn, MessageType_GetActorFrontierRequest, fbb.GetSize(),
                fbb.GetBufferPointer());

  int64_t type;
  std::vector<uint8_t> reply;
  read_vector(conn->conn, &type, reply);
  if (type == DISCONNECT_CLIENT) {
    RAY_LOG(DEBUG) << "Exiting because local scheduler closed connection.";
    exit(1);
  }
  RAY_CHECK(type == MessageType_GetActorFrontierReply);
  return reply;
}

void local_scheduler_set_actor_frontier(LocalSchedulerConnection *conn,
                                        const std::vector<uint8_t> &frontier) {
  write_message(conn->conn, MessageType_SetActorFrontier, frontier.size(),
                const_cast<uint8_t *>(frontier.data()));
}
