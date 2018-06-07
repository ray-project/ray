#include "local_scheduler_client.h"

#include "common_protocol.h"
#include "format/local_scheduler_generated.h"
#include "ray/raylet/format/node_manager_generated.h"

#include "common/io.h"
#include "common/task.h"
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

using MessageType = ray::local_scheduler::protocol::MessageType;

LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket,
    UniqueID client_id,
    bool is_worker) {
  LocalSchedulerConnection *result = new LocalSchedulerConnection();
  result->conn = connect_ipc_sock_retry(local_scheduler_socket, -1, -1);

  /* Register with the local scheduler.
   * NOTE(swang): If the local scheduler exits and we are registered as a
   * worker, we will get killed. */
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::local_scheduler::protocol::CreateRegisterClientRequest(
      fbb, is_worker, to_flatbuf(fbb, client_id), getpid());
  fbb.Finish(message);
  /* Register the process ID with the local scheduler. */
  int success = write_message(
      result->conn, static_cast<int64_t>(MessageType::RegisterClientRequest),
      fbb.GetSize(), fbb.GetBufferPointer());
  RAY_CHECK(success == 0) << "Unable to register worker with local scheduler";

  return result;
}

void LocalSchedulerConnection_free(LocalSchedulerConnection *conn) {
  close(conn->conn);
  delete conn;
}

void local_scheduler_disconnect_client(LocalSchedulerConnection *conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::local_scheduler::protocol::CreateDisconnectClient(fbb);
  fbb.Finish(message);
  write_message(conn->conn, static_cast<int64_t>(MessageType::DisconnectClient),
                fbb.GetSize(), fbb.GetBufferPointer());
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
  auto message = ray::local_scheduler::protocol::CreateEventLogMessage(
      fbb, key_string, value_string, timestamp);
  fbb.Finish(message);
  write_message(conn->conn, static_cast<int64_t>(MessageType::EventLogMessage),
                fbb.GetSize(), fbb.GetBufferPointer());
}

void local_scheduler_submit(LocalSchedulerConnection *conn,
                            TaskExecutionSpec &execution_spec) {
  flatbuffers::FlatBufferBuilder fbb;
  auto execution_dependencies =
      to_flatbuf(fbb, execution_spec.ExecutionDependencies());
  auto task_spec =
      fbb.CreateString(reinterpret_cast<char *>(execution_spec.Spec()),
                       execution_spec.SpecSize());
  auto message = ray::local_scheduler::protocol::CreateSubmitTaskRequest(
      fbb, execution_dependencies, task_spec);
  fbb.Finish(message);
  write_message(conn->conn, static_cast<int64_t>(MessageType::SubmitTask),
                fbb.GetSize(), fbb.GetBufferPointer());
}

void local_scheduler_submit_raylet(
    LocalSchedulerConnection *conn,
    const std::vector<ObjectID> &execution_dependencies,
    ray::raylet::TaskSpecification task_spec) {
  flatbuffers::FlatBufferBuilder fbb;
  auto execution_dependencies_message = to_flatbuf(fbb, execution_dependencies);
  auto message = ray::local_scheduler::protocol::CreateSubmitTaskRequest(
      fbb, execution_dependencies_message, task_spec.ToFlatbuffer(fbb));
  fbb.Finish(message);
  write_message(conn->conn, static_cast<int64_t>(MessageType::SubmitTask),
                fbb.GetSize(), fbb.GetBufferPointer());
}

TaskSpec *local_scheduler_get_task(LocalSchedulerConnection *conn,
                                   int64_t *task_size) {
  write_message(conn->conn, static_cast<int64_t>(MessageType::GetTask), 0,
                NULL);
  int64_t type;
  int64_t reply_size;
  uint8_t *reply;
  /* Receive a task from the local scheduler. This will block until the local
   * scheduler gives this client a task. */
  read_message(conn->conn, &type, &reply_size, &reply);
  if (type == static_cast<int64_t>(CommonMessageType::DISCONNECT_CLIENT)) {
    RAY_LOG(DEBUG) << "Exiting because local scheduler closed connection.";
    exit(1);
  }
  RAY_CHECK(static_cast<MessageType>(type) == MessageType::ExecuteTask);

  /* Parse the flatbuffer object. */
  auto reply_message =
      flatbuffers::GetRoot<ray::local_scheduler::protocol::GetTaskReply>(reply);

  /* Create a copy of the task spec so we can free the reply. */
  *task_size = reply_message->task_spec()->size();
  TaskSpec *data = (TaskSpec *) reply_message->task_spec()->data();
  TaskSpec *spec = TaskSpec_copy(data, *task_size);

  // Set the GPU IDs for this task. We only do this for non-actor tasks because
  // for actors the GPUs are associated with the actor itself and not with the
  // actor methods. Note that this also processes GPUs for actor creation tasks.
  if (!TaskSpec_is_actor_task(spec)) {
    conn->gpu_ids.clear();
    for (size_t i = 0; i < reply_message->gpu_ids()->size(); ++i) {
      conn->gpu_ids.push_back(reply_message->gpu_ids()->Get(i));
    }
  }

  /* Free the original message from the local scheduler. */
  free(reply);
  /* Return the copy of the task spec and pass ownership to the caller. */
  return spec;
}

void local_scheduler_task_done(LocalSchedulerConnection *conn) {
  write_message(conn->conn, static_cast<int64_t>(MessageType::TaskDone), 0,
                NULL);
}

void local_scheduler_reconstruct_object(LocalSchedulerConnection *conn,
                                        ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::local_scheduler::protocol::CreateReconstructObject(
      fbb, to_flatbuf(fbb, object_id));
  fbb.Finish(message);
  write_message(conn->conn,
                static_cast<int64_t>(MessageType::ReconstructObject),
                fbb.GetSize(), fbb.GetBufferPointer());
  /* TODO(swang): Propagate the error. */
}

void local_scheduler_log_message(LocalSchedulerConnection *conn) {
  write_message(conn->conn, static_cast<int64_t>(MessageType::EventLogMessage),
                0, NULL);
}

void local_scheduler_notify_unblocked(LocalSchedulerConnection *conn) {
  write_message(conn->conn, static_cast<int64_t>(MessageType::NotifyUnblocked),
                0, NULL);
}

void local_scheduler_put_object(LocalSchedulerConnection *conn,
                                TaskID task_id,
                                ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::local_scheduler::protocol::CreatePutObject(
      fbb, to_flatbuf(fbb, task_id), to_flatbuf(fbb, object_id));
  fbb.Finish(message);

  write_message(conn->conn, static_cast<int64_t>(MessageType::PutObject),
                fbb.GetSize(), fbb.GetBufferPointer());
}

const std::vector<uint8_t> local_scheduler_get_actor_frontier(
    LocalSchedulerConnection *conn,
    ActorID actor_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::local_scheduler::protocol::CreateGetActorFrontierRequest(
      fbb, to_flatbuf(fbb, actor_id));
  fbb.Finish(message);
  write_message(conn->conn,
                static_cast<int64_t>(MessageType::GetActorFrontierRequest),
                fbb.GetSize(), fbb.GetBufferPointer());

  int64_t type;
  std::vector<uint8_t> reply;
  read_vector(conn->conn, &type, reply);
  if (static_cast<CommonMessageType>(type) ==
      CommonMessageType::DISCONNECT_CLIENT) {
    RAY_LOG(DEBUG) << "Exiting because local scheduler closed connection.";
    exit(1);
  }
  RAY_CHECK(static_cast<MessageType>(type) ==
            MessageType::GetActorFrontierReply);
  return reply;
}

void local_scheduler_set_actor_frontier(LocalSchedulerConnection *conn,
                                        const std::vector<uint8_t> &frontier) {
  write_message(conn->conn, static_cast<int64_t>(MessageType::SetActorFrontier),
                frontier.size(), const_cast<uint8_t *>(frontier.data()));
}

std::pair<std::vector<ObjectID>, std::vector<ObjectID>> local_scheduler_wait(
    LocalSchedulerConnection *conn,
    const std::vector<ObjectID> &object_ids,
    int num_returns,
    int64_t timeout_milliseconds,
    bool wait_local) {
  // Write request.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateWaitRequest(
      fbb, to_flatbuf(fbb, object_ids), num_returns, timeout_milliseconds,
      wait_local);
  fbb.Finish(message);
  write_message(conn->conn,
                static_cast<int64_t>(ray::protocol::MessageType::WaitRequest),
                fbb.GetSize(), fbb.GetBufferPointer());
  // Read result.
  int64_t type;
  int64_t reply_size;
  uint8_t *reply;
  read_message(conn->conn, &type, &reply_size, &reply);
  RAY_CHECK(static_cast<ray::protocol::MessageType>(type) ==
            ray::protocol::MessageType::WaitReply);
  auto reply_message = flatbuffers::GetRoot<ray::protocol::WaitReply>(reply);
  // Convert result.
  std::pair<std::vector<ObjectID>, std::vector<ObjectID>> result;
  auto found = reply_message->found();
  for (uint i = 0; i < found->size(); i++) {
    ObjectID object_id = ObjectID::from_binary(found->Get(i)->str());
    result.first.push_back(object_id);
  }
  auto remaining = reply_message->remaining();
  for (uint i = 0; i < remaining->size(); i++) {
    ObjectID object_id = ObjectID::from_binary(remaining->Get(i)->str());
    result.second.push_back(object_id);
  }
  /* Free the original message from the local scheduler. */
  free(reply);
  return result;
}
