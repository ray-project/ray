#include "raylet_client.h"

#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include "ray/common/common_protocol.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/util/logging.h"

using MessageType = ray::protocol::MessageType;

// TODO(rkn): The io methods below should be removed.
int connect_ipc_sock(const std::string &socket_pathname) {
  struct sockaddr_un socket_address;
  int socket_fd;

  socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    RAY_LOG(ERROR) << "socket() failed for pathname " << socket_pathname;
    return -1;
  }

  memset(&socket_address, 0, sizeof(socket_address));
  socket_address.sun_family = AF_UNIX;
  if (socket_pathname.length() + 1 > sizeof(socket_address.sun_path)) {
    RAY_LOG(ERROR) << "Socket pathname is too long.";
    close(socket_fd);
    return -1;
  }
  strncpy(socket_address.sun_path, socket_pathname.c_str(), socket_pathname.length() + 1);

  if (connect(socket_fd, (struct sockaddr *)&socket_address, sizeof(socket_address)) !=
      0) {
    close(socket_fd);
    return -1;
  }
  return socket_fd;
}

int read_bytes(int socket_fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  // Termination condition: EOF or read 'length' bytes total.
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    nbytes = read(socket_fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return -1;  // Errno will be set.
    } else if (0 == nbytes) {
      // Encountered early EOF.
      return -1;
    }
    RAY_CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }
  return 0;
}

int write_bytes(int socket_fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    // While we haven't written the whole message, write to the file
    // descriptor, advance the cursor, and decrease the amount left to write.
    nbytes = write(socket_fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return -1;  // Errno will be set.
    } else if (0 == nbytes) {
      // Encountered early EOF.
      return -1;
    }
    RAY_CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }
  return 0;
}

RayletConnection::RayletConnection(const std::string &raylet_socket, int num_retries,
                                   int64_t timeout) {
  // Pick the default values if the user did not specify.
  if (num_retries < 0) {
    num_retries = RayConfig::instance().num_connect_attempts();
  }
  if (timeout < 0) {
    timeout = RayConfig::instance().connect_timeout_milliseconds();
  }
  RAY_CHECK(!raylet_socket.empty());
  conn_ = -1;
  for (int num_attempts = 0; num_attempts < num_retries; ++num_attempts) {
    conn_ = connect_ipc_sock(raylet_socket);
    if (conn_ >= 0) break;
    if (num_attempts > 0) {
      RAY_LOG(ERROR) << "Retrying to connect to socket for pathname " << raylet_socket
                     << " (num_attempts = " << num_attempts
                     << ", num_retries = " << num_retries << ")";
    }
    // Sleep for timeout milliseconds.
    usleep(timeout * 1000);
  }
  // If we could not connect to the socket, exit.
  if (conn_ == -1) {
    RAY_LOG(FATAL) << "Could not connect to socket " << raylet_socket;
  }
}

ray::Status RayletConnection::Disconnect() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateDisconnectClient(fbb);
  fbb.Finish(message);
  auto status = WriteMessage(MessageType::IntentionalDisconnectClient, &fbb);
  // Don't be too strict for disconnection errors.
  // Just create logs and prevent it from crash.
  if (!status.ok()) {
    RAY_LOG(ERROR) << status.ToString()
                   << " [RayletClient] Failed to disconnect from raylet.";
  }
  return ray::Status::OK();
}

ray::Status RayletConnection::ReadMessage(MessageType type,
                                          std::unique_ptr<uint8_t[]> &message) {
  int64_t cookie;
  int64_t type_field;
  int64_t length;
  int closed = read_bytes(conn_, (uint8_t *)&cookie, sizeof(cookie));
  if (closed) goto disconnected;
  RAY_CHECK(cookie == RayConfig::instance().ray_cookie());
  closed = read_bytes(conn_, (uint8_t *)&type_field, sizeof(type_field));
  if (closed) goto disconnected;
  closed = read_bytes(conn_, (uint8_t *)&length, sizeof(length));
  if (closed) goto disconnected;
  message = std::unique_ptr<uint8_t[]>(new uint8_t[length]);
  closed = read_bytes(conn_, message.get(), length);
  if (closed) {
    // Handle the case in which the socket is closed.
    message.reset(nullptr);
  disconnected:
    message = nullptr;
    type_field = static_cast<int64_t>(MessageType::DisconnectClient);
    length = 0;
  }
  if (type_field == static_cast<int64_t>(MessageType::DisconnectClient)) {
    return ray::Status::IOError("[RayletClient] Raylet connection closed.");
  }
  if (type_field != static_cast<int64_t>(type)) {
    return ray::Status::TypeError(
        std::string("[RayletClient] Raylet connection corrupted. ") +
        "Expected message type: " + std::to_string(static_cast<int64_t>(type)) +
        "; got message type: " + std::to_string(type_field) +
        ". Check logs or dmesg for previous errors.");
  }
  return ray::Status::OK();
}

ray::Status RayletConnection::WriteMessage(MessageType type,
                                           flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(write_mutex_);
  int64_t cookie = RayConfig::instance().ray_cookie();
  int64_t length = fbb ? fbb->GetSize() : 0;
  uint8_t *bytes = fbb ? fbb->GetBufferPointer() : nullptr;
  int64_t type_field = static_cast<int64_t>(type);
  auto io_error = ray::Status::IOError("[RayletClient] Connection closed unexpectedly.");
  int closed;
  closed = write_bytes(conn_, (uint8_t *)&cookie, sizeof(cookie));
  if (closed) return io_error;
  closed = write_bytes(conn_, (uint8_t *)&type_field, sizeof(type_field));
  if (closed) return io_error;
  closed = write_bytes(conn_, (uint8_t *)&length, sizeof(length));
  if (closed) return io_error;
  closed = write_bytes(conn_, bytes, length * sizeof(char));
  if (closed) return io_error;
  return ray::Status::OK();
}

ray::Status RayletConnection::AtomicRequestReply(
    MessageType request_type, MessageType reply_type,
    std::unique_ptr<uint8_t[]> &reply_message, flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(mutex_);
  auto status = WriteMessage(request_type, fbb);
  if (!status.ok()) return status;
  return ReadMessage(reply_type, reply_message);
}

RayletClient::RayletClient(std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client)
    : grpc_client_(std::move(grpc_client)) {}

RayletClient::RayletClient(std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client,
                           const std::string &raylet_socket, const WorkerID &worker_id,
                           bool is_worker, const JobID &job_id, const Language &language,
                           ClientID *raylet_id, int port)
    : grpc_client_(std::move(grpc_client)), worker_id_(worker_id), job_id_(job_id) {
  // For C++14, we could use std::make_unique
  conn_ = std::unique_ptr<RayletConnection>(new RayletConnection(raylet_socket, -1, -1));

  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateRegisterClientRequest(
      fbb, is_worker, to_flatbuf(fbb, worker_id), getpid(), to_flatbuf(fbb, job_id),
      language, port);
  fbb.Finish(message);
  // Register the process ID with the raylet.
  // NOTE(swang): If raylet exits and we are registered as a worker, we will get killed.
  std::unique_ptr<uint8_t[]> reply;
  auto status = conn_->AtomicRequestReply(MessageType::RegisterClientRequest,
                                          MessageType::RegisterClientReply, reply, &fbb);
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Unable to register worker with raylet.");
  auto reply_message =
      flatbuffers::GetRoot<ray::protocol::RegisterClientReply>(reply.get());
  *raylet_id = ClientID::FromBinary(reply_message->raylet_id()->str());
}

ray::Status RayletClient::SubmitTask(const ray::TaskSpecification &task_spec) {
  ray::rpc::SubmitTaskRequest request;
  for (size_t i = 0; i < task_spec.NumArgs(); i++) {
    if (task_spec.ArgByRef(i)) {
      for (size_t j = 0; j < task_spec.ArgIdCount(i); j++) {
        RAY_CHECK(!task_spec.ArgId(i, j).IsDirectCallType())
            << "Passing direct call objects to non-direct tasks is not allowed.";
      }
    }
  }
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  return grpc_client_->SubmitTask(request, /*callback=*/nullptr);
}

ray::Status RayletClient::TaskDone() {
  return conn_->WriteMessage(MessageType::TaskDone);
}

ray::Status RayletClient::FetchOrReconstruct(const std::vector<ObjectID> &object_ids,
                                             bool fetch_only,
                                             const TaskID &current_task_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ids_message = to_flatbuf(fbb, object_ids);
  auto message = ray::protocol::CreateFetchOrReconstruct(
      fbb, object_ids_message, fetch_only, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  auto status = conn_->WriteMessage(MessageType::FetchOrReconstruct, &fbb);
  return status;
}

ray::Status RayletClient::NotifyUnblocked(const TaskID &current_task_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      ray::protocol::CreateNotifyUnblocked(fbb, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyUnblocked, &fbb);
}

ray::Status RayletClient::NotifyDirectCallTaskBlocked() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateNotifyDirectCallTaskBlocked(fbb);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyDirectCallTaskBlocked, &fbb);
}

ray::Status RayletClient::NotifyDirectCallTaskUnblocked() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateNotifyDirectCallTaskUnblocked(fbb);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyDirectCallTaskUnblocked, &fbb);
}

ray::Status RayletClient::Wait(const std::vector<ObjectID> &object_ids, int num_returns,
                               int64_t timeout_milliseconds, bool wait_local,
                               const TaskID &current_task_id, WaitResultPair *result) {
  // Write request.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateWaitRequest(
      fbb, to_flatbuf(fbb, object_ids), num_returns, timeout_milliseconds, wait_local,
      to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  std::unique_ptr<uint8_t[]> reply;
  auto status = conn_->AtomicRequestReply(MessageType::WaitRequest,
                                          MessageType::WaitReply, reply, &fbb);
  if (!status.ok()) return status;
  // Parse the flatbuffer object.
  auto reply_message = flatbuffers::GetRoot<ray::protocol::WaitReply>(reply.get());
  auto found = reply_message->found();
  for (size_t i = 0; i < found->size(); i++) {
    ObjectID object_id = ObjectID::FromBinary(found->Get(i)->str());
    result->first.push_back(object_id);
  }
  auto remaining = reply_message->remaining();
  for (size_t i = 0; i < remaining->size(); i++) {
    ObjectID object_id = ObjectID::FromBinary(remaining->Get(i)->str());
    result->second.push_back(object_id);
  }
  return ray::Status::OK();
}

ray::Status RayletClient::WaitForDirectActorCallArgs(
    const std::vector<ObjectID> &object_ids, int64_t tag) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateWaitForDirectActorCallArgsRequest(
      fbb, to_flatbuf(fbb, object_ids), tag);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::WaitForDirectActorCallArgsRequest, &fbb);
}

ray::Status RayletClient::PushError(const ray::JobID &job_id, const std::string &type,
                                    const std::string &error_message, double timestamp) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreatePushErrorRequest(
      fbb, to_flatbuf(fbb, job_id), fbb.CreateString(type),
      fbb.CreateString(error_message), timestamp);
  fbb.Finish(message);

  return conn_->WriteMessage(MessageType::PushErrorRequest, &fbb);
}

ray::Status RayletClient::PushProfileEvents(const ProfileTableData &profile_events) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fbb.CreateString(profile_events.SerializeAsString());
  fbb.Finish(message);

  auto status = conn_->WriteMessage(MessageType::PushProfileEventsRequest, &fbb);
  // Don't be too strict for profile errors. Just create logs and prevent it from crash.
  if (!status.ok()) {
    RAY_LOG(ERROR) << status.ToString()
                   << " [RayletClient] Failed to push profile events.";
  }
  return ray::Status::OK();
}

ray::Status RayletClient::FreeObjects(const std::vector<ray::ObjectID> &object_ids,
                                      bool local_only, bool delete_creating_tasks) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateFreeObjectsRequest(
      fbb, local_only, delete_creating_tasks, to_flatbuf(fbb, object_ids));
  fbb.Finish(message);

  auto status = conn_->WriteMessage(MessageType::FreeObjectsInObjectStoreRequest, &fbb);
  return status;
}

ray::Status RayletClient::PrepareActorCheckpoint(const ActorID &actor_id,
                                                 ActorCheckpointID &checkpoint_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      ray::protocol::CreatePrepareActorCheckpointRequest(fbb, to_flatbuf(fbb, actor_id));
  fbb.Finish(message);

  std::unique_ptr<uint8_t[]> reply;
  auto status =
      conn_->AtomicRequestReply(MessageType::PrepareActorCheckpointRequest,
                                MessageType::PrepareActorCheckpointReply, reply, &fbb);
  if (!status.ok()) return status;
  auto reply_message =
      flatbuffers::GetRoot<ray::protocol::PrepareActorCheckpointReply>(reply.get());
  checkpoint_id = ActorCheckpointID::FromBinary(reply_message->checkpoint_id()->str());
  return ray::Status::OK();
}

ray::Status RayletClient::NotifyActorResumedFromCheckpoint(
    const ActorID &actor_id, const ActorCheckpointID &checkpoint_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateNotifyActorResumedFromCheckpoint(
      fbb, to_flatbuf(fbb, actor_id), to_flatbuf(fbb, checkpoint_id));
  fbb.Finish(message);

  return conn_->WriteMessage(MessageType::NotifyActorResumedFromCheckpoint, &fbb);
}

ray::Status RayletClient::SetResource(const std::string &resource_name,
                                      const double capacity,
                                      const ray::ClientID &client_Id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateSetResourceRequest(
      fbb, fbb.CreateString(resource_name), capacity, to_flatbuf(fbb, client_Id));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::SetResourceRequest, &fbb);
}

ray::Status RayletClient::ReportActiveObjectIDs(
    const std::unordered_set<ObjectID> &object_ids) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      ray::protocol::CreateReportActiveObjectIDs(fbb, to_flatbuf(fbb, object_ids));
  fbb.Finish(message);

  return conn_->WriteMessage(MessageType::ReportActiveObjectIDs, &fbb);
}

ray::Status RayletClient::RequestWorkerLease(
    const ray::TaskSpecification &resource_spec,
    const ray::rpc::ClientCallback<ray::rpc::WorkerLeaseReply> &callback) {
  ray::rpc::WorkerLeaseRequest request;
  request.mutable_resource_spec()->CopyFrom(resource_spec.GetMessage());
  return grpc_client_->RequestWorkerLease(request, callback);
}

ray::Status RayletClient::ReturnWorker(int worker_port, bool disconnect_worker) {
  ray::rpc::ReturnWorkerRequest request;
  request.set_worker_port(worker_port);
  request.set_disconnect_worker(disconnect_worker);
  return grpc_client_->ReturnWorker(
      request, [](const ray::Status &status, const ray::rpc::ReturnWorkerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Error returning worker: " << status;
        }
      });
}
