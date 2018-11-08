#include "local_scheduler_client.h"

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
#include <unistd.h>

#include "ray/common/common_protocol.h"
#include "ray/ray_config.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/task_spec.h"
#include "ray/util/logging.h"

using MessageType = ray::protocol::MessageType;

// TODO(rkn): The io methods below should be removed.

int connect_ipc_sock(const char *socket_pathname) {
  struct sockaddr_un socket_address;
  int socket_fd;

  socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    RAY_LOG(ERROR) << "socket() failed for pathname " << socket_pathname;
    return -1;
  }

  memset(&socket_address, 0, sizeof(socket_address));
  socket_address.sun_family = AF_UNIX;
  if (strlen(socket_pathname) + 1 > sizeof(socket_address.sun_path)) {
    RAY_LOG(ERROR) << "Socket pathname is too long.";
    return -1;
  }
  strncpy(socket_address.sun_path, socket_pathname, strlen(socket_pathname) + 1);

  if (connect(socket_fd, (struct sockaddr *)&socket_address, sizeof(socket_address)) !=
      0) {
    close(socket_fd);
    return -1;
  }

  return socket_fd;
}

int connect_ipc_sock_retry(const char *socket_pathname, int num_retries,
                           int64_t timeout) {
  /* Pick the default values if the user did not specify. */
  if (num_retries < 0) {
    num_retries = RayConfig::instance().num_connect_attempts();
  }
  if (timeout < 0) {
    timeout = RayConfig::instance().connect_timeout_milliseconds();
  }

  RAY_CHECK(socket_pathname);
  int fd = -1;
  for (int num_attempts = 0; num_attempts < num_retries; ++num_attempts) {
    fd = connect_ipc_sock(socket_pathname);
    if (fd >= 0) {
      break;
    }
    if (num_attempts == 0) {
      RAY_LOG(ERROR) << "Connection to socket failed for pathname " << socket_pathname;
    }
    /* Sleep for timeout milliseconds. */
    usleep(timeout * 1000);
  }
  /* If we could not connect to the socket, exit. */
  if (fd == -1) {
    RAY_LOG(FATAL) << "Could not connect to socket " << socket_pathname;
  }
  return fd;
}

int read_bytes(int fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  /* Termination condition: EOF or read 'length' bytes total. */
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    nbytes = read(fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return -1; /* Errno will be set. */
    } else if (0 == nbytes) {
      /* Encountered early EOF. */
      return -1;
    }
    RAY_CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }

  return 0;
}

void read_message(int fd, int64_t *type, int64_t *length, uint8_t **bytes) {
  int64_t version;
  int closed = read_bytes(fd, (uint8_t *)&version, sizeof(version));
  if (closed) {
    goto disconnected;
  }
  RAY_CHECK(version == RayConfig::instance().ray_protocol_version());
  closed = read_bytes(fd, (uint8_t *)type, sizeof(*type));
  if (closed) {
    goto disconnected;
  }
  closed = read_bytes(fd, (uint8_t *)length, sizeof(*length));
  if (closed) {
    goto disconnected;
  }
  *bytes = (uint8_t *)malloc(*length * sizeof(uint8_t));
  closed = read_bytes(fd, *bytes, *length);
  if (closed) {
    free(*bytes);
    goto disconnected;
  }
  return;

disconnected:
  /* Handle the case in which the socket is closed. */
  *type = static_cast<int64_t>(MessageType::DisconnectClient);
  *length = 0;
  *bytes = NULL;
  return;
}

int write_bytes(int fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    /* While we haven't written the whole message, write to the file
     * descriptor, advance the cursor, and decrease the amount left to write. */
    nbytes = write(fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return -1; /* Errno will be set. */
    } else if (0 == nbytes) {
      /* Encountered early EOF. */
      return -1;
    }
    RAY_CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }

  return 0;
}

int do_write_message(int fd, int64_t type, int64_t length, uint8_t *bytes) {
  int64_t version = RayConfig::instance().ray_protocol_version();
  int closed;
  closed = write_bytes(fd, (uint8_t *)&version, sizeof(version));
  if (closed) {
    return closed;
  }
  closed = write_bytes(fd, (uint8_t *)&type, sizeof(type));
  if (closed) {
    return closed;
  }
  closed = write_bytes(fd, (uint8_t *)&length, sizeof(length));
  if (closed) {
    return closed;
  }
  closed = write_bytes(fd, bytes, length * sizeof(char));
  if (closed) {
    return closed;
  }
  return 0;
}

int write_message(int fd, int64_t type, int64_t length, uint8_t *bytes,
                  std::mutex *mutex) {
  if (mutex != NULL) {
    std::unique_lock<std::mutex> guard(*mutex);
    return do_write_message(fd, type, length, bytes);
  } else {
    return do_write_message(fd, type, length, bytes);
  }
}

LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket, const UniqueID &client_id, bool is_worker,
    const JobID &driver_id, const Language &language) {
  LocalSchedulerConnection *result = new LocalSchedulerConnection();
  result->conn = connect_ipc_sock_retry(local_scheduler_socket, -1, -1);

  /* Register with the local scheduler.
   * NOTE(swang): If the local scheduler exits and we are registered as a
   * worker, we will get killed. */
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateRegisterClientRequest(
      fbb, is_worker, to_flatbuf(fbb, client_id), getpid(), to_flatbuf(fbb, driver_id),
      language);
  fbb.Finish(message);
  /* Register the process ID with the local scheduler. */
  int success = write_message(
      result->conn, static_cast<int64_t>(MessageType::RegisterClientRequest),
      fbb.GetSize(), fbb.GetBufferPointer(), &result->write_mutex);
  RAY_CHECK(success == 0) << "Unable to register worker with local scheduler";

  return result;
}

void LocalSchedulerConnection_free(LocalSchedulerConnection *conn) {
  close(conn->conn);
  delete conn;
}

void local_scheduler_disconnect_client(LocalSchedulerConnection *conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateDisconnectClient(fbb);
  fbb.Finish(message);
  write_message(conn->conn,
                static_cast<int64_t>(MessageType::IntentionalDisconnectClient),
                fbb.GetSize(), fbb.GetBufferPointer(), &conn->write_mutex);
}

void local_scheduler_submit_raylet(LocalSchedulerConnection *conn,
                                   const std::vector<ObjectID> &execution_dependencies,
                                   const ray::raylet::TaskSpecification &task_spec) {
  flatbuffers::FlatBufferBuilder fbb;
  auto execution_dependencies_message = to_flatbuf(fbb, execution_dependencies);
  auto message = ray::protocol::CreateSubmitTaskRequest(
      fbb, execution_dependencies_message, task_spec.ToFlatbuffer(fbb));
  fbb.Finish(message);
  write_message(conn->conn, static_cast<int64_t>(MessageType::SubmitTask), fbb.GetSize(),
                fbb.GetBufferPointer(), &conn->write_mutex);
}

ray::raylet::TaskSpecification *local_scheduler_get_task_raylet(
    LocalSchedulerConnection *conn) {
  int64_t type;
  int64_t reply_size;
  uint8_t *reply;
  {
    std::unique_lock<std::mutex> guard(conn->mutex);
    write_message(conn->conn, static_cast<int64_t>(MessageType::GetTask), 0, NULL,
                  &conn->write_mutex);
    // Receive a task from the local scheduler. This will block until the local
    // scheduler gives this client a task.
    read_message(conn->conn, &type, &reply_size, &reply);
  }
  if (type == static_cast<int64_t>(MessageType::DisconnectClient)) {
    RAY_LOG(DEBUG) << "Exiting because local scheduler closed connection.";
    exit(1);
  }
  if (type != static_cast<int64_t>(MessageType::ExecuteTask)) {
    RAY_LOG(FATAL) << "Problem communicating with raylet from worker: check logs or "
                      "dmesg for previous errors.";
  }

  // Parse the flatbuffer object.
  auto reply_message = flatbuffers::GetRoot<ray::protocol::GetTaskReply>(reply);

  // Set the resource IDs for this task.
  conn->resource_ids_.clear();
  for (size_t i = 0; i < reply_message->fractional_resource_ids()->size(); ++i) {
    auto const &fractional_resource_ids =
        reply_message->fractional_resource_ids()->Get(i);
    auto &acquired_resources = conn->resource_ids_[string_from_flatbuf(
        *fractional_resource_ids->resource_name())];

    size_t num_resource_ids = fractional_resource_ids->resource_ids()->size();
    size_t num_resource_fractions = fractional_resource_ids->resource_fractions()->size();
    RAY_CHECK(num_resource_ids == num_resource_fractions);
    RAY_CHECK(num_resource_ids > 0);
    for (size_t j = 0; j < num_resource_ids; ++j) {
      int64_t resource_id = fractional_resource_ids->resource_ids()->Get(j);
      double resource_fraction = fractional_resource_ids->resource_fractions()->Get(j);
      if (num_resource_ids > 1) {
        int64_t whole_fraction = resource_fraction;
        RAY_CHECK(whole_fraction == resource_fraction);
      }
      acquired_resources.push_back(std::make_pair(resource_id, resource_fraction));
    }
  }

  ray::raylet::TaskSpecification *task_spec = new ray::raylet::TaskSpecification(
      string_from_flatbuf(*reply_message->task_spec()));

  // Free the original message from the local scheduler.
  free(reply);

  // Return the copy of the task spec and pass ownership to the caller.
  return task_spec;
}

void local_scheduler_task_done(LocalSchedulerConnection *conn) {
  write_message(conn->conn, static_cast<int64_t>(MessageType::TaskDone), 0, NULL,
                &conn->write_mutex);
}

void local_scheduler_fetch_or_reconstruct(LocalSchedulerConnection *conn,
                                          const std::vector<ObjectID> &object_ids,
                                          bool fetch_only,
                                          const TaskID &current_task_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ids_message = to_flatbuf(fbb, object_ids);
  auto message = ray::protocol::CreateFetchOrReconstruct(
      fbb, object_ids_message, fetch_only, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  write_message(conn->conn, static_cast<int64_t>(MessageType::FetchOrReconstruct),
                fbb.GetSize(), fbb.GetBufferPointer(), &conn->write_mutex);
  /* TODO(swang): Propagate the error. */
}

void local_scheduler_notify_unblocked(LocalSchedulerConnection *conn,
                                      const TaskID &current_task_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      ray::protocol::CreateNotifyUnblocked(fbb, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  write_message(conn->conn, static_cast<int64_t>(MessageType::NotifyUnblocked),
                fbb.GetSize(), fbb.GetBufferPointer(), &conn->write_mutex);
}

std::pair<std::vector<ObjectID>, std::vector<ObjectID>> local_scheduler_wait(
    LocalSchedulerConnection *conn, const std::vector<ObjectID> &object_ids,
    int num_returns, int64_t timeout_milliseconds, bool wait_local,
    const TaskID &current_task_id) {
  // Write request.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateWaitRequest(
      fbb, to_flatbuf(fbb, object_ids), num_returns, timeout_milliseconds, wait_local,
      to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  int64_t type;
  int64_t reply_size;
  uint8_t *reply;
  {
    std::unique_lock<std::mutex> guard(conn->mutex);
    write_message(conn->conn,
                  static_cast<int64_t>(ray::protocol::MessageType::WaitRequest),
                  fbb.GetSize(), fbb.GetBufferPointer(), &conn->write_mutex);
    // Read result.
    read_message(conn->conn, &type, &reply_size, &reply);
  }
  if (static_cast<ray::protocol::MessageType>(type) !=
      ray::protocol::MessageType::WaitReply) {
    RAY_LOG(FATAL) << "Problem communicating with raylet from worker: check logs or "
                      "dmesg for previous errors.";
  }
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

void local_scheduler_push_error(LocalSchedulerConnection *conn, const JobID &job_id,
                                const std::string &type, const std::string &error_message,
                                double timestamp) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreatePushErrorRequest(
      fbb, to_flatbuf(fbb, job_id), fbb.CreateString(type),
      fbb.CreateString(error_message), timestamp);
  fbb.Finish(message);

  write_message(conn->conn,
                static_cast<int64_t>(ray::protocol::MessageType::PushErrorRequest),
                fbb.GetSize(), fbb.GetBufferPointer(), &conn->write_mutex);
}

void local_scheduler_push_profile_events(LocalSchedulerConnection *conn,
                                         const ProfileTableDataT &profile_events) {
  flatbuffers::FlatBufferBuilder fbb;

  auto message = CreateProfileTableData(fbb, &profile_events);
  fbb.Finish(message);

  write_message(conn->conn, static_cast<int64_t>(
                                ray::protocol::MessageType::PushProfileEventsRequest),
                fbb.GetSize(), fbb.GetBufferPointer(), &conn->write_mutex);
}

void local_scheduler_free_objects_in_object_store(
    LocalSchedulerConnection *conn, const std::vector<ray::ObjectID> &object_ids,
    bool local_only) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateFreeObjectsRequest(fbb, local_only,
                                                         to_flatbuf(fbb, object_ids));
  fbb.Finish(message);

  int success = write_message(
      conn->conn,
      static_cast<int64_t>(ray::protocol::MessageType::FreeObjectsInObjectStoreRequest),
      fbb.GetSize(), fbb.GetBufferPointer(), &conn->write_mutex);
  RAY_CHECK(success == 0) << "Failed to write message to raylet.";
}
