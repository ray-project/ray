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
#include "ray/ray_config.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/task_spec.h"
#include "ray/util/logging.h"

using ray::protocol::MessageType;

RayletClient::RayletClient(const std::string &raylet_socket, const ClientID &client_id,
                           bool is_worker, const JobID &driver_id,
                           const Language &language)
    : client_id_(client_id),
      is_worker_(is_worker),
      driver_id_(driver_id),
      language_(language) {
  std::unique_ptr<stream_protocol::socket> _socket;
  auto status = ray::UnixSocketConnect(raylet_socket, main_service_, _socket);
  RAY_CHECK_OK_PREPEND(status,
                       "Unable to connect to raylet socket " + raylet_socket + ".");
  conn_.reset(new ray::LocalSimpleConnection(std::move(*_socket.release())));
  conn_->SetClientID(client_id);
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateRegisterClientRequest(
      fbb, is_worker, to_flatbuf(fbb, client_id), getpid(), to_flatbuf(fbb, driver_id),
      language);
  fbb.Finish(message);
  // Register the process ID with the raylet.
  // NOTE(swang): If raylet exits and we are registered as a worker, we will get killed.
  status = conn_->WriteMessageThreadSafe(MessageType::RegisterClientRequest,
                                         fbb.GetSize(), fbb.GetBufferPointer());
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Unable to register worker with raylet.");
}

ray::Status RayletClient::Disconnect() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateDisconnectClient(fbb);
  fbb.Finish(message);
  auto status = conn_->WriteMessageThreadSafe(MessageType::IntentionalDisconnectClient,
                                              fbb.GetSize(), fbb.GetBufferPointer());
  // Don't be too strict for disconnection errors.
  // Just create logs and prevent it from crash.
  if (!status.ok()) {
    RAY_LOG(ERROR) << status.ToString()
                   << " [RayletClient] Failed to disconnect from raylet.";
  }
  return ray::Status::OK();
}

ray::Status RayletClient::SubmitTask(const std::vector<ObjectID> &execution_dependencies,
                                     const ray::raylet::TaskSpecification &task_spec) {
  flatbuffers::FlatBufferBuilder fbb;
  auto execution_dependencies_message = to_flatbuf(fbb, execution_dependencies);
  auto message = ray::protocol::CreateSubmitTaskRequest(
      fbb, execution_dependencies_message, task_spec.ToFlatbuffer(fbb));
  fbb.Finish(message);
  return conn_->WriteMessageThreadSafe(MessageType::SubmitTask, fbb.GetSize(),
                                       fbb.GetBufferPointer());
}

ray::Status RayletClient::GetTask(
    std::unique_ptr<ray::raylet::TaskSpecification> *task_spec) {
  std::unique_ptr<uint8_t[]> reply;
  // Receive a task from the raylet. This will block until the local
  // scheduler gives this client a task.
  auto status = conn_->AtomicRequestReply(MessageType::GetTask, 0, nullptr,
                                          MessageType::ExecuteTask, reply);
  if (!status.ok()) {
    return status;
  }
  // Parse the flatbuffer object.
  auto reply_message = flatbuffers::GetRoot<ray::protocol::GetTaskReply>(reply.get());
  // Set the resource IDs for this task.
  resource_ids_.clear();
  for (size_t i = 0; i < reply_message->fractional_resource_ids()->size(); ++i) {
    auto const &fractional_resource_ids =
        reply_message->fractional_resource_ids()->Get(i);
    auto &acquired_resources =
        resource_ids_[string_from_flatbuf(*fractional_resource_ids->resource_name())];

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

  // Return the copy of the task spec and pass ownership to the caller.
  task_spec->reset(new ray::raylet::TaskSpecification(
      string_from_flatbuf(*reply_message->task_spec())));
  return ray::Status::OK();
}

ray::Status RayletClient::TaskDone() {
  return conn_->WriteMessageThreadSafe(MessageType::TaskDone, 0, nullptr);
}

ray::Status RayletClient::FetchOrReconstruct(const std::vector<ObjectID> &object_ids,
                                             bool fetch_only,
                                             const TaskID &current_task_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ids_message = to_flatbuf(fbb, object_ids);
  auto message = ray::protocol::CreateFetchOrReconstruct(
      fbb, object_ids_message, fetch_only, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  auto status = conn_->WriteMessageThreadSafe(MessageType::FetchOrReconstruct,
                                              fbb.GetSize(), fbb.GetBufferPointer());
  return status;
}

ray::Status RayletClient::NotifyUnblocked(const TaskID &current_task_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      ray::protocol::CreateNotifyUnblocked(fbb, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  return conn_->WriteMessageThreadSafe(MessageType::NotifyUnblocked, fbb.GetSize(),
                                       fbb.GetBufferPointer());
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
  auto status =
      conn_->AtomicRequestReply(MessageType::WaitRequest, fbb.GetSize(),
                                fbb.GetBufferPointer(), MessageType::WaitReply, reply);
  if (!status.ok()) {
    return status;
  }
  // Parse the flatbuffer object.
  auto reply_message = flatbuffers::GetRoot<ray::protocol::WaitReply>(reply.get());
  auto found = reply_message->found();
  for (uint i = 0; i < found->size(); i++) {
    ObjectID object_id = ObjectID::from_binary(found->Get(i)->str());
    result->first.push_back(object_id);
  }
  auto remaining = reply_message->remaining();
  for (uint i = 0; i < remaining->size(); i++) {
    ObjectID object_id = ObjectID::from_binary(remaining->Get(i)->str());
    result->second.push_back(object_id);
  }
  return ray::Status::OK();
}

ray::Status RayletClient::PushError(const JobID &job_id, const std::string &type,
                                    const std::string &error_message, double timestamp) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreatePushErrorRequest(
      fbb, to_flatbuf(fbb, job_id), fbb.CreateString(type),
      fbb.CreateString(error_message), timestamp);
  fbb.Finish(message);
  return conn_->WriteMessageThreadSafe(MessageType::PushErrorRequest, fbb.GetSize(),
                                       fbb.GetBufferPointer());
}

ray::Status RayletClient::PushProfileEvents(const ProfileTableDataT &profile_events) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateProfileTableData(fbb, &profile_events);
  fbb.Finish(message);
  auto status = conn_->WriteMessageThreadSafe(MessageType::PushProfileEventsRequest,
                                              fbb.GetSize(), fbb.GetBufferPointer());
  // Don't be too strict for profile errors. Just create logs and prevent it from crash.
  if (!status.ok()) {
    RAY_LOG(ERROR) << status.ToString()
                   << " [RayletClient] Failed to push profile events.";
  }
  return ray::Status::OK();
}

ray::Status RayletClient::FreeObjects(const std::vector<ray::ObjectID> &object_ids,
                                      bool local_only) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::protocol::CreateFreeObjectsRequest(fbb, local_only,
                                                         to_flatbuf(fbb, object_ids));
  fbb.Finish(message);
  auto status =
      conn_->WriteMessageThreadSafe(MessageType::FreeObjectsInObjectStoreRequest,
                                    fbb.GetSize(), fbb.GetBufferPointer());
  return status;
}
