// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/raylet_client/raylet_client.h"

#include "ray/common/client_connection.h"
#include "ray/common/common_protocol.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

using MessageType = ray::protocol::MessageType;

namespace {
inline flatbuffers::Offset<ray::protocol::Address> to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb, const ray::rpc::Address &address) {
  return ray::protocol::CreateAddress(
      fbb, fbb.CreateString(address.raylet_id()), fbb.CreateString(address.ip_address()),
      address.port(), fbb.CreateString(address.worker_id()));
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<ray::protocol::Address>>>
AddressesToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb,
                      const std::vector<ray::rpc::Address> &addresses) {
  std::vector<flatbuffers::Offset<ray::protocol::Address>> address_vec;
  address_vec.reserve(addresses.size());
  for (const auto &addr : addresses) {
    address_vec.push_back(to_flatbuf(fbb, addr));
  }
  return fbb.CreateVector(address_vec);
}

}  // namespace

namespace ray {

raylet::RayletConnection::RayletConnection(instrumented_io_context &io_service,
                                           const std::string &raylet_socket,
                                           int num_retries, int64_t timeout) {
  local_stream_socket socket(io_service);
  Status s = ConnectSocketRetry(socket, raylet_socket, num_retries, timeout);
  // If we could not connect to the socket, exit.
  if (!s.ok()) {
    RAY_LOG(FATAL) << "Could not connect to socket " << raylet_socket;
  }
  conn_ = ServerConnection::Create(std::move(socket));
}

Status raylet::RayletConnection::WriteMessage(MessageType type,
                                              flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(write_mutex_);
  int64_t length = fbb ? fbb->GetSize() : 0;
  uint8_t *bytes = fbb ? fbb->GetBufferPointer() : nullptr;
  return conn_->WriteMessage(static_cast<int64_t>(type), length, bytes);
}

Status raylet::RayletConnection::AtomicRequestReply(MessageType request_type,
                                                    MessageType reply_type,
                                                    std::vector<uint8_t> *reply_message,
                                                    flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(mutex_);
  RAY_RETURN_NOT_OK(WriteMessage(request_type, fbb));
  return conn_->ReadMessage(static_cast<int64_t>(reply_type), reply_message);
}

raylet::RayletClient::RayletClient(
    std::shared_ptr<rpc::NodeManagerWorkerClient> grpc_client)
    : grpc_client_(std::move(grpc_client)) {}

raylet::RayletClient::RayletClient(
    instrumented_io_context &io_service,
    std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client,
    const std::string &raylet_socket, const WorkerID &worker_id,
    rpc::WorkerType worker_type, const JobID &job_id, const Language &language,
    const std::string &ip_address, Status *status, NodeID *raylet_id, int *port,
    std::string *serialized_job_config)
    : grpc_client_(std::move(grpc_client)), worker_id_(worker_id), job_id_(job_id) {
  conn_ = std::make_unique<raylet::RayletConnection>(io_service, raylet_socket, -1, -1);

  flatbuffers::FlatBufferBuilder fbb;
  // TODO(suquark): Use `WorkerType` in `common.proto` without converting to int.
  auto message = protocol::CreateRegisterClientRequest(
      fbb, static_cast<int>(worker_type), to_flatbuf(fbb, worker_id), getpid(),
      to_flatbuf(fbb, job_id), language, fbb.CreateString(ip_address), /*port=*/0,
      fbb.CreateString(*serialized_job_config));
  fbb.Finish(message);
  // Register the process ID with the raylet.
  // NOTE(swang): If raylet exits and we are registered as a worker, we will get killed.
  std::vector<uint8_t> reply;
  auto request_status = conn_->AtomicRequestReply(
      MessageType::RegisterClientRequest, MessageType::RegisterClientReply, &reply, &fbb);
  if (!request_status.ok()) {
    *status =
        Status(request_status.code(),
               std::string("[RayletClient] Unable to register worker with raylet. ") +
                   request_status.message());
    return;
  }
  auto reply_message = flatbuffers::GetRoot<protocol::RegisterClientReply>(reply.data());
  bool success = reply_message->success();
  if (success) {
    *status = Status::OK();
  } else {
    *status = Status::Invalid(string_from_flatbuf(*reply_message->failure_reason()));
    return;
  }
  *raylet_id = NodeID::FromBinary(reply_message->raylet_id()->str());
  *port = reply_message->port();

  *serialized_job_config = reply_message->serialized_job_config()->str();
}

Status raylet::RayletClient::Disconnect(
    rpc::WorkerExitType exit_type,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  RAY_LOG(INFO) << "RayletClient::Disconnect, exit_type="
                << rpc::WorkerExitType_Name(exit_type)
                << ", has creation_task_exception_pb_bytes="
                << (creation_task_exception_pb_bytes != nullptr);
  flatbuffers::FlatBufferBuilder fbb;
  flatbuffers::Offset<flatbuffers::Vector<uint8_t>>
      creation_task_exception_pb_bytes_fb_vector;
  if (creation_task_exception_pb_bytes != nullptr) {
    creation_task_exception_pb_bytes_fb_vector =
        fbb.CreateVector(creation_task_exception_pb_bytes->Data(),
                         creation_task_exception_pb_bytes->Size());
  }
  protocol::DisconnectClientBuilder builder(fbb);
  // Add to table builder here to avoid nested construction of flatbuffers
  if (creation_task_exception_pb_bytes != nullptr) {
    builder.add_creation_task_exception_pb(creation_task_exception_pb_bytes_fb_vector);
  }
  builder.add_disconnect_type(static_cast<int>(exit_type));
  fbb.Finish(builder.Finish());
  auto status = conn_->WriteMessage(MessageType::DisconnectClient, &fbb);
  // Don't be too strict for disconnection errors.
  // Just create logs and prevent it from crash.
  if (!status.ok()) {
    RAY_LOG(ERROR) << status.ToString()
                   << " [RayletClient] Failed to disconnect from raylet.";
  }
  return Status::OK();
}

Status raylet::RayletClient::AnnounceWorkerPort(int port) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateAnnounceWorkerPort(fbb, port);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::AnnounceWorkerPort, &fbb);
}

Status raylet::RayletClient::SubmitTask(const TaskSpecification &task_spec) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      protocol::CreateSubmitTaskRequest(fbb, fbb.CreateString(task_spec.Serialize()));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::SubmitTask, &fbb);
}

Status raylet::RayletClient::TaskDone() {
  return conn_->WriteMessage(MessageType::TaskDone);
}

Status raylet::RayletClient::FetchOrReconstruct(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses, bool fetch_only,
    bool mark_worker_blocked, const TaskID &current_task_id) {
  RAY_CHECK(object_ids.size() == owner_addresses.size());
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ids_message = to_flatbuf(fbb, object_ids);
  auto message = protocol::CreateFetchOrReconstruct(
      fbb, object_ids_message, AddressesToFlatbuffer(fbb, owner_addresses), fetch_only,
      mark_worker_blocked, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::FetchOrReconstruct, &fbb);
}

Status raylet::RayletClient::NotifyUnblocked(const TaskID &current_task_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyUnblocked(fbb, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyUnblocked, &fbb);
}

Status raylet::RayletClient::NotifyDirectCallTaskBlocked(bool release_resources) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyDirectCallTaskBlocked(fbb, release_resources);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyDirectCallTaskBlocked, &fbb);
}

Status raylet::RayletClient::NotifyDirectCallTaskUnblocked() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyDirectCallTaskUnblocked(fbb);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyDirectCallTaskUnblocked, &fbb);
}

Status raylet::RayletClient::Wait(const std::vector<ObjectID> &object_ids,
                                  const std::vector<rpc::Address> &owner_addresses,
                                  int num_returns, int64_t timeout_milliseconds,
                                  bool mark_worker_blocked, const TaskID &current_task_id,
                                  WaitResultPair *result) {
  // Write request.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateWaitRequest(
      fbb, to_flatbuf(fbb, object_ids), AddressesToFlatbuffer(fbb, owner_addresses),
      num_returns, timeout_milliseconds, mark_worker_blocked,
      to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  std::vector<uint8_t> reply;
  RAY_RETURN_NOT_OK(conn_->AtomicRequestReply(MessageType::WaitRequest,
                                              MessageType::WaitReply, &reply, &fbb));
  // Parse the flatbuffer object.
  auto reply_message = flatbuffers::GetRoot<protocol::WaitReply>(reply.data());
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
  return Status::OK();
}

Status raylet::RayletClient::WaitForDirectActorCallArgs(
    const std::vector<rpc::ObjectReference> &references, int64_t tag) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<ObjectID> object_ids;
  std::vector<rpc::Address> owner_addresses;
  for (const auto &ref : references) {
    object_ids.push_back(ObjectID::FromBinary(ref.object_id()));
    owner_addresses.push_back(ref.owner_address());
  }
  auto message = protocol::CreateWaitForDirectActorCallArgsRequest(
      fbb, to_flatbuf(fbb, object_ids), AddressesToFlatbuffer(fbb, owner_addresses), tag);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::WaitForDirectActorCallArgsRequest, &fbb);
}

Status raylet::RayletClient::PushError(const JobID &job_id, const std::string &type,
                                       const std::string &error_message,
                                       double timestamp) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreatePushErrorRequest(
      fbb, to_flatbuf(fbb, job_id), fbb.CreateString(type),
      fbb.CreateString(error_message), timestamp);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::PushErrorRequest, &fbb);
}

Status raylet::RayletClient::PushProfileEvents(const ProfileTableData &profile_events) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fbb.CreateString(profile_events.SerializeAsString());
  fbb.Finish(message);

  auto status = conn_->WriteMessage(MessageType::PushProfileEventsRequest, &fbb);
  // Don't be too strict for profile errors. Just create logs and prevent it from crash.
  if (!status.ok()) {
    RAY_LOG(ERROR) << status.ToString()
                   << " [RayletClient] Failed to push profile events.";
  }
  return Status::OK();
}

Status raylet::RayletClient::FreeObjects(const std::vector<ObjectID> &object_ids,
                                         bool local_only) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      protocol::CreateFreeObjectsRequest(fbb, local_only, to_flatbuf(fbb, object_ids));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::FreeObjectsInObjectStoreRequest, &fbb);
}

Status raylet::RayletClient::SetResource(const std::string &resource_name,
                                         const double capacity, const NodeID &node_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateSetResourceRequest(fbb, fbb.CreateString(resource_name),
                                                    capacity, to_flatbuf(fbb, node_id));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::SetResourceRequest, &fbb);
}

void raylet::RayletClient::RequestWorkerLease(
    const TaskSpecification &resource_spec,
    const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback,
    const int64_t backlog_size) {
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_resource_spec()->CopyFrom(resource_spec.GetMessage());
  request.set_backlog_size(backlog_size);
  grpc_client_->RequestWorkerLease(request, callback);
}

/// Spill objects to external storage.
void raylet::RayletClient::RequestObjectSpillage(
    const ObjectID &object_id,
    const rpc::ClientCallback<rpc::RequestObjectSpillageReply> &callback) {
  rpc::RequestObjectSpillageRequest request;
  request.set_object_id(object_id.Binary());
  grpc_client_->RequestObjectSpillage(request, callback);
}

void raylet::RayletClient::RestoreSpilledObject(
    const ObjectID &object_id, const std::string &object_url,
    const NodeID &spilled_node_id,
    const rpc::ClientCallback<rpc::RestoreSpilledObjectReply> &callback) {
  RAY_CHECK(!spilled_node_id.IsNil());
  rpc::RestoreSpilledObjectRequest request;
  request.set_object_id(object_id.Binary());
  request.set_object_url(object_url);
  request.set_spilled_node_id(spilled_node_id.Binary());
  grpc_client_->RestoreSpilledObject(request, callback);
}

Status raylet::RayletClient::ReturnWorker(int worker_port, const WorkerID &worker_id,
                                          bool disconnect_worker) {
  rpc::ReturnWorkerRequest request;
  request.set_worker_port(worker_port);
  request.set_worker_id(worker_id.Binary());
  request.set_disconnect_worker(disconnect_worker);
  grpc_client_->ReturnWorker(
      request, [](const Status &status, const rpc::ReturnWorkerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(INFO) << "Error returning worker: " << status;
        }
      });
  return Status::OK();
}

void raylet::RayletClient::ReleaseUnusedWorkers(
    const std::vector<WorkerID> &workers_in_use,
    const rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply> &callback) {
  rpc::ReleaseUnusedWorkersRequest request;
  for (auto &worker_id : workers_in_use) {
    request.add_worker_ids_in_use(worker_id.Binary());
  }
  grpc_client_->ReleaseUnusedWorkers(
      request,
      [callback](const Status &status, const rpc::ReleaseUnusedWorkersReply &reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING)
              << "Error releasing workers from raylet, the raylet may have died:"
              << status;
        }
        callback(status, reply);
      });
}

void raylet::RayletClient::CancelWorkerLease(
    const TaskID &task_id,
    const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) {
  rpc::CancelWorkerLeaseRequest request;
  request.set_task_id(task_id.Binary());
  grpc_client_->CancelWorkerLease(request, callback);
}

void raylet::RayletClient::PrepareBundleResources(
    const BundleSpecification &bundle_spec,
    const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback) {
  rpc::PrepareBundleResourcesRequest request;
  request.mutable_bundle_spec()->CopyFrom(bundle_spec.GetMessage());
  grpc_client_->PrepareBundleResources(request, callback);
}

void raylet::RayletClient::CommitBundleResources(
    const BundleSpecification &bundle_spec,
    const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback) {
  rpc::CommitBundleResourcesRequest request;
  request.mutable_bundle_spec()->CopyFrom(bundle_spec.GetMessage());
  grpc_client_->CommitBundleResources(request, callback);
}

void raylet::RayletClient::CancelResourceReserve(
    BundleSpecification &bundle_spec,
    const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback) {
  rpc::CancelResourceReserveRequest request;
  request.mutable_bundle_spec()->CopyFrom(bundle_spec.GetMessage());
  grpc_client_->CancelResourceReserve(request, callback);
}

void raylet::RayletClient::ReleaseUnusedBundles(
    const std::vector<rpc::Bundle> &bundles_in_use,
    const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) {
  rpc::ReleaseUnusedBundlesRequest request;
  for (auto &bundle : bundles_in_use) {
    request.add_bundles_in_use()->CopyFrom(bundle);
  }
  grpc_client_->ReleaseUnusedBundles(
      request,
      [callback](const Status &status, const rpc::ReleaseUnusedBundlesReply &reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING)
              << "Error releasing bundles from raylet, the raylet may have died:"
              << status;
        }
        callback(status, reply);
      });
}

void raylet::RayletClient::PinObjectIDs(
    const rpc::Address &caller_address, const std::vector<ObjectID> &object_ids,
    const rpc::ClientCallback<rpc::PinObjectIDsReply> &callback) {
  rpc::PinObjectIDsRequest request;
  request.mutable_owner_address()->CopyFrom(caller_address);
  for (const ObjectID &object_id : object_ids) {
    request.add_object_ids(object_id.Binary());
  }
  grpc_client_->PinObjectIDs(request, callback);
}

void raylet::RayletClient::GlobalGC(
    const rpc::ClientCallback<rpc::GlobalGCReply> &callback) {
  rpc::GlobalGCRequest request;
  grpc_client_->GlobalGC(request, callback);
}

void raylet::RayletClient::RequestResourceReport(
    const rpc::ClientCallback<rpc::RequestResourceReportReply> &callback) {
  rpc::RequestResourceReportRequest request;
  grpc_client_->RequestResourceReport(request, callback);
}

void raylet::RayletClient::SubscribeToPlasma(const ObjectID &object_id,
                                             const rpc::Address &owner_address) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateSubscribePlasmaReady(fbb, to_flatbuf(fbb, object_id),
                                                      to_flatbuf(fbb, owner_address));
  fbb.Finish(message);

  RAY_CHECK_OK(conn_->WriteMessage(MessageType::SubscribePlasmaReady, &fbb));
}

void raylet::RayletClient::GetSystemConfig(
    const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback) {
  rpc::GetSystemConfigRequest request;
  grpc_client_->GetSystemConfig(request, callback);
}

}  // namespace ray
