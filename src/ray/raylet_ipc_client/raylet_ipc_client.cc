// Copyright 2025 The Ray Authors.
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

#include "ray/raylet_ipc_client/raylet_ipc_client.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "ray/common/flatbuf_utils.h"
#include "ray/common/ray_config.h"
#include "ray/flatbuffers/node_manager_generated.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

namespace ray::ipc {

namespace {

flatbuffers::Offset<protocol::Address> AddressToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb, const rpc::Address &address) {
  return protocol::CreateAddress(fbb,
                                 fbb.CreateString(address.node_id()),
                                 fbb.CreateString(address.ip_address()),
                                 address.port(),
                                 fbb.CreateString(address.worker_id()));
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<protocol::Address>>>
AddressesToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb,
                      const std::vector<rpc::Address> &addresses) {
  std::vector<flatbuffers::Offset<protocol::Address>> address_vec;
  address_vec.reserve(addresses.size());
  for (const auto &addr : addresses) {
    address_vec.push_back(AddressToFlatbuffer(fbb, addr));
  }
  return fbb.CreateVector(address_vec);
}

void ShutdownIfLocalRayletDisconnected(const Status &status) {
  // Check if the Raylet process is still alive.
  // If we know the Raylet PID, check using that.
  // Else, assume the Raylet is our parent process.
  bool raylet_alive = true;
  auto raylet_pid = RayConfig::instance().RAYLET_PID();
  if (!raylet_pid.empty()) {
    if (!IsProcessAlive(static_cast<pid_t>(std::stoi(raylet_pid)))) {
      raylet_alive = false;
    }
  } else if (!IsParentProcessAlive()) {
    raylet_alive = false;
  }

  if (!status.ok() && !raylet_alive) {
    RAY_LOG(WARNING) << "Exiting because the Raylet IPC connection failed and the local "
                        "Raylet is dead. Status: "
                     << status;
    QuickExit();
  }
}

}  // namespace

RayletIpcClient::RayletIpcClient(instrumented_io_context &io_service,
                                 const std::string &address,
                                 int num_retries,
                                 int64_t timeout) {
  local_stream_socket socket(io_service);
  Status s = ConnectSocketRetry(socket, address, num_retries, timeout);
  if (!s.ok()) {
    RAY_LOG(FATAL) << "Failed to connect to socket at address:" << address;
  }

  conn_ = ServerConnection::Create(std::move(socket));
}

ray::Status RayletIpcClient::RegisterClient(const WorkerID &worker_id,
                                            rpc::WorkerType worker_type,
                                            const JobID &job_id,
                                            int runtime_env_hash,
                                            const rpc::Language &language,
                                            const std::string &ip_address,
                                            const std::string &serialized_job_config,
                                            const StartupToken &startup_token,
                                            NodeID *node_id,
                                            int *assigned_port) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      protocol::CreateRegisterClientRequest(fbb,
                                            static_cast<int>(worker_type),
                                            flatbuf::to_flatbuf(fbb, worker_id),
                                            getpid(),
                                            startup_token,
                                            flatbuf::to_flatbuf(fbb, job_id),
                                            runtime_env_hash,
                                            language,
                                            fbb.CreateString(ip_address),
                                            /*port=*/0,
                                            fbb.CreateString(serialized_job_config));
  fbb.Finish(message);
  std::vector<uint8_t> reply;
  Status status = AtomicRequestReply(
      MessageType::RegisterClientRequest, MessageType::RegisterClientReply, &reply, &fbb);
  RAY_RETURN_NOT_OK(status);

  auto reply_message = flatbuffers::GetRoot<protocol::RegisterClientReply>(reply.data());
  bool success = reply_message->success();
  if (!success) {
    return Status::Invalid(reply_message->failure_reason()->str());
  }

  *node_id = NodeID::FromBinary(reply_message->node_id()->str());
  *assigned_port = reply_message->port();
  return Status::OK();
}

Status RayletIpcClient::Disconnect(
    const rpc::WorkerExitType &exit_type,
    const std::string &exit_detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  RAY_LOG(INFO) << "RayletIpcClient::Disconnect, exit_type="
                << rpc::WorkerExitType_Name(exit_type) << ", exit_detail=" << exit_detail
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
  const auto &fb_exit_detail = fbb.CreateString(exit_detail);
  protocol::DisconnectClientRequestBuilder builder(fbb);
  builder.add_disconnect_type(static_cast<int>(exit_type));
  builder.add_disconnect_detail(fb_exit_detail);
  // Add to table builder here to avoid nested construction of flatbuffers
  if (creation_task_exception_pb_bytes != nullptr) {
    builder.add_creation_task_exception_pb(creation_task_exception_pb_bytes_fb_vector);
  }
  fbb.Finish(builder.Finish());
  std::vector<uint8_t> reply;
  // NOTE(edoakes): AtomicRequestReply will fast fail and exit the process if the raylet
  // is already dead.
  // TODO(edoakes): we should add a timeout to this call in case the raylet is overloaded.
  return AtomicRequestReply(MessageType::DisconnectClientRequest,
                            MessageType::DisconnectClientReply,
                            &reply,
                            &fbb);
}

Status RayletIpcClient::AnnounceWorkerPortForWorker(int port) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateAnnounceWorkerPort(fbb, port, fbb.CreateString(""));
  fbb.Finish(message);
  return WriteMessage(MessageType::AnnounceWorkerPort, &fbb);
}

Status RayletIpcClient::AnnounceWorkerPortForDriver(int port,
                                                    const std::string &entrypoint) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      protocol::CreateAnnounceWorkerPort(fbb, port, fbb.CreateString(entrypoint));
  fbb.Finish(message);
  std::vector<uint8_t> reply;
  RAY_RETURN_NOT_OK(AtomicRequestReply(MessageType::AnnounceWorkerPort,
                                       MessageType::AnnounceWorkerPortReply,
                                       &reply,
                                       &fbb));
  auto reply_message =
      flatbuffers::GetRoot<protocol::AnnounceWorkerPortReply>(reply.data());
  if (reply_message->success()) {
    return Status::OK();
  }
  return Status::Invalid(reply_message->failure_reason()->str());
}

Status RayletIpcClient::ActorCreationTaskDone() {
  return WriteMessage(MessageType::ActorCreationTaskDone);
}

Status RayletIpcClient::AsyncGetObjects(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses) {
  RAY_CHECK(object_ids.size() == owner_addresses.size());
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ids_message = flatbuf::to_flatbuf(fbb, object_ids);
  auto message = protocol::CreateAsyncGetObjectsRequest(
      fbb, object_ids_message, AddressesToFlatbuffer(fbb, owner_addresses));
  fbb.Finish(message);
  return WriteMessage(MessageType::AsyncGetObjectsRequest, &fbb);
}

Status RayletIpcClient::CancelGetRequest() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateCancelGetRequest(fbb);
  fbb.Finish(message);
  return WriteMessage(MessageType::CancelGetRequest, &fbb);
}

Status RayletIpcClient::NotifyWorkerBlocked() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyWorkerBlocked(fbb);
  fbb.Finish(message);
  return WriteMessage(MessageType::NotifyWorkerBlocked, &fbb);
}

Status RayletIpcClient::NotifyWorkerUnblocked() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyWorkerUnblocked(fbb);
  fbb.Finish(message);
  return WriteMessage(MessageType::NotifyWorkerUnblocked, &fbb);
}

StatusOr<absl::flat_hash_set<ObjectID>> RayletIpcClient::Wait(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses,
    int num_returns,
    int64_t timeout_milliseconds) {
  // Write request.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateWaitRequest(fbb,
                                             flatbuf::to_flatbuf(fbb, object_ids),
                                             AddressesToFlatbuffer(fbb, owner_addresses),
                                             num_returns,
                                             timeout_milliseconds);
  fbb.Finish(message);
  std::vector<uint8_t> reply;
  RAY_RETURN_NOT_OK(
      AtomicRequestReply(MessageType::WaitRequest, MessageType::WaitReply, &reply, &fbb));
  // Parse the flatbuffer object.
  auto reply_message = flatbuffers::GetRoot<protocol::WaitReply>(reply.data());
  auto *found = reply_message->found();
  absl::flat_hash_set<ObjectID> result;
  result.reserve(found->size());
  for (size_t i = 0; i < found->size(); i++) {
    result.insert(ObjectID::FromBinary(found->Get(i)->str()));
  }
  return result;
}

Status RayletIpcClient::WaitForActorCallArgs(
    const std::vector<rpc::ObjectReference> &references, int64_t tag) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<ObjectID> object_ids;
  std::vector<rpc::Address> owner_addresses;
  for (const auto &ref : references) {
    object_ids.push_back(ObjectID::FromBinary(ref.object_id()));
    owner_addresses.push_back(ref.owner_address());
  }
  auto message = protocol::CreateWaitForActorCallArgsRequest(
      fbb,
      flatbuf::to_flatbuf(fbb, object_ids),
      AddressesToFlatbuffer(fbb, owner_addresses),
      tag);
  fbb.Finish(message);
  return WriteMessage(MessageType::WaitForActorCallArgsRequest, &fbb);
}

Status RayletIpcClient::PushError(const JobID &job_id,
                                  const std::string &type,
                                  const std::string &error_message,
                                  double timestamp) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreatePushErrorRequest(fbb,
                                                  flatbuf::to_flatbuf(fbb, job_id),
                                                  fbb.CreateString(type),
                                                  fbb.CreateString(error_message),
                                                  timestamp);
  fbb.Finish(message);
  return WriteMessage(MessageType::PushErrorRequest, &fbb);
}

Status RayletIpcClient::FreeObjects(const std::vector<ObjectID> &object_ids,
                                    bool local_only) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateFreeObjectsRequest(
      fbb, local_only, flatbuf::to_flatbuf(fbb, object_ids));
  fbb.Finish(message);
  return WriteMessage(MessageType::FreeObjectsInObjectStoreRequest, &fbb);
}

void RayletIpcClient::SubscribePlasmaReady(const ObjectID &object_id,
                                           const rpc::Address &owner_address) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateSubscribePlasmaReady(
      fbb, flatbuf::to_flatbuf(fbb, object_id), AddressToFlatbuffer(fbb, owner_address));
  fbb.Finish(message);

  RAY_CHECK_OK(WriteMessage(MessageType::SubscribePlasmaReady, &fbb));
}

Status RayletIpcClient::WriteMessage(MessageType type,
                                     flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(write_mutex_);
  int64_t length = fbb != nullptr ? fbb->GetSize() : 0;
  uint8_t *bytes = fbb != nullptr ? fbb->GetBufferPointer() : nullptr;
  auto status = conn_->WriteMessage(static_cast<int64_t>(type), length, bytes);
  ShutdownIfLocalRayletDisconnected(status);
  return status;
}

Status RayletIpcClient::AtomicRequestReply(MessageType request_type,
                                           MessageType reply_type,
                                           std::vector<uint8_t> *reply_message,
                                           flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(mutex_);
  RAY_RETURN_NOT_OK(WriteMessage(request_type, fbb));
  auto status = conn_->ReadMessage(static_cast<int64_t>(reply_type), reply_message);
  ShutdownIfLocalRayletDisconnected(status);
  return status;
}

}  // namespace ray::ipc
