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

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/synchronization/notification.h"
#include "ray/common/client_connection.h"
#include "ray/common/common_protocol.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/util/logging.h"

using MessageType = ray::protocol::MessageType;

namespace {

flatbuffers::Offset<ray::protocol::Address> to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb, const ray::rpc::Address &address) {
  return ray::protocol::CreateAddress(fbb,
                                      fbb.CreateString(address.raylet_id()),
                                      fbb.CreateString(address.ip_address()),
                                      address.port(),
                                      fbb.CreateString(address.worker_id()));
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

namespace ray::raylet {

RayletClient::RayletClient(std::shared_ptr<rpc::NodeManagerWorkerClient> grpc_client)
    : grpc_client_(std::move(grpc_client)) {}

RayletClient::RayletClient(std::unique_ptr<RayletConnection> raylet_conn,
                           std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client,
                           const WorkerID &worker_id)
    : grpc_client_(std::move(grpc_client)),
      worker_id_(worker_id),
      conn_(std::move(raylet_conn)) {}

Status RayletClient::Disconnect(
    const rpc::WorkerExitType &exit_type,
    const std::string &exit_detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  RAY_LOG(INFO) << "RayletClient::Disconnect, exit_type="
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
  return conn_->AtomicRequestReply(MessageType::DisconnectClientRequest,
                                   MessageType::DisconnectClientReply,
                                   &reply,
                                   &fbb);
}

// TODO(hjiang): After we merge register client and announce port, should delete this
// function.
Status RayletClient::AnnounceWorkerPortForWorker(int port) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateAnnounceWorkerPort(fbb, port, fbb.CreateString(""));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::AnnounceWorkerPort, &fbb);
}

Status RayletClient::AnnounceWorkerPortForDriver(int port,
                                                 const std::string &entrypoint) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      protocol::CreateAnnounceWorkerPort(fbb, port, fbb.CreateString(entrypoint));
  fbb.Finish(message);
  std::vector<uint8_t> reply;
  RAY_RETURN_NOT_OK(conn_->AtomicRequestReply(MessageType::AnnounceWorkerPort,
                                              MessageType::AnnounceWorkerPortReply,
                                              &reply,
                                              &fbb));
  auto reply_message =
      flatbuffers::GetRoot<protocol::AnnounceWorkerPortReply>(reply.data());
  if (reply_message->success()) {
    return Status::OK();
  }
  return Status::Invalid(string_from_flatbuf(*reply_message->failure_reason()));
}

Status RayletClient::ActorCreationTaskDone() {
  return conn_->WriteMessage(MessageType::ActorCreationTaskDone);
}

Status RayletClient::FetchOrReconstruct(const std::vector<ObjectID> &object_ids,
                                        const std::vector<rpc::Address> &owner_addresses,
                                        bool fetch_only,
                                        const TaskID &current_task_id) {
  RAY_CHECK(object_ids.size() == owner_addresses.size());
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ids_message = to_flatbuf(fbb, object_ids);
  auto message =
      protocol::CreateFetchOrReconstruct(fbb,
                                         object_ids_message,
                                         AddressesToFlatbuffer(fbb, owner_addresses),
                                         fetch_only,
                                         to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::FetchOrReconstruct, &fbb);
}

Status RayletClient::NotifyUnblocked(const TaskID &current_task_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyUnblocked(fbb, to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyUnblocked, &fbb);
}

Status RayletClient::NotifyDirectCallTaskBlocked() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyDirectCallTaskBlocked(fbb);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyDirectCallTaskBlocked, &fbb);
}

Status RayletClient::NotifyDirectCallTaskUnblocked() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyDirectCallTaskUnblocked(fbb);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::NotifyDirectCallTaskUnblocked, &fbb);
}

StatusOr<absl::flat_hash_set<ObjectID>> RayletClient::Wait(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses,
    int num_returns,
    int64_t timeout_milliseconds,
    const TaskID &current_task_id) {
  // Write request.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateWaitRequest(fbb,
                                             to_flatbuf(fbb, object_ids),
                                             AddressesToFlatbuffer(fbb, owner_addresses),
                                             num_returns,
                                             timeout_milliseconds,
                                             to_flatbuf(fbb, current_task_id));
  fbb.Finish(message);
  std::vector<uint8_t> reply;
  RAY_RETURN_NOT_OK(conn_->AtomicRequestReply(
      MessageType::WaitRequest, MessageType::WaitReply, &reply, &fbb));
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

Status RayletClient::WaitForDirectActorCallArgs(
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

Status RayletClient::PushError(const JobID &job_id,
                               const std::string &type,
                               const std::string &error_message,
                               double timestamp) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreatePushErrorRequest(fbb,
                                                  to_flatbuf(fbb, job_id),
                                                  fbb.CreateString(type),
                                                  fbb.CreateString(error_message),
                                                  timestamp);
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::PushErrorRequest, &fbb);
}

Status RayletClient::FreeObjects(const std::vector<ObjectID> &object_ids,
                                 bool local_only) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      protocol::CreateFreeObjectsRequest(fbb, local_only, to_flatbuf(fbb, object_ids));
  fbb.Finish(message);
  return conn_->WriteMessage(MessageType::FreeObjectsInObjectStoreRequest, &fbb);
}

void RayletClient::RequestWorkerLease(
    const rpc::TaskSpec &task_spec,
    bool grant_or_reject,
    const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback,
    const int64_t backlog_size,
    const bool is_selected_based_on_locality) {
  google::protobuf::Arena arena;
  auto request =
      google::protobuf::Arena::CreateMessage<rpc::RequestWorkerLeaseRequest>(&arena);
  // The unsafe allocating here is actually safe because the life-cycle of
  // task_spec is longer than request.
  // Request will be sent before the end of this call, and after that, it won't be
  // used any more.
  request->unsafe_arena_set_allocated_resource_spec(
      const_cast<rpc::TaskSpec *>(&task_spec));
  request->set_grant_or_reject(grant_or_reject);
  request->set_backlog_size(backlog_size);
  request->set_is_selected_based_on_locality(is_selected_based_on_locality);
  grpc_client_->RequestWorkerLease(*request, callback);
}

void RayletClient::PrestartWorkers(
    const rpc::PrestartWorkersRequest &request,
    const rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback) {
  grpc_client_->PrestartWorkers(request, callback);
}

std::shared_ptr<grpc::Channel> RayletClient::GetChannel() const {
  return grpc_client_->Channel();
}

void RayletClient::ReportWorkerBacklog(
    const WorkerID &worker_id,
    const std::vector<rpc::WorkerBacklogReport> &backlog_reports) {
  rpc::ReportWorkerBacklogRequest request;
  request.set_worker_id(worker_id.Binary());
  request.mutable_backlog_reports()->Add(backlog_reports.begin(), backlog_reports.end());
  grpc_client_->ReportWorkerBacklog(
      request,
      [](const Status &status, rpc::ReportWorkerBacklogReply &&reply /*unused*/) {
        RAY_LOG_IF_ERROR(INFO, status)
            << "Error reporting task backlog information: " << status;
      });
}

Status RayletClient::ReturnWorker(int worker_port,
                                  const WorkerID &worker_id,
                                  bool disconnect_worker,
                                  const std::string &disconnect_worker_error_detail,
                                  bool worker_exiting) {
  rpc::ReturnWorkerRequest request;
  request.set_worker_port(worker_port);
  request.set_worker_id(worker_id.Binary());
  request.set_disconnect_worker(disconnect_worker);
  request.set_disconnect_worker_error_detail(disconnect_worker_error_detail);
  request.set_worker_exiting(worker_exiting);
  grpc_client_->ReturnWorker(
      request, [](const Status &status, rpc::ReturnWorkerReply &&reply /*unused*/) {
        RAY_LOG_IF_ERROR(INFO, status) << "Error returning worker: " << status;
      });
  return Status::OK();
}

void RayletClient::GetTaskFailureCause(
    const TaskID &task_id,
    const ray::rpc::ClientCallback<ray::rpc::GetTaskFailureCauseReply> &callback) {
  rpc::GetTaskFailureCauseRequest request;
  request.set_task_id(task_id.Binary());
  grpc_client_->GetTaskFailureCause(
      request, [callback](const Status &status, rpc::GetTaskFailureCauseReply &&reply) {
        RAY_LOG_IF_ERROR(INFO, status) << "Error getting task result: " << status;
        callback(status, std::move(reply));
      });
}

void RayletClient::RegisterMutableObjectReader(
    const ObjectID &writer_object_id,
    int64_t num_readers,
    const ObjectID &reader_object_id,
    const ray::rpc::ClientCallback<ray::rpc::RegisterMutableObjectReply> &callback) {
  rpc::RegisterMutableObjectRequest request;
  request.set_writer_object_id(writer_object_id.Binary());
  request.set_num_readers(num_readers);
  request.set_reader_object_id(reader_object_id.Binary());
  grpc_client_->RegisterMutableObject(request, callback);
}

void RayletClient::PushMutableObject(
    const ObjectID &writer_object_id,
    uint64_t data_size,
    uint64_t metadata_size,
    void *data,
    const ray::rpc::ClientCallback<ray::rpc::PushMutableObjectReply> &callback) {
  // Ray sets the gRPC max payload size to ~512 MiB. We set the max chunk size to a
  // slightly lower value to allow extra padding just in case.
  uint64_t kMaxGrpcPayloadSize = RayConfig::instance().max_grpc_message_size() * 0.98;
  uint64_t total_size = data_size + metadata_size;
  uint64_t total_num_chunks = total_size / kMaxGrpcPayloadSize;
  // If `total_size` is not a multiple of `kMaxGrpcPayloadSize`, then we need to send an
  // extra chunk with the remaining data.
  if (total_size % kMaxGrpcPayloadSize) {
    total_num_chunks++;
  }

  for (uint64_t i = 0; i < total_num_chunks; i++) {
    rpc::PushMutableObjectRequest request;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(data_size);
    request.set_total_metadata_size(metadata_size);

    uint64_t chunk_size = (i < total_num_chunks - 1) ? kMaxGrpcPayloadSize
                                                     : (total_size % kMaxGrpcPayloadSize);
    uint64_t offset = i * kMaxGrpcPayloadSize;
    request.set_offset(offset);
    request.set_chunk_size(chunk_size);
    // This assumes that the format of the object is a contiguous buffer of (data |
    // metadata).
    request.set_payload(static_cast<char *>(data) + offset, chunk_size);

    // TODO(jackhumphries): Add failure recovery, retries, and timeout.
    grpc_client_->PushMutableObject(
        request, [callback](const Status &status, rpc::PushMutableObjectReply &&reply) {
          RAY_LOG_IF_ERROR(ERROR, status) << "Error pushing mutable object: " << status;
          if (reply.done()) {
            // The callback is only executed once the receiver node receives all chunks
            // for the mutable object write.
            callback(status, std::move(reply));
          }
        });
  }
}

void RayletClient::ReleaseUnusedActorWorkers(
    const std::vector<WorkerID> &workers_in_use,
    const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) {
  rpc::ReleaseUnusedActorWorkersRequest request;
  for (auto &worker_id : workers_in_use) {
    request.add_worker_ids_in_use(worker_id.Binary());
  }
  grpc_client_->ReleaseUnusedActorWorkers(
      request,
      [callback](const Status &status, rpc::ReleaseUnusedActorWorkersReply &&reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING)
              << "Error releasing workers from raylet, the raylet may have died:"
              << status;
        }
        callback(status, std::move(reply));
      });
}

void RayletClient::CancelWorkerLease(
    const TaskID &task_id,
    const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) {
  rpc::CancelWorkerLeaseRequest request;
  request.set_task_id(task_id.Binary());
  grpc_client_->CancelWorkerLease(request, callback);
}

void RayletClient::PrepareBundleResources(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
    const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback) {
  rpc::PrepareBundleResourcesRequest request;
  std::set<std::string> nodes;
  for (const auto &bundle_spec : bundle_specs) {
    nodes.insert(bundle_spec->NodeId().Hex());
    auto message_bundle = request.add_bundle_specs();
    message_bundle->CopyFrom(bundle_spec->GetMessage());
  }
  RAY_CHECK(nodes.size() == 1);
  grpc_client_->PrepareBundleResources(request, callback);
}

void RayletClient::CommitBundleResources(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
    const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback) {
  rpc::CommitBundleResourcesRequest request;
  std::set<std::string> nodes;
  for (const auto &bundle_spec : bundle_specs) {
    nodes.insert(bundle_spec->NodeId().Hex());
    auto message_bundle = request.add_bundle_specs();
    message_bundle->CopyFrom(bundle_spec->GetMessage());
  }
  RAY_CHECK(nodes.size() == 1);
  grpc_client_->CommitBundleResources(request, callback);
}

void RayletClient::CancelResourceReserve(
    const BundleSpecification &bundle_spec,
    const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback) {
  rpc::CancelResourceReserveRequest request;
  request.mutable_bundle_spec()->CopyFrom(bundle_spec.GetMessage());
  grpc_client_->CancelResourceReserve(request, callback);
}

void RayletClient::ReleaseUnusedBundles(
    const std::vector<rpc::Bundle> &bundles_in_use,
    const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) {
  rpc::ReleaseUnusedBundlesRequest request;
  for (auto &bundle : bundles_in_use) {
    request.add_bundles_in_use()->CopyFrom(bundle);
  }
  grpc_client_->ReleaseUnusedBundles(
      request, [callback](const Status &status, rpc::ReleaseUnusedBundlesReply &&reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING)
              << "Error releasing bundles from raylet, the raylet may have died:"
              << status;
        }
        callback(status, std::move(reply));
      });
}

void RayletClient::PinObjectIDs(
    const rpc::Address &caller_address,
    const std::vector<ObjectID> &object_ids,
    const ObjectID &generator_id,
    const rpc::ClientCallback<rpc::PinObjectIDsReply> &callback) {
  rpc::PinObjectIDsRequest request;
  request.mutable_owner_address()->CopyFrom(caller_address);
  for (const ObjectID &object_id : object_ids) {
    request.add_object_ids(object_id.Binary());
  }
  if (!generator_id.IsNil()) {
    request.set_generator_id(generator_id.Binary());
  }
  pins_in_flight_++;
  auto rpc_callback = [this, callback = std::move(callback)](
                          Status status, rpc::PinObjectIDsReply &&reply) {
    pins_in_flight_--;
    callback(status, std::move(reply));
  };
  grpc_client_->PinObjectIDs(request, rpc_callback);
}

void RayletClient::ShutdownRaylet(
    const NodeID &node_id,
    bool graceful,
    const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback) {
  rpc::ShutdownRayletRequest request;
  request.set_graceful(graceful);
  grpc_client_->ShutdownRaylet(request, callback);
}

void RayletClient::DrainRaylet(
    const rpc::autoscaler::DrainNodeReason &reason,
    const std::string &reason_message,
    int64_t deadline_timestamp_ms,
    const rpc::ClientCallback<rpc::DrainRayletReply> &callback) {
  rpc::DrainRayletRequest request;
  request.set_reason(reason);
  request.set_reason_message(reason_message);
  request.set_deadline_timestamp_ms(deadline_timestamp_ms);
  grpc_client_->DrainRaylet(request, callback);
}

void RayletClient::IsLocalWorkerDead(
    const WorkerID &worker_id,
    const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) {
  rpc::IsLocalWorkerDeadRequest request;
  request.set_worker_id(worker_id.Binary());
  grpc_client_->IsLocalWorkerDead(request, callback);
}

void RayletClient::GlobalGC(const rpc::ClientCallback<rpc::GlobalGCReply> &callback) {
  rpc::GlobalGCRequest request;
  grpc_client_->GlobalGC(request, callback);
}

void RayletClient::GetResourceLoad(
    const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) {
  rpc::GetResourceLoadRequest request;
  grpc_client_->GetResourceLoad(request, callback);
}

void RayletClient::CancelTasksWithResourceShapes(
    const std::vector<google::protobuf::Map<std::string, double>> &resource_shapes,
    const rpc::ClientCallback<rpc::CancelTasksWithResourceShapesReply> &callback) {
  rpc::CancelTasksWithResourceShapesRequest request;

  for (const auto &resource_shape : resource_shapes) {
    rpc::CancelTasksWithResourceShapesRequest::ResourceShape *resource_shape_proto =
        request.add_resource_shapes();
    resource_shape_proto->mutable_resource_shape()->insert(resource_shape.begin(),
                                                           resource_shape.end());
  }

  grpc_client_->CancelTasksWithResourceShapes(request, callback);
}

void RayletClient::NotifyGCSRestart(
    const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback) {
  rpc::NotifyGCSRestartRequest request;
  grpc_client_->NotifyGCSRestart(request, callback);
}

void RayletClient::SubscribeToPlasma(const ObjectID &object_id,
                                     const rpc::Address &owner_address) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateSubscribePlasmaReady(
      fbb, to_flatbuf(fbb, object_id), to_flatbuf(fbb, owner_address));
  fbb.Finish(message);

  RAY_CHECK_OK(conn_->WriteMessage(MessageType::SubscribePlasmaReady, &fbb));
}

void RayletClient::GetSystemConfig(
    const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback) {
  rpc::GetSystemConfigRequest request;
  grpc_client_->GetSystemConfig(request, callback);
}

}  // namespace ray::raylet
