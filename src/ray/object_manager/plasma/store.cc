// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// PLASMA STORE: This is a simple object store server process
//
// It accepts incoming client connections on a unix domain socket
// (name passed in via the -s option of the executable) and uses a
// single thread to serve the clients. Each client establishes a
// connection and can create objects, wait for objects and seal
// objects through that connection.
//
// It keeps a hash table that maps object_ids (which are 20 byte long,
// just enough to store and SHA1 hash) to memory mapped files.

#include "ray/object_manager/plasma/store.h"

#include <boost/bind/bind.hpp>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/get_request_queue.h"
#include "ray/object_manager/plasma/malloc.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/protocol.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/network_util.h"

namespace ph = boost::placeholders;
namespace fb = plasma::flatbuf;

namespace plasma {
namespace {

ray::ObjectID GetCreateRequestObjectId(const std::vector<uint8_t> &message) {
  const uint8_t *input = const_cast<uint8_t *>(message.data());
  size_t input_size = message.size();
  auto request = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(input);
  RAY_DCHECK(plasma::VerifyFlatbuffer(request, input, input_size));
  return ray::ObjectID::FromBinary(request->object_id()->str());
}
}  // namespace

PlasmaStore::PlasmaStore(instrumented_io_context &main_service,
                         IAllocator &allocator,
                         ray::FileSystemMonitor &fs_monitor,
                         const std::string &socket_name,
                         uint32_t delay_on_oom_ms,
                         ray::SpillObjectsCallback spill_objects_callback,
                         std::function<void()> object_store_full_callback,
                         ray::AddObjectCallback add_object_callback,
                         ray::DeleteObjectCallback delete_object_callback)
    : io_context_(main_service),
      socket_name_(socket_name),
      acceptor_(main_service, ray::ParseUrlEndpoint(socket_name)),
      socket_(main_service),
      allocator_(allocator),
      fs_monitor_(fs_monitor),
      add_object_callback_(add_object_callback),
      delete_object_callback_(delete_object_callback),
      object_lifecycle_mgr_(allocator_, delete_object_callback_),
      delay_on_oom_ms_(delay_on_oom_ms),
      create_request_queue_(
          fs_monitor_,
          /*oom_grace_period_s=*/RayConfig::instance().oom_grace_period_s(),
          spill_objects_callback,
          object_store_full_callback,
          /*get_time=*/
          []() { return absl::GetCurrentTimeNanos(); },
          // absl can't check thread safety for lambda
          [this]() ABSL_NO_THREAD_SAFETY_ANALYSIS {
            mutex_.AssertHeld();
            return GetDebugDump();
          }),
      get_request_queue_(
          io_context_,
          object_lifecycle_mgr_,
          // absl failed to check thread safety for lambda
          [this](const ObjectID &object_id,
                 std::optional<MEMFD_TYPE> fallback_allocated_fd,
                 const auto &request) ABSL_NO_THREAD_SAFETY_ANALYSIS {
            mutex_.AssertHeld();
            this->AddToClientObjectIds(
                object_id, fallback_allocated_fd, request->client_);
          },
          [this](const auto &request) { this->ReturnFromGet(request); }) {
  ray::SetCloseOnExec(acceptor_);

  if (RayConfig::instance().event_stats_print_interval_ms() > 0 &&
      RayConfig::instance().event_stats()) {
    PrintAndRecordDebugDump();
  }

  if (RayConfig::instance().metrics_report_interval_ms() > 0) {
    ScheduleRecordMetrics();
  }
}

// TODO(pcm): Get rid of this destructor by using RAII to clean up data.
PlasmaStore::~PlasmaStore() {}

void PlasmaStore::Start() {
  // Start listening for clients.
  DoAccept();
}

void PlasmaStore::Stop() { acceptor_.close(); }

// If this client is not already using the object, add the client to the
// object's list of clients, otherwise do nothing.
void PlasmaStore::AddToClientObjectIds(const ObjectID &object_id,
                                       std::optional<MEMFD_TYPE> fallback_allocated_fd,
                                       const std::shared_ptr<ClientInterface> &client) {
  // Check if this client is already using the object.
  auto &object_ids = client->GetObjectIDs();
  if (object_ids.find(object_id) != object_ids.end()) {
    return;
  }
  RAY_CHECK(object_lifecycle_mgr_.AddReference(object_id));
  // Add object id to the list of object ids that this client is using.
  client->MarkObjectAsUsed(object_id, fallback_allocated_fd);
}

PlasmaError PlasmaStore::HandleCreateObjectRequest(const std::shared_ptr<Client> &client,
                                                   const std::vector<uint8_t> &message,
                                                   bool fallback_allocator,
                                                   PlasmaObject *object) {
  const uint8_t *input = const_cast<uint8_t *>(message.data());
  size_t input_size = message.size();
  ray::ObjectInfo object_info;
  fb::ObjectSource source;
  int device_num;
  ReadCreateRequest(input, input_size, &object_info, &source, &device_num);

  if (device_num != 0) {
    RAY_LOG(ERROR) << "device_num != 0 but CUDA not enabled";
    return PlasmaError::OutOfMemory;
  }

  auto error = CreateObject(object_info, source, client, fallback_allocator, object);
  if (error == PlasmaError::OutOfMemory) {
    RAY_LOG(DEBUG) << "Not enough memory to create the object " << object_info.object_id
                   << ", data_size=" << object_info.data_size
                   << ", metadata_size=" << object_info.metadata_size;
  }
  return error;
}

PlasmaError PlasmaStore::CreateObject(const ray::ObjectInfo &object_info,
                                      fb::ObjectSource source,
                                      const std::shared_ptr<Client> &client,
                                      bool fallback_allocator,
                                      PlasmaObject *result) {
  auto pair = object_lifecycle_mgr_.CreateObject(object_info, source, fallback_allocator);
  auto entry = pair.first;
  auto error = pair.second;
  if (entry == nullptr) {
    return error;
  }
  entry->ToPlasmaObject(result, /* check sealed */ false);
  // Record that this client is using this object.
  std::optional<MEMFD_TYPE> fallback_allocated_fd = std::nullopt;
  if (entry->GetAllocation().fallback_allocated_) {
    fallback_allocated_fd = entry->GetAllocation().fd_;
  }
  AddToClientObjectIds(object_info.object_id, fallback_allocated_fd, client);
  return PlasmaError::OK;
}

void PlasmaStore::ReturnFromGet(const std::shared_ptr<GetRequest> &get_request) {
  // If the get request is already removed, do no-op. This can happen because the boost
  // timer is not atomic. See https://github.com/ray-project/ray/pull/15071.
  if (get_request->IsRemoved()) {
    return;
  }

  // Figure out how many file descriptors we need to send.
  absl::flat_hash_set<MEMFD_TYPE> fds_to_send;
  std::vector<MEMFD_TYPE> store_fds;
  std::vector<int64_t> mmap_sizes;
  for (const auto &object_id : get_request->object_ids_) {
    const PlasmaObject &object = get_request->objects_[object_id];
    MEMFD_TYPE fd = object.store_fd;
    if (object.data_size != -1 && fds_to_send.count(fd) == 0 && fd.first != INVALID_FD) {
      fds_to_send.insert(fd);
      store_fds.push_back(fd);
      mmap_sizes.push_back(object.mmap_size);
    }
  }
  // Send the get reply to the client.
  Status s = SendGetReply(std::dynamic_pointer_cast<Client>(get_request->client_),
                          &get_request->object_ids_[0],
                          get_request->objects_,
                          get_request->object_ids_.size(),
                          store_fds,
                          mmap_sizes);
  // If we successfully sent the get reply message to the client, then also send
  // the file descriptors.
  if (s.ok()) {
    // Send all of the file descriptors for the present objects.
    for (MEMFD_TYPE store_fd : store_fds) {
      Status send_fd_status = get_request->client_->SendFd(store_fd);
      if (!send_fd_status.ok()) {
        RAY_LOG(ERROR) << "Failed to send mmap results to client on fd "
                       << get_request->client_;
      }
    }
  } else {
    RAY_LOG(ERROR) << "Failed to send Get reply to client on fd " << get_request->client_;
  }
}

void PlasmaStore::ProcessGetRequest(const std::shared_ptr<Client> &client,
                                    const std::vector<ObjectID> &object_ids,
                                    int64_t timeout_ms) {
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "Adding get request " << object_id;
  }
  get_request_queue_.AddRequest(client, object_ids, timeout_ms);
}

bool PlasmaStore::RemoveFromClientObjectIds(const ObjectID &object_id,
                                            const std::shared_ptr<Client> &client) {
  auto &object_ids = client->GetObjectIDs();
  auto it = object_ids.find(object_id);
  if (it != object_ids.end()) {
    bool should_unmap = client->MarkObjectAsUnused(object_id);
    RAY_LOG(DEBUG) << "Object " << object_id
                   << " no longer in use by client, should_unmap = " << should_unmap;
    // Decrease reference count.
    object_lifecycle_mgr_.RemoveReference(object_id);
    // Return true to indicate that the client should unmap the fd for this object_id.
    return should_unmap;
  } else {
    // No mmap sections applicable.
    return false;
  }
}

bool PlasmaStore::ReleaseObject(const ObjectID &object_id,
                                const std::shared_ptr<Client> &client) {
  auto entry = object_lifecycle_mgr_.GetObject(object_id);
  if (entry != nullptr) {
    // Remove the client from the object's array of clients.
    return RemoveFromClientObjectIds(object_id, client);
  }
  return false;
}

void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    RAY_LOG(DEBUG) << "sealing object " << object_ids[i];
    auto entry = object_lifecycle_mgr_.SealObject(object_ids[i]);
    RAY_CHECK(entry) << object_ids[i] << " is missing or not sealed.";
    add_object_callback_(entry->GetObjectInfo());
  }

  for (size_t i = 0; i < object_ids.size(); ++i) {
    get_request_queue_.MarkObjectSealed(object_ids[i]);
  }
}

int PlasmaStore::AbortObject(const ObjectID &object_id,
                             const std::shared_ptr<Client> &client) {
  auto &object_ids = client->GetObjectIDs();
  auto it = object_ids.find(object_id);
  if (it == object_ids.end()) {
    // If the client requesting the abort is not the creator, do not
    // perform the abort.
    return 0;
  }
  // The client requesting the abort is the creator. Free the object.
  RAY_CHECK(object_lifecycle_mgr_.AbortObject(object_id) == PlasmaError::OK);
  client->MarkObjectAsUnused(object_id);
  return 1;
}

void PlasmaStore::ConnectClient(const boost::system::error_code &error) {
  if (!error) {
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = Client::Create(
        /*message_handler=*/
        [this](const std::shared_ptr<Client> &client,
               fb::MessageType message_type,
               const std::vector<uint8_t> &message) -> Status {
          return ProcessClientMessage(client, message_type, message);
        },
        /*connection_error_handler=*/
        [this](const std::shared_ptr<Client> &client,
               const boost::system::error_code &err) -> void {
          return HandleClientConnectionError(client, err);
        },
        std::move(socket_));

    // Start receiving messages.
    new_connection->ProcessMessages();
  }

  if (error != boost::asio::error::operation_aborted) {
    // We're ready to accept another client.
    DoAccept();
  }
}

void PlasmaStore::DisconnectClient(const std::shared_ptr<Client> &client) {
  client->Close();
  RAY_LOG(DEBUG) << "Disconnecting client on fd " << client;
  // Release all the objects that the client was using.
  absl::flat_hash_map<ObjectID, const LocalObject *> sealed_objects;
  auto &object_ids = client->GetObjectIDs();
  for (const auto &object_id : object_ids) {
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    if (entry == nullptr) {
      continue;
    }

    if (entry->Sealed()) {
      // Add sealed objects to a temporary list of object IDs. Do not perform
      // the remove here, since it potentially modifies the object_ids table.
      sealed_objects[object_id] = entry;
    } else {
      // Abort unsealed object.
      object_lifecycle_mgr_.AbortObject(object_id);
    }
  }

  /// Remove all of the client's GetRequests.
  get_request_queue_.RemoveGetRequestsForClient(client);

  for (const auto &[object_id, _] : sealed_objects) {
    RemoveFromClientObjectIds(object_id, client);
  }

  create_request_queue_.RemoveDisconnectedClientRequests(client);
}

void PlasmaStore::HandleClientConnectionError(const std::shared_ptr<Client> &client,
                                              const boost::system::error_code &error) {
  absl::MutexLock lock(&mutex_);
  RAY_LOG(WARNING) << "Disconnecting client due to connection error with code "
                   << error.value() << ": " << error.message();
  DisconnectClient(client);
}

Status PlasmaStore::ProcessClientMessage(const std::shared_ptr<Client> &client,
                                         fb::MessageType type,
                                         const std::vector<uint8_t> &message) {
  absl::MutexLock lock(&mutex_);
  // TODO(suquark): We should convert these interfaces to const later.
  const uint8_t *input = const_cast<uint8_t *>(message.data());
  size_t input_size = message.size();

  // Process the different types of requests.
  switch (type) {
  case fb::MessageType::PlasmaCreateRequest: {
    const auto &object_id = GetCreateRequestObjectId(message);
    const auto &request = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(input);
    const size_t object_size = request->data_size() + request->metadata_size();

    // absl failed analyze mutex safety for lambda
    auto handle_create = [this, client, message](
                             bool fallback_allocator,
                             PlasmaObject *result) ABSL_NO_THREAD_SAFETY_ANALYSIS {
      mutex_.AssertHeld();
      return HandleCreateObjectRequest(client, message, fallback_allocator, result);
    };

    if (request->try_immediately()) {
      RAY_LOG(DEBUG) << "Received request to create object " << object_id
                     << " immediately";
      auto result_error = create_request_queue_.TryRequestImmediately(
          object_id, client, handle_create, object_size);
      const auto &result = result_error.first;
      const auto &error = result_error.second;
      if (SendCreateReply(client, object_id, result, error).ok() &&
          error == PlasmaError::OK && result.device_num == 0) {
        static_cast<void>(client->SendFd(result.store_fd));
      }
    } else {
      auto req_id =
          create_request_queue_.AddRequest(object_id, client, handle_create, object_size);
      RAY_LOG(DEBUG) << "Received create request for object " << object_id
                     << " assigned request ID " << req_id << ", " << object_size
                     << " bytes";
      ProcessCreateRequests();
      ReplyToCreateClient(client, object_id, req_id);
    }
  } break;
  case fb::MessageType::PlasmaCreateRetryRequest: {
    auto request = flatbuffers::GetRoot<fb::PlasmaCreateRetryRequest>(input);
    RAY_DCHECK(plasma::VerifyFlatbuffer(request, input, input_size));
    const auto &object_id = ObjectID::FromBinary(request->object_id()->str());
    ReplyToCreateClient(client, object_id, request->request_id());
  } break;
  case fb::MessageType::PlasmaAbortRequest: {
    ObjectID object_id;
    ReadAbortRequest(input, input_size, &object_id);
    RAY_CHECK(AbortObject(object_id, client) == 1) << "To abort an object, the only "
                                                      "client currently using it "
                                                      "must be the creator.";
    RAY_RETURN_NOT_OK(SendAbortReply(client, object_id));
  } break;
  case fb::MessageType::PlasmaGetRequest: {
    std::vector<ObjectID> object_ids_to_get;
    int64_t timeout_ms;
    ReadGetRequest(input, input_size, object_ids_to_get, &timeout_ms);
    ProcessGetRequest(client, object_ids_to_get, timeout_ms);
  } break;
  case fb::MessageType::PlasmaReleaseRequest: {
    // May unmap: client knows a fallback-allocated fd is involved.
    // Should unmap: server finds refcnt == 0 -> need to be unmapped.
    bool may_unmap;
    ObjectID object_id;
    ReadReleaseRequest(input, input_size, &object_id, &may_unmap);
    bool should_unmap = ReleaseObject(object_id, client);
    if (!may_unmap) {
      RAY_CHECK(!should_unmap)
          << "Plasma client thinks a mmap should not be unmapped but server thinks so. "
             "This should not happen because a client knows the object is "
             "fallback-allocated in Get/Create time. Object ID: "
          << object_id;
    }
    if (may_unmap) {
      RAY_RETURN_NOT_OK(
          SendReleaseReply(client, object_id, should_unmap, PlasmaError::OK));
    }
  } break;
  case fb::MessageType::PlasmaDeleteRequest: {
    std::vector<ObjectID> object_ids;
    std::vector<PlasmaError> error_codes;
    ReadDeleteRequest(input, input_size, &object_ids);
    error_codes.reserve(object_ids.size());
    for (auto &object_id : object_ids) {
      error_codes.push_back(object_lifecycle_mgr_.DeleteObject(object_id));
    }
    RAY_RETURN_NOT_OK(SendDeleteReply(client, object_ids, error_codes));
  } break;
  case fb::MessageType::PlasmaContainsRequest: {
    ObjectID object_id;
    ReadContainsRequest(input, input_size, &object_id);
    if (object_lifecycle_mgr_.IsObjectSealed(object_id)) {
      RAY_RETURN_NOT_OK(SendContainsReply(client, object_id, 1));
    } else {
      RAY_RETURN_NOT_OK(SendContainsReply(client, object_id, 0));
    }
  } break;
  case fb::MessageType::PlasmaSealRequest: {
    ObjectID object_id;
    ReadSealRequest(input, input_size, &object_id);
    SealObjects({object_id});
    RAY_RETURN_NOT_OK(SendSealReply(client, object_id, PlasmaError::OK));
  } break;
  case fb::MessageType::PlasmaConnectRequest: {
    RAY_RETURN_NOT_OK(SendConnectReply(client, allocator_.GetFootprintLimit()));
  } break;
  case fb::MessageType::PlasmaDisconnectClient:
    RAY_LOG(DEBUG) << "Disconnecting client on fd " << client;
    DisconnectClient(client);
    return Status::Disconnected("The Plasma Store client is disconnected.");
    break;
  case fb::MessageType::PlasmaGetDebugStringRequest: {
    std::stringstream output_string_stream;
    object_lifecycle_mgr_.GetDebugDump(output_string_stream);
    output_string_stream << "\nEviction Stats:";
    output_string_stream << object_lifecycle_mgr_.EvictionPolicyDebugString();
    RAY_RETURN_NOT_OK(SendGetDebugStringReply(client, output_string_stream.str()));
  } break;
  default:
    // This code should be unreachable.
    RAY_LOG(FATAL) << "Invalid Plasma message type. type=" << static_cast<long>(type)
                   << ". " << kCorruptedRequestErrorMessage;
    RAY_CHECK(0);
  }
  return Status::OK();
}

void PlasmaStore::DoAccept() {
  acceptor_.async_accept(
      socket_,
      boost::bind(&PlasmaStore::ConnectClient, this, boost::asio::placeholders::error));
}

void PlasmaStore::ProcessCreateRequests() {
  // Only try to process requests if the timer is not set. If the timer is set,
  // that means that the first request is currently not serviceable because
  // there is not enough memory. In that case, we should wait for the timer to
  // expire before trying any requests again.
  if (create_timer_) {
    return;
  }

  auto status = create_request_queue_.ProcessRequests();
  uint32_t retry_after_ms = 0;
  if (!status.ok()) {
    retry_after_ms = delay_on_oom_ms_;

    if (!dumped_on_oom_) {
      RAY_LOG(INFO) << "Plasma store at capacity\n" << GetDebugDump();
      dumped_on_oom_ = true;
    }
  } else {
    dumped_on_oom_ = false;
  }

  if (retry_after_ms > 0) {
    // Try to process requests later, after space has been made.
    create_timer_ = execute_after(
        io_context_,
        [this]() {
          absl::MutexLock lock(&mutex_);
          create_timer_ = nullptr;
          ProcessCreateRequests();
        },
        std::chrono::milliseconds(retry_after_ms));
  }
}

void PlasmaStore::ReplyToCreateClient(const std::shared_ptr<Client> &client,
                                      const ObjectID &object_id,
                                      uint64_t req_id) {
  PlasmaObject result = {};
  PlasmaError error;
  bool finished = create_request_queue_.GetRequestResult(req_id, &result, &error);
  if (finished) {
    RAY_LOG(DEBUG) << "Finishing create object " << object_id << " request ID " << req_id;
    if (SendCreateReply(client, object_id, result, error).ok() &&
        error == PlasmaError::OK && result.device_num == 0) {
      static_cast<void>(client->SendFd(result.store_fd));
    }
  } else {
    static_cast<void>(SendUnfinishedCreateReply(client, object_id, req_id));
  }
}

bool PlasmaStore::IsObjectSpillable(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  auto entry = object_lifecycle_mgr_.GetObject(object_id);
  if (!entry) {
    // Object already evicted or deleted.
    return false;
  }
  return entry->Sealed() && entry->GetRefCount() == 1;
}

void PlasmaStore::PrintAndRecordDebugDump() const {
  absl::MutexLock lock(&mutex_);
  RAY_LOG(INFO) << GetDebugDump();
  stats_timer_ = execute_after(
      io_context_,
      [this]() { PrintAndRecordDebugDump(); },
      std::chrono::milliseconds(RayConfig::instance().event_stats_print_interval_ms()));
}

void PlasmaStore::ScheduleRecordMetrics() const {
  absl::MutexLock lock(&mutex_);
  object_lifecycle_mgr_.RecordMetrics();

  metric_timer_ = execute_after(
      io_context_,
      [this]() { ScheduleRecordMetrics(); },
      // divide by 2 to make sure record happens before reporting
      // this also matches with  NodeManager::RecordMetrics interval
      std::chrono::milliseconds(RayConfig::instance().metrics_report_interval_ms() / 2));
}

std::string PlasmaStore::GetDebugDump() const {
  std::stringstream buffer;
  buffer << "Plasma store debug dump: \n";
  buffer << "Current usage: " << (allocator_.Allocated() / 1e9) << " / "
         << (allocator_.GetFootprintLimit() / 1e9) << " GB\n";
  buffer << "- num bytes created total: "
         << object_lifecycle_mgr_.GetNumBytesCreatedTotal() << "\n";
  auto num_pending_requests = create_request_queue_.NumPendingRequests();
  auto num_pending_bytes = create_request_queue_.NumPendingBytes();
  buffer << num_pending_requests << " pending objects of total size "
         << num_pending_bytes / 1024 / 1024 << "MB\n";
  object_lifecycle_mgr_.GetDebugDump(buffer);
  return buffer.str();
}

}  // namespace plasma
