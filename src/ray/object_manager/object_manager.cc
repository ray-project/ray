#include "object_manager.h"

namespace asio = boost::asio;

namespace ray {

// TODO(hme): Clean up commented logs once multi-threading integration is completed.
// Note: Current implementation has everything needed for concurrent transfers,
// but concurrency is currently disabled for integration purposes. Concurrency is
// disabled by running all asio components on the main thread (main_service).
ObjectManager::ObjectManager(asio::io_service &main_service,
                             std::unique_ptr<asio::io_service> object_manager_service,
                             ObjectManagerConfig config,
                             std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
    // TODO(hme): Eliminate knowledge of GCS.
    : client_id_(gcs_client->client_table().GetLocalClientId()),
      object_directory_(new ObjectDirectory(gcs_client)),
      object_manager_service_(std::move(object_manager_service)),
      work_(*object_manager_service_),
      connection_pool_(object_directory_.get(), &main_service, client_id_),
      transfer_queue_() {
  main_service_ = &main_service;
  config_ = config;
  store_notification_.reset(
      new ObjectStoreNotification(main_service, config.store_socket_name));
  store_notification_->SubscribeObjAdded(
      [this](const ObjectID &oid) { NotifyDirectoryObjectAdd(oid); });
  store_notification_->SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });
  store_pool_.reset(new ObjectStorePool(config.store_socket_name));
  StartIOService();
};

ObjectManager::ObjectManager(asio::io_service &main_service,
                             std::unique_ptr<asio::io_service> object_manager_service,
                             ObjectManagerConfig config,
                             std::unique_ptr<ObjectDirectoryInterface> od)
    : object_directory_(std::move(od)),
      object_manager_service_(std::move(object_manager_service)),
      work_(*object_manager_service_),
      connection_pool_(object_directory_.get(), &main_service, client_id_),
      transfer_queue_() {
  // TODO(hme) Client ID is never set with this constructor.
  main_service_ = &main_service;
  config_ = config;
  store_notification_ = std::unique_ptr<ObjectStoreNotification>(
      new ObjectStoreNotification(main_service, config.store_socket_name));
  store_notification_->SubscribeObjAdded(
      [this](const ObjectID &oid) { NotifyDirectoryObjectAdd(oid); });
  store_notification_->SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });
  StartIOService();
};

void ObjectManager::StartIOService() {
  io_thread_ = std::thread(&ObjectManager::IOServiceLoop, this);
  // thread_group_.create_thread(boost::bind(&asio::io_service::run,
  // &io_service_));
}

void ObjectManager::IOServiceLoop() { object_manager_service_->run(); }

void ObjectManager::StopIOService() {
  object_manager_service_->stop();
  io_thread_.join();
  // thread_group_.join_all();
}

void ObjectManager::NotifyDirectoryObjectAdd(const ObjectID &object_id) {
  local_objects_.insert(object_id);
  ray::Status status = object_directory_->ReportObjectAdded(object_id, client_id_);
}

void ObjectManager::NotifyDirectoryObjectDeleted(const ObjectID &object_id) {
  local_objects_.erase(object_id);
  ray::Status status = object_directory_->ReportObjectRemoved(object_id, client_id_);
}

ray::Status ObjectManager::Terminate() {
  StopIOService();
  ray::Status status_code = object_directory_->Terminate();
  // TODO: evaluate store client termination status.
  store_notification_->Terminate();
  store_pool_->Terminate();
  return status_code;
};

ray::Status ObjectManager::SubscribeObjAdded(
    std::function<void(const ObjectID &)> callback) {
  store_notification_->SubscribeObjAdded(callback);
  return ray::Status::OK();
};

ray::Status ObjectManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  store_notification_->SubscribeObjDeleted(callback);
  return ray::Status::OK();
};

ray::Status ObjectManager::Pull(const ObjectID &object_id) {
  main_service_->post([this, object_id]() { RAY_CHECK_OK(Pull_(object_id)); });
  return Status::OK();
};

void ObjectManager::SchedulePull(const ObjectID &object_id, int wait_ms) {
  pull_requests_[object_id] = Timer(
      new asio::deadline_timer(*main_service_, boost::posix_time::milliseconds(wait_ms)));
  pull_requests_[object_id]->async_wait(
      [this, object_id](const boost::system::error_code &error_code) {
        pull_requests_.erase(object_id);
        RAY_CHECK_OK(Pull_(object_id));
      });
}

ray::Status ObjectManager::Pull_(const ObjectID &object_id) {
  ray::Status status_code = object_directory_->GetLocations(
      object_id,
      [this](const std::vector<ClientID> &client_ids, const ObjectID &object_id) {
        return GetLocationsSuccess(client_ids, object_id);
      },
      [this](const ObjectID &object_id) { return GetLocationsFailed(object_id); });
  return status_code;
}

void ObjectManager::GetLocationsSuccess(const std::vector<ray::ClientID> &client_ids,
                                        const ray::ObjectID &object_id) {
  RAY_CHECK(!client_ids.empty());
  ClientID client_id = client_ids.front();
  pull_requests_.erase(object_id);
  ray::Status status_code = Pull(object_id, client_id);
};

void ObjectManager::GetLocationsFailed(const ObjectID &object_id) {
  SchedulePull(object_id, config_.pull_timeout_ms);
};

ray::Status ObjectManager::Pull(const ObjectID &object_id, const ClientID &client_id) {
  // Check if object is already local, and client_id is not itself.
  if (local_objects_.count(object_id) != 0 || client_id == client_id_) {
    return ray::Status::OK();
  }

  Status status =
      connection_pool_.GetSender(ConnectionPool::MESSAGE, client_id,
                                 [this, object_id](SenderConnection::pointer conn) {
                                   Status status = ExecutePull(object_id, conn);
                                 },
                                 [this, object_id]() {
                                   // connection failed, so reschedule pull.
                                   SchedulePull(object_id, config_.pull_timeout_ms);
                                 });
  return status;
};

ray::Status ObjectManager::ExecutePull(const ObjectID &object_id,
                                       SenderConnection::pointer conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePullRequestMessage(fbb, fbb.CreateString(client_id_.binary()),
                                          fbb.CreateString(object_id.binary()));
  fbb.Finish(message);
  (void)conn->WriteMessage(OMMessageType_PullRequest, fbb.GetSize(),
                           fbb.GetBufferPointer());
  (void)connection_pool_.ReleaseSender(ConnectionPool::MESSAGE, conn);
  return ray::Status::OK();
};

ray::Status ObjectManager::Push(const ObjectID &object_id, const ClientID &client_id) {
  main_service_->post(
      [this, object_id, client_id]() { RAY_CHECK_OK(Push_(object_id, client_id)); });
  return Status::OK();
};

ray::Status ObjectManager::Push_(const ObjectID &object_id, const ClientID &client_id) {
  //  RAY_LOG(INFO) << "Push_ "
  //                << object_id << " "
  //                << client_id << " ";
  transfer_queue_.QueueSend(client_id, object_id);
  return DequeueTransfers();
}

ray::Status ObjectManager::DequeueTransfers() {
  ray::Status status = ray::Status::OK();
  while (num_transfers_ < max_transfers_) {
    if (transfer_queue_.Empty()) {
      return ray::Status::OK();
    }

    // RAY_LOG(INFO) << "DequeueTransfers " << client_id_;

    // this should run on the main thread, so no need to worry about
    // concurrent changes of num_transfers_.
    num_transfers_ += 1;

    if ((transfer_queue_.LastTransferType() == TransferQueue::SEND ||
         transfer_queue_.SendCount() == 0) &&
        transfer_queue_.ReceiveCount() > 0) {
      TransferQueue::ReceiveRequest req = transfer_queue_.DequeueReceive();
      RAY_CHECK_OK(
          ExecuteReceive(req.client_id, req.object_id, req.object_size, req.conn));
    } else {
      // send count > 0.
      TransferQueue::SendRequest req = transfer_queue_.DequeueSend();
      RAY_CHECK_OK(ExecuteSend(req.object_id, req.client_id));
    }
  }
  return status;
};

ray::Status ObjectManager::TransferCompleted() {
  // RAY_LOG(INFO) << "TransferCompleted " << client_id_;
  num_transfers_ -= 1;
  return DequeueTransfers();
};

ray::Status ObjectManager::ExecuteSend(const ObjectID &object_id,
                                       const ClientID &client_id) {
  //  RAY_LOG(INFO) << "ExecuteSend "
  //                << object_id << " "
  //                << client_id << " ";
  ray::Status status;
  status = connection_pool_.GetSender(ConnectionPool::TRANSFER, client_id,
                                      [this, object_id](SenderConnection::pointer conn) {
                                        // TODO(hme): Handle this error. It's
                                        // possible that the object is not local,
                                        // so this should not be a fatal error.
                                        RAY_CHECK_OK(SendHeaders(object_id, conn));
                                      },
                                      [this, object_id]() {
                                        // Push is best effort, so do nothing on failure.
                                      });
  return status;
}

ray::Status ObjectManager::SendHeaders(const ObjectID &object_id_const,
                                       SenderConnection::pointer conn) {
  // RAY_LOG(INFO) << "SendHeaders";
  ObjectID object_id = ObjectID(object_id_const);
  // Allocate and append the request to the transfer queue.
  plasma::ObjectBuffer object_buffer;
  plasma::ObjectID plasma_id = object_id.to_plasma_id();
  std::shared_ptr<plasma::PlasmaClient> store_client = store_pool_->GetObjectStore();
  ARROW_CHECK_OK(store_client->Get(&plasma_id, 1, 0, &object_buffer));
  if (object_buffer.data_size == -1) {
    RAY_LOG(ERROR) << "Failed to get object";
    // If the object wasn't locally available, exit immediately. If the object
    // later appears locally, the requesting plasma manager should request the
    // transfer again.
    (void)connection_pool_.ReleaseSender(ConnectionPool::TRANSFER, conn);
    return ray::Status::IOError(
        "Unable to transfer object to requesting plasma manager, object not local.");
  }
  RAY_CHECK(object_buffer.metadata->data() ==
            object_buffer.data->data() + object_buffer.data_size);

  TransferQueue::SendContext context;
  context.client_id = conn->GetClientID();
  context.object_id = object_id;
  context.object_size = object_buffer.data_size;
  context.data = const_cast<uint8_t *>(object_buffer.data->data());
  UniqueID context_id = transfer_queue_.AddContext(context);

  // Create buffer.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePushRequestMessage(fbb, fbb.CreateString(object_id.binary()),
                                          context.object_size);
  fbb.Finish(message);
  // Pack into asio buffer.
  size_t length = fbb.GetSize();
  std::vector<asio::const_buffer> buffer;
  buffer.push_back(asio::buffer(&length, sizeof(length)));
  buffer.push_back(asio::buffer(fbb.GetBufferPointer(), length));
  // Send asynchronously.
  asio::async_write(conn->GetSocket(), buffer,
                    boost::bind(&ObjectManager::SendObject, this, conn, context_id,
                                store_client, asio::placeholders::error));
  return ray::Status::OK();
};

void ObjectManager::SendObject(SenderConnection::pointer conn, const UniqueID &context_id,
                               std::shared_ptr<plasma::PlasmaClient> store_client,
                               const boost::system::error_code &header_ec) {
  TransferQueue::SendContext context = transfer_queue_.GetContext(context_id);
  // RAY_LOG(INFO) << "SendObject";
  if (header_ec.value() != 0) {
    // push failed.
    // TODO(hme): Trash sender.
    (void)connection_pool_.ReleaseSender(ConnectionPool::TRANSFER, conn);
    return;
  }
  boost::system::error_code ec;
  asio::write(conn->GetSocket(), asio::buffer(context.data, context.object_size), ec);
  if (ec.value() != 0) {
    // push failed.
    // TODO(hme): Trash sender.
    (void)connection_pool_.ReleaseSender(ConnectionPool::TRANSFER, conn);
    return;
  }
  // Do this regardless of whether it failed or succeeded.
  ARROW_CHECK_OK(store_client->Release(context.object_id.to_plasma_id()));
  store_pool_->ReleaseObjectStore(store_client);
  // RAY_LOG(INFO) << "ReleaseSender " << conn->GetClientID();
  (void)connection_pool_.ReleaseSender(ConnectionPool::TRANSFER, conn);
  (void)transfer_queue_.RemoveContext(context_id);
  ray::Status ray_status = TransferCompleted();
}

ray::Status ObjectManager::Cancel(const ObjectID &object_id) {
  // TODO(hme): Account for pull timers.
  ray::Status status = object_directory_->Cancel(object_id);
  return ray::Status::OK();
};

ray::Status ObjectManager::Wait(const std::vector<ObjectID> &object_ids,
                                uint64_t timeout_ms, int num_ready_objects,
                                const WaitCallback &callback) {
  // TODO: Implement wait.
  return ray::Status::OK();
};

void ObjectManager::ProcessNewClient(std::shared_ptr<ReceiverConnection> conn) {
  conn->ProcessMessages();
};

void ObjectManager::ProcessClientMessage(std::shared_ptr<ReceiverConnection> conn,
                                         int64_t message_type, const uint8_t *message) {
  switch (message_type) {
  case OMMessageType_PushRequest: {
    // RAY_LOG(INFO) << "ProcessClientMessage PushRequest";
    // TODO(hme): Realize design with transfer requests handled in this manner.
    break;
  }
  case OMMessageType_PullRequest: {
    // RAY_LOG(INFO) << "ProcessClientMessage PullRequest";
    ReceivePullRequest(conn, message);
    conn->ProcessMessages();
    break;
  }
  case OMMessageType_ConnectClient: {
    // RAY_LOG(INFO) << "ProcessClientMessage ConnectClient";
    ConnectClient(conn, message);
    break;
  }
  case OMMessageType_DisconnectClient: {
    // RAY_LOG(INFO) << "ProcessClientMessage DisconnectClient";
    DisconnectClient(conn, message);
    break;
  }
  default: { RAY_LOG(FATAL) << "invalid request " << message_type; }
  }
};

void ObjectManager::ConnectClient(std::shared_ptr<ReceiverConnection> &conn,
                                  const uint8_t *message) {
  // TODO: trash connection on failure.
  auto info = flatbuffers::GetRoot<ConnectClientMessage>(message);
  ClientID client_id = ObjectID::from_binary(info->client_id()->str());
  bool is_transfer = info->is_transfer();
  conn->SetClientID(client_id);
  //  RAY_LOG(INFO) << "ConnectClient "
  //                << client_id << " "
  //                << is_transfer;
  if (is_transfer) {
    connection_pool_.RegisterReceiver(ConnectionPool::TRANSFER, client_id, conn);
    (void)WaitPushReceive(conn);
  } else {
    connection_pool_.RegisterReceiver(ConnectionPool::MESSAGE, client_id, conn);
    conn->ProcessMessages();
  }
};

void ObjectManager::DisconnectClient(std::shared_ptr<ReceiverConnection> &conn,
                                     const uint8_t *message) {
  auto info = flatbuffers::GetRoot<DisconnectClientMessage>(message);
  ClientID client_id = ObjectID::from_binary(info->client_id()->str());
  bool is_transfer = info->is_transfer();
  if (is_transfer) {
    connection_pool_.RemoveReceiver(ConnectionPool::TRANSFER, client_id, conn);
  } else {
    connection_pool_.RemoveReceiver(ConnectionPool::MESSAGE, client_id, conn);
  }
};

void ObjectManager::ReceivePullRequest(std::shared_ptr<ReceiverConnection> &conn,
                                       const uint8_t *message) {
  // Serialize.
  auto pr = flatbuffers::GetRoot<PullRequestMessage>(message);
  ObjectID object_id = ObjectID::from_binary(pr->object_id()->str());
  ClientID client_id = ClientID::from_binary(pr->client_id()->str());
  // Push object to requesting client.
  ray::Status push_status = Push(object_id, client_id);
};

ray::Status ObjectManager::WaitPushReceive(std::shared_ptr<ReceiverConnection> conn) {
  //  RAY_LOG(INFO) << "WaitPushReceive";
  asio::async_read(conn->GetSocket(), asio::buffer(&read_length_, sizeof(read_length_)),
                   boost::bind(&ObjectManager::HandlePushReceive, this, conn,
                               asio::placeholders::error));
  return ray::Status::OK();
};

void ObjectManager::HandlePushReceive(std::shared_ptr<ReceiverConnection> conn,
                                      const boost::system::error_code &length_ec) {
  //  RAY_LOG(INFO) << "HandlePushReceive";
  std::vector<uint8_t> message;
  message.resize(read_length_);
  boost::system::error_code ec;
  asio::read(conn->GetSocket(), asio::buffer(message), ec);
  // Serialize.
  auto object_header = flatbuffers::GetRoot<PushRequestMessage>(message.data());
  ObjectID object_id = ObjectID::from_binary(object_header->object_id()->str());
  int64_t object_size = (int64_t)object_header->object_size();
  // TODO(hme): Queue receives...
  // transfer_queue_.QueueReceive(conn->GetClientID(), object_id, object_size, conn);
  // DequeueTransfers();
  (void)ExecuteReceive(conn->GetClientID(), object_id, object_size, conn);
}

ray::Status ObjectManager::ExecuteReceive(ClientID client_id, ObjectID object_id,
                                          uint64_t object_size,
                                          std::shared_ptr<ReceiverConnection> conn) {
  boost::system::error_code ec;
  //  RAY_LOG(INFO) << "ExecuteReceive "
  //                << object_id << " "
  //                << object_size << " ";
  int64_t metadata_size = 0;
  // Try to create shared buffer.
  std::shared_ptr<Buffer> data;
  std::shared_ptr<plasma::PlasmaClient> store_client = store_pool_->GetObjectStore();
  arrow::Status s = store_client->Create(object_id.to_plasma_id(), object_size, NULL,
                                         metadata_size, &data);
  if (s.ok()) {
    // Read object into store.
    uint8_t *mutable_data = data->mutable_data();
    asio::read(conn->GetSocket(), asio::buffer(mutable_data, object_size), ec);
    if (!ec.value()) {
      ARROW_CHECK_OK(store_client->Seal(object_id.to_plasma_id()));
      ARROW_CHECK_OK(store_client->Release(object_id.to_plasma_id()));
    } else {
      ARROW_CHECK_OK(store_client->Release(object_id.to_plasma_id()));
      ARROW_CHECK_OK(store_client->Abort(object_id.to_plasma_id()));
      RAY_LOG(ERROR) << "Receive Failed";
    }
  } else {
    RAY_LOG(ERROR) << "Buffer Create Failed: " << s.message();
    // Read object into empty buffer.
    uint8_t *mutable_data = (uint8_t *)malloc(object_size + metadata_size);
    asio::read(conn->GetSocket(), asio::buffer(mutable_data, object_size), ec);
  }
  store_pool_->ReleaseObjectStore(store_client);
  // Wait for another push.
  ray::Status status = WaitPushReceive(conn);
  // TransferCompleted();
  return status;
};

}  // namespace ray
