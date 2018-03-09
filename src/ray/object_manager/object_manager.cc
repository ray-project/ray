#include "object_manager.h"

namespace ray {

ObjectManager::ObjectManager(boost::asio::io_service &io_service,
                             ObjectManagerConfig config,
                             std::shared_ptr<ray::GcsClient> gcs_client)
    : object_directory_(new ObjectDirectory(gcs_client)), work_(io_service_) {
  config_ = config;
  store_client_ = std::unique_ptr<ObjectStoreClient>(
      new ObjectStoreClient(io_service, config.store_socket_name));
  store_client_->SubscribeObjAdded(
      [this](const ObjectID &oid) { NotifyDirectoryObjectAdd(oid); });
  store_client_->SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });
  StartIOService();
};

ObjectManager::ObjectManager(boost::asio::io_service &io_service,
                             ObjectManagerConfig config,
                             std::unique_ptr<ObjectDirectoryInterface> od)
    : object_directory_(std::move(od)), work_(io_service_) {
  config_ = config;
  store_client_ = std::unique_ptr<ObjectStoreClient>(
      new ObjectStoreClient(io_service, config.store_socket_name));
  store_client_->SubscribeObjAdded(
      [this](const ObjectID &oid) { NotifyDirectoryObjectAdd(oid); });
  store_client_->SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });
  StartIOService();
};

void ObjectManager::StartIOService() {
  io_thread_ = std::thread(&ObjectManager::IOServiceLoop, this);
  //  thread_group_.create_thread(boost::bind(&boost::asio::io_service::run,
  //  &io_service_));
}

void ObjectManager::IOServiceLoop() { io_service_.run(); }

void ObjectManager::StopIOService() {
  io_service_.stop();
  io_thread_.join();
  //  thread_group_.join_all();
}

void ObjectManager::SetClientID(const ClientID &client_id) { client_id_ = client_id; }

ClientID ObjectManager::GetClientID() { return client_id_; }

void ObjectManager::NotifyDirectoryObjectAdd(const ObjectID &object_id) {
  ray::Status status = object_directory_->ReportObjectAdded(object_id, client_id_);
}

void ObjectManager::NotifyDirectoryObjectDeleted(const ObjectID &object_id) {
  ray::Status status = object_directory_->ReportObjectRemoved(object_id, client_id_);
}

ray::Status ObjectManager::Terminate() {
  StopIOService();
  ray::Status status_code = object_directory_->Terminate();
  // TODO: evaluate store client termination status.
  store_client_->Terminate();
  return status_code;
};

ray::Status ObjectManager::SubscribeObjAdded(
    std::function<void(const ObjectID &)> callback) {
  store_client_->SubscribeObjAdded(callback);
  return ray::Status::OK();
};

ray::Status ObjectManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  store_client_->SubscribeObjDeleted(callback);
  return ray::Status::OK();
};

ray::Status ObjectManager::Pull(const ObjectID &object_id) {
  // TODO(hme): Need to correct. Workaround to get all pull requests on the same thread.
  SchedulePull(object_id, 0);
  return Status::OK();
};

void ObjectManager::SchedulePull(const ObjectID &object_id, int wait_ms) {
  pull_requests_[object_id] = Timer(new boost::asio::deadline_timer(
      io_service_, boost::posix_time::milliseconds(wait_ms)));
  pull_requests_[object_id]->async_wait(
      [this, object_id](const boost::system::error_code &error_code) {
        RAY_CHECK_OK(SchedulePullHandler(object_id));
      });
}

ray::Status ObjectManager::SchedulePullHandler(const ObjectID &object_id) {
  pull_requests_.erase(object_id);
  ray::Status status_code = object_directory_->GetLocations(
      object_id,
      [this](const std::vector<RemoteConnectionInfo> &vec, const ObjectID &object_id) {
        return GetLocationsSuccess(vec, object_id);
      },
      [this](ray::Status status, const ObjectID &object_id) {
        return GetLocationsFailed(status, object_id);
      });
  return status_code;
}

void ObjectManager::GetLocationsSuccess(const std::vector<ray::RemoteConnectionInfo> &vec,
                                        const ray::ObjectID &object_id) {
  RemoteConnectionInfo info = vec.front();
  pull_requests_.erase(object_id);
  ray::Status status_code = Pull(object_id, info.client_id);
};

void ObjectManager::GetLocationsFailed(ray::Status status, const ObjectID &object_id) {
  SchedulePull(object_id, config_.pull_timeout_ms);
};

ray::Status ObjectManager::Pull(const ObjectID &object_id, const ClientID &client_id) {
  Status status =
      GetMsgConnection(client_id, [this, object_id](SenderConnection::pointer client) {
        Status status = ExecutePull(object_id, client);
      });
  return status;
};

ray::Status ObjectManager::ExecutePull(const ObjectID &object_id,
                                       SenderConnection::pointer conn) {
  size_t message_type = OMMessageType_PullRequest;
  boost::system::error_code error_code;
  boost::asio::write(conn->GetSocket(),
                     boost::asio::buffer(&message_type, sizeof(message_type)),
                     error_code);
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePullRequest(fbb, fbb.CreateString(client_id_.binary()),
                                   fbb.CreateString(object_id.binary()));
  fbb.Finish(message);
  size_t length = fbb.GetSize();
  std::vector<boost::asio::const_buffer> buffer;
  buffer.push_back(boost::asio::buffer(&length, sizeof(length)));
  buffer.push_back(boost::asio::buffer(fbb.GetBufferPointer(), length));
  boost::asio::write(conn->GetSocket(), buffer);
  return ray::Status::OK();
};

ray::Status ObjectManager::Push(const ObjectID &object_id, const ClientID &client_id) {
  ray::Status status;
  status =
      GetTransferConnection(client_id, [this, object_id](SenderConnection::pointer conn) {
        ray::Status status = QueuePush(object_id, conn);
      });
  return status;
};

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

ray::Status ObjectManager::GetMsgConnection(
    const ClientID &client_id, std::function<void(SenderConnection::pointer)> callback) {
  ray::Status status = Status::OK();
  if (message_send_connections_.count(client_id) > 0) {
    callback(message_send_connections_[client_id]);
  } else {
    status = object_directory_->GetInformation(
        client_id,
        [this, callback](RemoteConnectionInfo info) {
          Status status = CreateMsgConnection(info, callback);
        },
        [this](const Status &status) {
          // TODO: deal with failure.
        });
  }
  return status;
};

ray::Status ObjectManager::CreateMsgConnection(
    const RemoteConnectionInfo &info,
    std::function<void(SenderConnection::pointer)> callback) {
  message_send_connections_.emplace(
      info.client_id, SenderConnection::Create(io_service_, info.ip, info.port));
  // Prepare client connection info buffer.
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = false;
  auto message =
      CreateClientConnectionInfo(fbb, fbb.CreateString(client_id_.binary()), is_transfer);
  fbb.Finish(message);
  // Pack into asio buffer.
  size_t length = fbb.GetSize();
  std::vector<boost::asio::const_buffer> buffer;
  buffer.push_back(boost::asio::buffer(&length, sizeof(length)));
  buffer.push_back(boost::asio::buffer(fbb.GetBufferPointer(), length));
  // Send synchronously.
  SenderConnection::pointer conn = message_send_connections_[info.client_id];
  boost::system::error_code error;
  boost::asio::write(conn->GetSocket(), buffer);
  // The connection is ready, invoke callback with connection info.
  callback(message_send_connections_[info.client_id]);
  return Status::OK();
};

ray::Status ObjectManager::GetTransferConnection(
    const ClientID &client_id, std::function<void(SenderConnection::pointer)> callback) {
  ray::Status status = Status::OK();
  if (transfer_send_connections_.count(client_id) > 0) {
    callback(transfer_send_connections_[client_id]);
  } else {
    status = object_directory_->GetInformation(
        client_id,
        [this, callback](RemoteConnectionInfo info) {
          Status status = CreateTransferConnection(info, callback);
        },
        [this](const Status &status) {
          // TODO(hme): deal with failure.
        });
  }
  return status;
};

ray::Status ObjectManager::CreateTransferConnection(
    const RemoteConnectionInfo &info,
    std::function<void(SenderConnection::pointer)> callback) {
  transfer_send_connections_.emplace(
      info.client_id, SenderConnection::Create(io_service_, info.ip, info.port));
  // Prepare client connection info buffer.
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = true;
  auto message =
      CreateClientConnectionInfo(fbb, fbb.CreateString(client_id_.binary()), is_transfer);
  fbb.Finish(message);
  // Pack into asio buffer.
  size_t length = fbb.GetSize();
  std::vector<boost::asio::const_buffer> buffer;
  buffer.push_back(boost::asio::buffer(&length, sizeof(length)));
  buffer.push_back(boost::asio::buffer(fbb.GetBufferPointer(), length));
  // Send synchronously.
  SenderConnection::pointer conn = transfer_send_connections_[info.client_id];
  boost::system::error_code ec;
  boost::asio::write(conn->GetSocket(), buffer, ec);
  callback(transfer_send_connections_[info.client_id]);
  return Status::OK();
};

ray::Status ObjectManager::AcceptConnection(TCPClientConnection::pointer conn) {
  boost::system::error_code ec;
  // read header
  size_t length;
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&length, sizeof(length)));
  boost::asio::read(conn->GetSocket(), header, ec);
  // read data
  std::vector<uint8_t> message;
  message.resize(length);
  boost::asio::read(conn->GetSocket(), boost::asio::buffer(message), ec);
  // Serialize
  auto info = flatbuffers::GetRoot<ClientConnectionInfo>(message.data());
  ClientID client_id = ObjectID::from_binary(info->client_id()->str());
  bool is_transfer = info->is_transfer();
  // TODO: trash connection if either fails.
  if (is_transfer) {
    transfer_receive_connections_[client_id] = conn;
    Status status = WaitPushReceive(conn);
    return status;
  } else {
    message_receive_connections_[client_id] = conn;
    Status status = WaitMessage(conn);
    return status;
  }
};

ray::Status ObjectManager::WaitPushReceive(TCPClientConnection::pointer conn) {
  boost::asio::async_read(
      conn->GetSocket(),
      boost::asio::buffer(&conn->message_length_, sizeof(conn->message_length_)),
      boost::bind(&ObjectManager::HandlePushReceive, this, conn,
                  boost::asio::placeholders::error));
  return ray::Status::OK();
}

void ObjectManager::HandlePushReceive(TCPClientConnection::pointer conn,
                                      BoostEC length_ec) {
  std::vector<uint8_t> message;
  message.resize(conn->message_length_);
  boost::system::error_code ec;
  boost::asio::read(conn->GetSocket(), boost::asio::buffer(message), ec);
  // Serialize.
  auto object_header = flatbuffers::GetRoot<ObjectHeader>(message.data());
  ObjectID object_id = ObjectID::from_binary(object_header->object_id()->str());
  int64_t object_size = (int64_t)object_header->object_size();
  int64_t metadata_size = 0;
  // Try to create shared buffer.
  std::shared_ptr<Buffer> data;
  arrow::Status s = store_client_->GetClient().Create(
      object_id.to_plasma_id(), object_size, NULL, metadata_size, &data);
  if (s.ok()) {
    // Read object into store.
    uint8_t *mutable_data = data->mutable_data();
    boost::asio::read(conn->GetSocket(), boost::asio::buffer(mutable_data, object_size),
                      ec);
    if (!ec.value()) {
      ARROW_CHECK_OK(store_client_->GetClient().Seal(object_id.to_plasma_id()));
      ARROW_CHECK_OK(store_client_->GetClient().Release(object_id.to_plasma_id()));
    } else {
      ARROW_CHECK_OK(store_client_->GetClient().Release(object_id.to_plasma_id()));
      ARROW_CHECK_OK(store_client_->GetClient().Abort(object_id.to_plasma_id()));
      RAY_LOG(ERROR) << "Receive Failed";
    }
  } else {
    RAY_LOG(ERROR) << "Buffer Create Failed: " << s.message();
    // Read object into empty buffer.
    uint8_t *mutable_data = (uint8_t *)malloc(object_size + metadata_size);
    boost::asio::read(conn->GetSocket(), boost::asio::buffer(mutable_data, object_size),
                      ec);
  }
  // Wait for another push.
  ray::Status ray_status = WaitPushReceive(conn);
};

ray::Status ObjectManager::QueuePush(const ObjectID &object_id_const,
                                     SenderConnection::pointer conn) {
  ObjectID object_id = ObjectID(object_id_const);
  if (conn->ObjectIdQueued(object_id)) {
    // For now, return with status OK if the object is already in the send queue.
    return ray::Status::OK();
  }
  conn->QueueObjectId(object_id);
  if (num_transfers_ < max_transfers_) {
    return ExecutePushQueue(conn);
  }
  return ray::Status::OK();
};

ray::Status ObjectManager::ExecutePushQueue(SenderConnection::pointer conn) {
  ray::Status status = ray::Status::OK();
  while (num_transfers_ < max_transfers_) {
    if (conn->IsObjectIdQueueEmpty()) {
      return ray::Status::OK();
    }
    ObjectID object_id = conn->DequeueObjectId();
    // The threads that increment/decrement num_transfers_ are different.
    // It's important to increment num_transfers_ before executing the push.
    num_transfers_ += 1;
    status = ExecutePushHeaders(object_id, conn);
  }
  return status;
};

ray::Status ObjectManager::ExecutePushHeaders(const ObjectID &object_id_const,
                                              SenderConnection::pointer conn) {
  ObjectID object_id = ObjectID(object_id_const);
  // Allocate and append the request to the transfer queue.
  plasma::ObjectBuffer object_buffer;
  plasma::ObjectID plasma_id = object_id.to_plasma_id();
  ARROW_CHECK_OK(store_client_->GetClientOther().Get(&plasma_id, 1, 0, &object_buffer));
  if (object_buffer.data_size == -1) {
    RAY_LOG(ERROR) << "Failed to get object";
    // If the object wasn't locally available, exit immediately. If the object
    // later appears locally, the requesting plasma manager should request the
    // transfer again.
    return ray::Status::IOError(
        "Unable to transfer object to requesting plasma manager, object not local.");
  }
  RAY_CHECK(object_buffer.metadata->data() ==
            object_buffer.data->data() + object_buffer.data_size);
  SendRequest send_request;
  send_request.object_id = object_id;
  send_request.object_size = object_buffer.data_size;
  send_request.data = const_cast<uint8_t *>(object_buffer.data->data());
  conn->AddSendRequest(object_id, send_request);
  // Create buffer.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateObjectHeader(fbb, fbb.CreateString(object_id.binary()),
                                    send_request.object_size);
  fbb.Finish(message);
  // Pack into asio buffer.
  size_t length = fbb.GetSize();
  std::vector<boost::asio::const_buffer> buffer;
  buffer.push_back(boost::asio::buffer(&length, sizeof(length)));
  buffer.push_back(boost::asio::buffer(fbb.GetBufferPointer(), length));
  // Send asynchronously.
  boost::asio::async_write(conn->GetSocket(), buffer,
                           boost::bind(&ObjectManager::ExecutePushObject, this, conn,
                                       object_id, boost::asio::placeholders::error));
  return ray::Status::OK();
};

void ObjectManager::ExecutePushObject(SenderConnection::pointer conn,
                                      const ObjectID &object_id,
                                      const boost::system::error_code &header_ec) {
  SendRequest &send_request = conn->GetSendRequest(object_id);
  boost::system::error_code ec;
  boost::asio::write(
      conn->GetSocket(),
      boost::asio::buffer(send_request.data, (size_t)send_request.object_size), ec);
  // Do this regardless of whether it failed or succeeded.
  ARROW_CHECK_OK(
      store_client_->GetClientOther().Release(send_request.object_id.to_plasma_id()));

  ray::Status ray_status = ExecutePushCompleted(object_id, conn);
}

ray::Status ObjectManager::ExecutePushCompleted(const ObjectID &object_id,
                                                SenderConnection::pointer conn) {
  conn->RemoveSendRequest(object_id);
  num_transfers_ -= 1;
  return ExecutePushQueue(conn);
};

ray::Status ObjectManager::WaitMessage(TCPClientConnection::pointer conn) {
  boost::asio::async_read(
      conn->GetSocket(),
      boost::asio::buffer(&conn->message_type_, sizeof(conn->message_type_)),
      boost::bind(&ObjectManager::HandleMessage, this, conn,
                  boost::asio::placeholders::error));
  return ray::Status::OK();
}

void ObjectManager::HandleMessage(TCPClientConnection::pointer conn, BoostEC msg_ec) {
  switch (conn->message_type_) {
  case OMMessageType_PullRequest:
    ReceivePullRequest(conn);
  }
}

void ObjectManager::ReceivePullRequest(TCPClientConnection::pointer conn) {
  boost::asio::read(
      conn->GetSocket(),
      boost::asio::buffer(&conn->message_length_, sizeof(conn->message_length_)));
  std::vector<uint8_t> message;
  message.resize(conn->message_length_);
  boost::system::error_code error_code;
  boost::asio::read(conn->GetSocket(), boost::asio::buffer(message), error_code);
  // Serialize.
  auto pull_request = flatbuffers::GetRoot<PullRequest>(message.data());
  ObjectID object_id = ObjectID::from_binary(pull_request->object_id()->str());
  ClientID client_id = ClientID::from_binary(pull_request->client_id()->str());
  // Push object to requesting client.
  ray::Status push_status = Push(object_id, client_id);
  ray::Status wait_status = WaitMessage(conn);
}

}  // namespace ray
