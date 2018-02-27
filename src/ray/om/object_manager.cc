#include <iostream>
#include "object_store_client.h"
#include "object_directory.h"
#include "object_manager.h"

using namespace std;

namespace ray {

ObjectManager::ObjectManager(boost::asio::io_service &io_service, OMConfig config) :
    work_(io_service_)
{

  this->od = unique_ptr<ObjectDirectory>(new ObjectDirectory());
  this->store_client_ = unique_ptr<ObjectStoreClient>(new ObjectStoreClient(io_service, config.store_socket_name, od));
  this->StartIOService();
};

ObjectManager::ObjectManager(boost::asio::io_service &io_service,
                             OMConfig config,
                             shared_ptr<ObjectDirectoryInterface> od) :
    work_(io_service_)
{
  this->od = od;
  this->store_client_ = unique_ptr<ObjectStoreClient>(new ObjectStoreClient(io_service, config.store_socket_name, od));
  this->StartIOService();
};

void ObjectManager::StartIOService(){
  this->io_thread_ = std::thread(&ObjectManager::IOServiceLoop, this);
//  this->thread_group_.create_thread(boost::bind(&boost::asio::io_service::run, &io_service_));
}

void ObjectManager::IOServiceLoop(){
   this->io_service_.run();
}

void ObjectManager::StopIOService(){
  this->io_service_.stop();
  this->io_thread_.join();
//  this->thread_group_.join_all();
}

void ObjectManager::SetClientID(const ClientID &client_id){
  this->client_id_ = client_id;
  this->store_client_->SetClientID(client_id);
}

ClientID ObjectManager::GetClientID(){
  return this->client_id_;
}

ray::Status ObjectManager::Terminate() {
  StopIOService();
  ray::Status status_code = this->od->Terminate();
  // TODO: evaluate store client termination status.
  this->store_client_->Terminate();
  return status_code;
};

ray::Status ObjectManager::SubscribeObjAdded(std::function<void(const ObjectID&)> callback) {
  this->store_client_->SubscribeObjAdded(callback);
  return ray::Status::OK();
};

ray::Status ObjectManager::SubscribeObjDeleted(std::function<void(const ObjectID&)> callback) {
  this->store_client_->SubscribeObjDeleted(callback);
  return ray::Status::OK();
};

ray::Status ObjectManager::Pull(const ObjectID &object_id) {
  // cout << "Pull " << object_id.hex() << endl;
  // TODO(hme): Need to correct. Workaround to get all pull requests on the same thread.
  SchedulePull(object_id, 0);
  return Status::OK();
};

// Private callback implementation for success on get location. Called inside OD.
void ObjectManager::GetLocationsSuccess(const vector<ray::ODRemoteConnectionInfo> &v,
                                        const ray::ObjectID &object_id) {
  // cout << "GetLocationsSuccess " << v.size() << endl;
  ODRemoteConnectionInfo info = v.front();
  pull_requests_.erase(object_id);
  ray::Status status_code = this->Pull(object_id, info.client_id);
};

// Private callback implementation for failure on get location. Called inside OD.
void ObjectManager::GetLocationsFailed(ray::Status status,
                                       const ObjectID &object_id){
  // cout << "GetLocationsFailed" << endl;
  SchedulePull(object_id, config.pull_timeout_ms);
};

ray::Status ObjectManager::Pull(const ObjectID &object_id,
                                const ClientID &client_id) {
  GetMsgConnection(
      client_id,
      [this, object_id](SenderConnection::pointer client){
        ExecutePull(object_id, client);
      });
  return Status::OK();
};

ray::Status ObjectManager::ExecutePull(const ObjectID &object_id,
                                       SenderConnection::pointer conn) {
  // cout << "ExecutePull: " << object_id.hex() << endl;
  SendPullRequest(object_id, conn);
  return ray::Status::OK();
};

ray::Status ObjectManager::Push(const ObjectID &object_id,
                                const ClientID &client_id) {
  // cout << "Push: " << object_id.hex() << " " << client_id.hex() << endl;
  GetTransferConnection(
      client_id,
      [this, object_id](SenderConnection::pointer conn){
        QueuePush(object_id, conn).ok();
      });
  return Status::OK();
};

ray::Status ObjectManager::Cancel(const ObjectID &object_id) {
  ray::Status status = this->od->Cancel(object_id);
  return ray::Status::OK();
};

ray::Status ObjectManager::Wait(const vector<ObjectID> &object_ids,
                                uint64_t timeout_ms,
                                int num_ready_objects,
                                const WaitCallback &callback) {
  return ray::Status::OK();
};

ray::Status ObjectManager::GetMsgConnection(const ClientID &client_id,
                                            std::function<void(SenderConnection::pointer)> callback){
  if(message_send_connections_.count(client_id) > 0){
    callback(message_send_connections_[client_id]);
  } else {
    this->od->GetInformation(client_id,
                             [this, callback](ODRemoteConnectionInfo info){
                               CreateMsgConnection(info, callback);
                             },
                             [this](Status status){
                               // deal with failure.
                             });
  }
  return Status::OK();
};

ray::Status ObjectManager::CreateMsgConnection(const ODRemoteConnectionInfo &info,
                                               std::function<void(SenderConnection::pointer)> callback){
  message_send_connections_.emplace(info.client_id, SenderConnection::Create(io_service_, info));

  // Prepare client connection info buffer.
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = false;
  auto message = CreateClientConnectionInfo(fbb, fbb.CreateString(client_id_.binary()), is_transfer);
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

ray::Status ObjectManager::GetTransferConnection(const ClientID &client_id,
                                                 std::function<void(SenderConnection::pointer)> callback) {
  if (transfer_send_connections_.count(client_id) > 0) {
    callback(transfer_send_connections_[client_id]);
  } else {
    this->od->GetInformation(client_id,
                             [this, callback](ODRemoteConnectionInfo info){
                               CreateTransferConnection(info, callback);
                             },
                             [this](Status status){
                               // TODO(hme): deal with failure.
                             }
    );
  }
  return Status::OK();
};

ray::Status ObjectManager::CreateTransferConnection(const ODRemoteConnectionInfo &info,
                                                    std::function<void(SenderConnection::pointer)> callback){

  transfer_send_connections_.emplace(info.client_id, SenderConnection::Create(io_service_, info));

  // Prepare client connection info buffer.
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = true;
  auto message = CreateClientConnectionInfo(fbb, fbb.CreateString(client_id_.binary()), is_transfer);
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

  // cout << "CreateTransferConnection: " << info.client_id.hex() << endl;
  // cout << conn->GetSocket().local_endpoint().address() << endl;
  // cout << conn->GetSocket().local_endpoint().port() << endl;

  callback(transfer_send_connections_[info.client_id]);
  return Status::OK();
};

ray::Status ObjectManager::AddSock(TCPClientConnection::pointer conn){
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

  // cout << "AddSock: " << client_id.hex() << " " << is_transfer << endl;
  // cout << conn->GetSocket().local_endpoint().address() << endl;
  // cout << conn->GetSocket().local_endpoint().port() << endl;

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

ray::Status ObjectManager::WaitPushReceive(TCPClientConnection::pointer conn){
  // cout << "WaitPushReceive" << endl;
  boost::asio::async_read(conn->GetSocket(),
                          boost::asio::buffer(&conn->message_length_, sizeof(conn->message_length_)),
                          boost::bind(&ObjectManager::HandlePushReceive,
                                      this,
                                      conn,
                                      boost::asio::placeholders::error));
  return ray::Status::OK();
}

void ObjectManager::HandlePushReceive(TCPClientConnection::pointer conn,
                                      const boost::system::error_code& length_ec){
  std::vector<uint8_t> message;
  message.resize(conn->message_length_);
  boost::system::error_code ec;
  boost::asio::read(conn->GetSocket(), boost::asio::buffer(message), ec);

  // Serialize
  auto object_header = flatbuffers::GetRoot<ObjectHeader>(message.data());
  ObjectID object_id = ObjectID::from_binary(object_header->object_id()->str());
  int64_t object_size = (int64_t) object_header->object_size();
  int64_t metadata_size = 0;

  cout << "HandlePushReceive: " << object_id.hex() << " " << object_size << endl;

  // Try to create shared buffer.
  std::shared_ptr<Buffer> data;
  arrow::Status s = store_client_->GetClient()->Create(object_id.to_plasma_id(), object_size, NULL, metadata_size, &data);

  if(s.ok()){
    // cout << "Buffer Create Succeeded" << endl;
    // Read object into store.
    uint8_t *mutable_data = data->mutable_data();
    boost::asio::read(conn->GetSocket(), boost::asio::buffer(&mutable_data, object_size), ec);
    if(!ec.value()){
      ARROW_CHECK_OK(store_client_->GetClient()->Seal(object_id.to_plasma_id()));
      ARROW_CHECK_OK(store_client_->GetClient()->Release(object_id.to_plasma_id()));
      // cout << "Receive Succeeded" << endl;
    } else {
      ARROW_CHECK_OK(store_client_->GetClient()->Release(object_id.to_plasma_id()));
      ARROW_CHECK_OK(store_client_->GetClient()->Abort(object_id.to_plasma_id()));
      cout << "Receive Failed" << endl;
    }
  } else {
    cout << "Buffer Create Failed: " << s.message() << endl;
    // Read object into empty buffer.
    uint8_t *mutable_data = (uint8_t *) malloc(object_size + metadata_size);
    boost::asio::read(conn->GetSocket(), boost::asio::buffer(mutable_data, object_size), ec);
  }

  // Wait for another push.
  WaitPushReceive(conn);
};

ray::Status ObjectManager::QueuePush(const ObjectID &object_id_const,
                                     SenderConnection::pointer conn){
  // cout << "QueuePush: " << object_id_const.hex() << endl;

  ObjectID object_id = ObjectID(object_id_const);
  if(conn->ObjectIdQueued(object_id)){
    // For now, return with status OK if the object is already in the send queue.
    return ray::Status::OK();
  }

  conn->QueueObjectId(object_id);
  if(num_transfers_ < max_transfers_){
    return ExecutePushQueue(conn);
  }
  return ray::Status::OK();
};

ray::Status ObjectManager::ExecutePushQueue(SenderConnection::pointer conn){
  while(num_transfers_ < max_transfers_){
    if (conn->IsObjectIdQueueEmpty()){
      return ray::Status::OK();
    }
    ObjectID object_id = conn->DeQueueObjectId();
    // cout << "ExecutePushQueue: " << object_id.hex() << endl;
    // Note: The threads that increment/decrement num_transfers_ are different.
    // It's important to increment num_transfers_ before executing the push.
    num_transfers_ += 1;
    ExecutePush(object_id, conn);
  }
  return ray::Status::OK();
};

ray::Status ObjectManager::ExecutePush(const ObjectID &object_id_const,
                                       SenderConnection::pointer conn) {
  // cout << "ExecutePush: " << object_id_const.hex() << endl;
  ObjectID object_id = ObjectID(object_id_const);

  /* Allocate and append the request to the transfer queue. */
  plasma::ObjectBuffer object_buffer;
  plasma::ObjectID plasma_id = object_id.to_plasma_id();
  ARROW_CHECK_OK(store_client_->GetClientOther()->Get(&plasma_id, 1, 0, &object_buffer));
  if (object_buffer.data_size == -1) {
    cout << "Failed to get object" << endl;
    /* If the object wasn't locally available, exit immediately. If the object
     * later appears locally, the requesting plasma manager should request the
     * transfer again. */
    return ray::Status::IOError("Unable to transfer object to requesting plasma manager, object not local.");
  }

  ARROW_CHECK(object_buffer.metadata->data() == object_buffer.data->data() + object_buffer.data_size);

  SendRequest send_request;
  send_request.object_id = object_id;
  send_request.object_size = object_buffer.data_size;
  send_request.data = const_cast<uint8_t *>(object_buffer.data->data());
  conn->AddSendRequest(object_id, send_request);

  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateObjectHeader(fbb, fbb.CreateString(object_id.binary()), send_request.object_size);
  fbb.Finish(message);

  // Pack into asio buffer.
  size_t length = fbb.GetSize();
  std::vector<boost::asio::const_buffer> buffer;
  buffer.push_back(boost::asio::buffer(&length, sizeof(length)));
  buffer.push_back(boost::asio::buffer(fbb.GetBufferPointer(), length));

  // Send asynchronously.
  boost::asio::async_write(conn->GetSocket(),
                           buffer,
                           boost::bind(&ObjectManager::HandlePushSend,
                                       this,
                                       conn,
                                       object_id,
                                       boost::asio::placeholders::error));

  return ray::Status::OK();
};

void ObjectManager::HandlePushSend(SenderConnection::pointer conn,
                                   const ObjectID &object_id,
                                   const boost::system::error_code &header_ec){
  SendRequest &send_request = conn->GetSendRequest(object_id);
  // cout << "HandlePushSend: " << object_id.hex() << " " << send_request.object_size << endl;

  boost::system::error_code ec;
  boost::asio::write(conn->GetSocket(),
                     boost::asio::buffer(send_request.data, (size_t) send_request.object_size),
                     ec);
  // Do this regardless of whether it failed or succeeded.
  ARROW_CHECK_OK(store_client_->GetClientOther()->Release(send_request.object_id.to_plasma_id()));

  ExecutePushCompleted(object_id, conn);
}

ray::Status ObjectManager::ExecutePushCompleted(const ObjectID &object_id,
                                                SenderConnection::pointer conn){
  // cout << "ExecutePushCompleted: " << object_id.hex() << endl;
  conn->RemoveSendRequest(object_id);
  num_transfers_ -= 1;
  return ExecutePushQueue(conn);
};

} // end ray
