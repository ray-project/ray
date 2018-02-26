#include <iostream>
#include "object_store_client.h"
#include "object_directory.h"
#include "object_manager.h"

using namespace std;

namespace ray {

ObjectManager::ObjectManager(boost::asio::io_service &io_service, OMConfig config) :
    work_(io_service_) {

  this->store_client_ = unique_ptr<ObjectStoreClient>(new ObjectStoreClient(io_service, config.store_socket_name));
  this->od = unique_ptr<ObjectDirectory>(new ObjectDirectory());
  this->StartIOService();
};

ObjectManager::ObjectManager(boost::asio::io_service &io_service,
                             OMConfig config,
                             shared_ptr<ObjectDirectoryInterface> od) :
    work_(io_service_) {
  this->store_client_ = unique_ptr<ObjectStoreClient>(new ObjectStoreClient(io_service, config.store_socket_name));
  this->od = od;
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
  ray::Status status_code = this->od->GetLocations(
      object_id,
      [this](const vector<ODRemoteConnectionInfo>& v, const ObjectID &object_id) {
        return this->GetLocationsSuccess(v, object_id);
      } ,
      [this](ray::Status status, const ObjectID &object_id) {
        return this->GetLocationsFailed(status, object_id);
      }
  );
  return status_code;
};

// Private callback implementation for success on get location. Called inside OD.
void ObjectManager::GetLocationsSuccess(const vector<ray::ODRemoteConnectionInfo> &v,
                                        const ray::ObjectID &object_id) {
  const ODRemoteConnectionInfo &info = v.front();
  ray::Status status_code = this->Pull(object_id, info.client_id);
};

// Private callback implementation for failure on get location. Called inside OD.
void ObjectManager::GetLocationsFailed(ray::Status status,
                                       const ObjectID &object_id){
  throw std::runtime_error("GetLocations Failed: " + status.message());
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
                                       SenderConnection::pointer client) {
  cout << "ExecutePull: " << object_id.hex() << endl;
  // TODO: implement pull.
  return ray::Status::OK();
};

ray::Status ObjectManager::Push(const ObjectID &object_id,
                                const ClientID &client_id) {
  // cout << "Push: " << object_id.hex() << " " << client_id.hex() << endl;
  GetTransferConnection(
      client_id,
      [this, object_id](SenderConnection::pointer client){
        ExecutePush(object_id, client).ok();
      });
  return Status::OK();
};

ray::Status ObjectManager::ExecutePush(const ObjectID &object_id,
                                       SenderConnection::pointer client) {
  cout << "ExecutePush: " << object_id.hex() << endl;
  return ray::Status::OK();
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
                               // deal with failure.
                             });
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

  if (is_transfer) {
    message_receive_connections_[client_id] = conn;
  } else {
    transfer_receive_connections_[client_id] = conn;
  }

  return ray::Status::OK();
};

//ProcessPushReceive(){
//
//}

} // end ray
