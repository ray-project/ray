#include "object_store_client.h"
#include "object_directory.h"
#include "object_manager.h"

// #include "common_protocol.h"
using namespace std;

namespace ray {

ObjectManager::ObjectManager(boost::asio::io_service &io_service,
                             OMConfig config) {
  this->store_client = unique_ptr<ObjectStoreClient>(new ObjectStoreClient(io_service, config.store_socket_name));
  this->od = unique_ptr<ObjectDirectory>(new ObjectDirectory());
};

ObjectManager::ObjectManager(boost::asio::io_service &io_service,
                             OMConfig config,
                             shared_ptr<ObjectDirectoryInterface> od) {
  this->store_client = unique_ptr<ObjectStoreClient>(new ObjectStoreClient(io_service, config.store_socket_name));
  this->od = od;
};

ray::Status ObjectManager::Terminate() {
  this->od->Terminate();
  this->store_client->Terminate();
  return ray::Status::OK();
};

//ray::Status ObjectManager::SubscribeObjAdded(void (*callback)(const ObjectID&)) {
ray::Status ObjectManager::SubscribeObjAdded(std::function<void(const ObjectID&)> callback) {
  this->store_client->SubscribeObjAdded(callback);
  return ray::Status::OK();
};

ray::Status ObjectManager::SubscribeObjDeleted(std::function<void(const ObjectID&)> callback) {
  this->store_client->SubscribeObjDeleted(callback);
  return ray::Status::OK();
};

ray::Status ObjectManager::Push(const ObjectID &object_id,
                         const ClientID &dbclient_id) {
  this->ExecutePush(object_id, dbclient_id);
  return ray::Status::OK();
};

ray::Status ObjectManager::Pull(const ObjectID &object_id) {
  this->od->GetLocations(object_id,
                         [this](const vector<ODRemoteConnectionInfo>& v,
                                const ObjectID &object_id) {
                           return this->GetLocationsSuccess(v, object_id);
                         } ,
                         [this](ray::Status status,
                                const ObjectID &object_id) {
                           return this->GetLocationsFailed(status, object_id);
                         });
  return ray::Status::OK();
};
//                         bind(&ObjectManager::GetLocationsSuccess, this,
//                              placeholders::_1,
//                              placeholders::_2),
//                         bind(&ObjectManager::GetLocationsFailed, this,
//                              placeholders::_1,
//                              placeholders::_2));


// Private callback implementation for success on get location. Called inside OD.
void ObjectManager::GetLocationsSuccess(const vector<ray::ODRemoteConnectionInfo> &v,
                                        const ray::ObjectID &object_id) {
  this->ExecutePull(object_id, v.front().client_id);
};

// Private callback impelmentation for failure on get location. Called inside OD.
void ObjectManager::GetLocationsFailed(ray::Status status,
                                       const ObjectID &object_id){
  throw std::runtime_error("GetLocations Failed.");
};

ray::Status ObjectManager::Pull(const ObjectID &object_id,
                                const ClientID &dbclient_id) {
  return this->ExecutePull(object_id, dbclient_id);
};

ray::Status ObjectManager::ExecutePull(const ObjectID &object_id,
                                       const ClientID &dbclient_id) {
  // TODO(hme): Lookup connection and pull.
  return ray::Status::OK();
};

ray::Status ObjectManager::ExecutePush(const ObjectID &object_id,
                                       const ClientID &dbclient_id) {
  // TODO(hme): Lookup connection and push.
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

} // end ray
