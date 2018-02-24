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
                             shared_ptr<ObjectDirectoryInterface> od,
                             OMConfig config) {
  this->store_client = unique_ptr<ObjectStoreClient>(new ObjectStoreClient(io_service, config.store_socket_name));
  this->od = od;
};

void ObjectManager::Terminate() {
  this->od->Terminate();
  this->store_client->Terminate();
};

void ObjectManager::SubscribeObjAdded(void (*callback)(const ObjectID&)) {
  this->store_client->SubscribeObjAdded(callback);
};

void ObjectManager::SubscribeObjDeleted(void (*callback)(const ObjectID&)) {
  this->store_client->SubscribeObjDeleted(callback);
};

void ObjectManager::Push(const ObjectID &object_id,
                         const ClientID &dbclient_id) {
  this->ExecutePush(object_id, dbclient_id);
};

void ObjectManager::Pull(const ObjectID &object_id) {
  this->od->GetLocations(object_id,
                         bind(&ObjectManager::GetLocationsSuccess, this,
                              placeholders::_1,
                              placeholders::_2),
                         bind(&ObjectManager::GetLocationsFailed, this,
                              placeholders::_1,
                              placeholders::_2));
};

void ObjectManager::GetLocationsSuccess(const vector<ray::ODRemoteConnectionInfo> &v,
                                        const ray::ObjectID &object_id) {
  this->ExecutePull(object_id, v.front().dbc_id);
};

void ObjectManager::GetLocationsFailed(Status status,
                                       const ObjectID &object_id){
  throw std::runtime_error("GetLocations Failed.");
};

void ObjectManager::Pull(const ObjectID &object_id,
                         const ClientID &dbclient_id) {
  this->ExecutePull(object_id, dbclient_id);
};

void ObjectManager::ExecutePull(const ObjectID &object_id,
                                const ClientID &dbclient_id) {
  // TODO(hme): Lookup connection and pull.
};

void ObjectManager::ExecutePush(const ObjectID &object_id,
                                const ClientID &dbclient_id) {
  // TODO(hme): Lookup connection and push.
};

void ObjectManager::Cancel(const ObjectID &object_id) {
  this->od->Cancel(object_id);
};

void ObjectManager::Wait(const list<ObjectID> &object_ids,
                           uint64_t timeout_ms,
                           int num_ready_objects,
                           const WaitCallback &callback) {
};

} // end ray
