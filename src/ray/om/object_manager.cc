#include "store_messenger.h"
#include "object_directory.h"
#include "object_manager.h"

// #include "common_protocol.h"
using namespace std;

namespace ray {

ObjectManager::ObjectManager(boost::asio::io_service &io_service,
                             OMConfig *config) {
  this->config = config == nullptr? OMConfig() : *config;
  this->sm = unique_ptr<StoreMessenger>(new StoreMessenger(io_service, config->store_socket_name));
  this->od = unique_ptr<ObjectDirectory>(new ObjectDirectory());
};

ObjectManager::ObjectManager(boost::asio::io_service &io_service,
                             shared_ptr<ObjectDirectoryInterface> od,
                             OMConfig *config) {
  this->config = config == nullptr? OMConfig() : *config;
  this->sm = unique_ptr<StoreMessenger>(new StoreMessenger(io_service, config->store_socket_name));
  this->od = od;
};

void ObjectManager::Terminate() {
  this->od->Terminate();
  this->sm->Terminate();
};

Status ObjectManager::SubscribeObjAdded(void (*callback)(const ObjectID&)) {
  return this->sm->SubscribeObjAdded(callback);
};

Status ObjectManager::SubscribeObjDeleted(void (*callback)(const ObjectID&)) {
  return this->sm->SubscribeObjDeleted(callback);
};

UniqueID ObjectManager::Push(const ObjectID &object_id,
                             const DBClientID &dbclient_id) {
  UniqueID id = UniqueID().from_random();
  this->ExecutePush(object_id, dbclient_id, id);
  return id;
};

UniqueID ObjectManager::Pull(const ObjectID &object_id) {
  UniqueID id = UniqueID().from_random();
  // Request req = {id, object_id};
  // getloc_requests[id] = req;
  // this->od->GetLocations(object_id, GetLocationsResult, &id);
  return id;
};

void ObjectManager::GetLocationsResult(ray::Status status,
                                       const ObjectID &object_id,
                                       const UniqueID &id,
                                       const vector<DBClientID>& v) {
  // Request req = getloc_requests[id];
  // req.dbclient_id = v[0];
  // getloc_requests.erase(id);
  this->ExecutePull(object_id, v.front(), id);
};

UniqueID ObjectManager::Pull(const ObjectID &object_id,
                             const DBClientID &dbclient_id) {
  UniqueID id = UniqueID().from_random();
  this->ExecutePull(object_id, dbclient_id, id);
  return id;
};

void ObjectManager::ExecutePull(const ObjectID &object_id,
                                const DBClientID &dbclient_id,
                                const UniqueID &invocation_id) {

};

void ObjectManager::ExecutePush(const ObjectID &object_id,
                                const DBClientID &dbclient_id,
                                const UniqueID &invocation_id) {

};

Status ObjectManager::Cancel(const UniqueID &request_id) {
  return Status::OK();
};

Status ObjectManager::Wait(const list<ObjectID> &object_ids,
                           uint64_t timeout_ms,
                           int num_ready_objects,
                           const WaitCallback &callback) {
  return Status::OK();
};

} // end ray
