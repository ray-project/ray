#include <iostream>

#include "object_directory.h"
#include "object_manager.h"

namespace ray {

ObjectManager::ObjectManager(const ObjectDirectory *object_directory) {
  this->object_directory = object_directory;
};

Status ObjectManager::SubscribeObjectAdded(const void *callback) {
  Status status = Status();
  return status;
};

Status ObjectManager::SubscribeObjectDeleted(const void *callback) {
  Status status = Status();
  return status;
};

RequestID ObjectManager::Push(const ObjectID &object_id,
                              const DBClientID &dbclient_id,
                              const void *callback) {
  RequestID id = RequestID();
  return id;
};

RequestID ObjectManager::Pull(const ObjectID &object_id,
                              const DBClientID &dbclient_id,
                              const void *callback) {
  RequestID id = RequestID();
  return id;
};

Status ObjectManager::Cancel(const RequestID &request_id) {
  Status status = Status();
  return status;
};

Status ObjectManager::Wait(const std::list<ObjectID> *object_ids,
                           uint64_t timeout_ms,
                           int num_ready_objects,
                           const void *callback) {
  Status status = Status();
  return status;
};

} // end ray
