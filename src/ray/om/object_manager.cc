#include <iostream>

#include "object_directory.h"
#include "object_manager.h"


ObjectManager::ObjectManager(const ObjectDirectory *object_directory){
  this->object_directory = object_directory;
};

ray::Status ObjectManager::SubscribeObjectAdded(const void *callback) {
  ray::Status status = ray::Status();
  return status;
};

ray::Status ObjectManager::SubscribeObjectDeleted(const void *callback) {
  ray::Status status = ray::Status();
  return status;
};

ray::RequestID ObjectManager::Push(const ray::ObjectID &object_id,
                                   const ray::DBClientID &dbclient_id,
                                   const void *callback) {
  ray::RequestID id = ray::RequestID();
  return id;
};

ray::RequestID ObjectManager::Pull(const ray::ObjectID &object_id,
                                   const ray::DBClientID &dbclient_id,
                                   const void *callback) {
  ray::RequestID id = ray::RequestID();
  return id;
};

ray::Status ObjectManager::Cancel(const ray::RequestID &request_id) {
  ray::Status status = ray::Status();
  return status;
};

ray::Status ObjectManager::Wait(const std::list<ray::ObjectID> *object_ids,
                                uint64_t timeout_ms,
                                int num_ready_objects,
                                const void *callback) {
  ray::Status status = ray::Status();
  return status;
};
