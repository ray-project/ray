#ifndef RAY_OBJECTMANAGER_H
#define RAY_OBJECTMANAGER_H

#include "cstdint"
#include "list"

#include "ray/id.h"
#include "ray/status.h"

#include "object_directory.h"

namespace ray {
  typedef UniqueID RequestID;
}

// TODO(hme): Document.
class ObjectManager {
 public:
  explicit ObjectManager(const ObjectDirectory *object_directory);

  ray::Status SubscribeObjectAdded(const void *callback);
  ray::Status SubscribeObjectDeleted(const void *callback);
  ray::RequestID Push(const ray::ObjectID &object_id,
                      const ray::DBClientID &dbclient_id,
                      const void *callback);
  ray::RequestID Pull(const ray::ObjectID &object_id,
                      const ray::DBClientID &dbclient_id,
                      const void *callback);
  ray::Status Cancel(const ray::RequestID &request_id);
  ray::Status Wait(const std::list<ray::ObjectID> *object_ids,
                   uint64_t timeout_ms,
                   int num_ready_objects,
                   const void *callback);

 private:
  const ObjectDirectory *object_directory;
};

#endif