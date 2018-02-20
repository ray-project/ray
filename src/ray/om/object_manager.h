#ifndef RAY_OBJECTMANAGER_H
#define RAY_OBJECTMANAGER_H

#include "cstdint"
#include "list"

#include "ray/id.h"
#include "ray/status.h"

#include "object_directory.h"

namespace ray {

typedef UniqueID RequestID;

// TODO(hme): Document.
class ObjectManager {
 public:
  explicit ObjectManager(const ObjectDirectory *object_directory);

  Status SubscribeObjectAdded(const void *callback);
  Status SubscribeObjectDeleted(const void *callback);
  RequestID Push(const ObjectID &object_id,
                 const DBClientID &dbclient_id,
                 const void *callback);
  RequestID Pull(const ObjectID &object_id,
                 const DBClientID &dbclient_id,
                 const void *callback);
  Status Cancel(const RequestID &request_id);
  Status Wait(const std::list<ObjectID> *object_ids,
              uint64_t timeout_ms,
              int num_ready_objects,
              const void *callback);

 private:
  const ObjectDirectory *object_directory;
};

} // end namespace

#endif // RAY_OBJECTMANAGER_H
