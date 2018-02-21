#ifndef RAY_OBJECTMANAGER_H
#define RAY_OBJECTMANAGER_H

#include "memory"
#include "cstdint"
#include "list"

#include "ray/id.h"
#include "ray/status.h"

#include "object_directory.h"

using namespace std;

namespace ray {

class ObjectManager {
 public:

  // Callback for Push and Pull.
  using TransferCallback = function<void(ray::Status,
                                         const ray::ObjectID&,
                                         const ray::DBClientID&)>;

  using WaitCallback = function<void(const ray::Status,
                                     uint64_t,
                                     const list<ray::ObjectID>&)>;

  // Instantiates Ray implementation of ObjectDirectory.
  explicit ObjectManager();

  // Takes user-defined ObjectDirectoryInterface implementation.
  // When this constructor is used, the ObjectManager assumes ownership of
  // the given ObjectDirectory instance.
  explicit ObjectManager(shared_ptr<ObjectDirectoryInterface> od);

  // Subscribe to notifications of objects added to local store.
  // Upon subscribing, the callback will be invoked for all objects that
  // already exist in the local store.
  Status SubscribeObjAdded(void (*callback)(const ObjectID&));

  // Subscribe to notifications of objects deleted from local store.
  Status SubscribeObjDeleted(void (*callback)(const ObjectID&));

  // Push an object to DBClientID. Returns UniqueID associated with
  // an invocation of this method.
  UniqueID Push(const ObjectID &object_id,
                const DBClientID &dbclient_id,
                const TransferCallback &callback);

  // Discover DBClientID via ObjectDirectory, then pull object
  // from DBClientID associated with ObjectID.
  // Returns UniqueID associated with an invocation of this method.
  UniqueID Pull(const ObjectID &object_id,
                const TransferCallback &callback);

  // Pull an object from DBClientID. Returns UniqueID associated with
  // an invocation of this method.
  UniqueID Pull(const ObjectID &object_id,
                DBClientID &dbclient_id,
                const TransferCallback &callback);

  // Push and Pull return UniqueID. This method cancels invocations
  // of those methods.
  Status Cancel(const UniqueID &callback_id);

  // Wait for timeout_ms before invoking the provided callback.
  // If num_ready_objects is satisfied before the timeout, then
  // invoke the callback.
  Status Wait(const list<ObjectID> &object_ids,
              uint64_t timeout_ms,
              int num_ready_objects,
              const WaitCallback &callback);

 private:
  shared_ptr<ObjectDirectoryInterface> od;

};

} // end namespace

#endif // RAY_OBJECTMANAGER_H
