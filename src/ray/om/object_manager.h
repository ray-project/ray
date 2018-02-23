#ifndef RAY_OBJECTMANAGER_H
#define RAY_OBJECTMANAGER_H

#include "memory"
#include "cstdint"
#include "list"
#include "map"

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/plasma.h"
#include "plasma/events.h"
#include "plasma/protocol.h"
#include "plasma/client.h"

#include "ray/id.h"
#include "ray/status.h"

#include "object_directory.h"
#include "store_messenger.h"

using namespace std;

namespace ray {

struct OMConfig {
  int num_retries = 5;
  string store_socket_name;
};

struct Request {
  UniqueID id;
  ObjectID object_id;
  DBClientID dbclient_id;
};

class ObjectManager {
 public:

  // Callback signatures for Push and Pull. Please keep until we're certain
  // they will not be necessary (hme).
  using TransferCallback = function<void(ray::Status,
                                         const ray::ObjectID&,
                                         const ray::DBClientID&)>;

  using WaitCallback = function<void(const ray::Status,
                                     uint64_t,
                                     const list<ray::ObjectID>&)>;

  // Instantiates Ray implementation of ObjectDirectory.
  explicit ObjectManager(boost::asio::io_service &io_service, OMConfig *config=nullptr);

  // Takes user-defined ObjectDirectoryInterface implementation.
  // When this constructor is used, the ObjectManager assumes ownership of
  // the given ObjectDirectory instance.
  explicit ObjectManager(boost::asio::io_service &io_service, shared_ptr<ObjectDirectoryInterface> od, OMConfig *config=nullptr);

  // Subscribe to notifications of objects added to local store.
  // Upon subscribing, the callback will be invoked for all objects that
  // already exist in the local store.
  Status SubscribeObjAdded(void (*callback)(const ObjectID&));

  // Subscribe to notifications of objects deleted from local store.
  Status SubscribeObjDeleted(void (*callback)(const ObjectID&));

  // Push an object to DBClientID. Returns UniqueID associated with
  // an invocation of this method.
  UniqueID Push(const ObjectID &object_id,
                const DBClientID &dbclient_id);

  // Pull an object from DBClientID. Returns UniqueID associated with
  // an invocation of this method.
  UniqueID Pull(const ObjectID &object_id);

  // Discover DBClientID via ObjectDirectory, then pull object
  // from DBClientID associated with ObjectID.
  // Returns UniqueID associated with an invocation of this method.
  UniqueID Pull(const ObjectID &object_id,
                const DBClientID &dbclient_id);

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

  void Terminate();

 private:
  OMConfig config;
  shared_ptr<ObjectDirectoryInterface> od;
  unique_ptr<StoreMessenger> sm;

  // map<UniqueID, Request, UniqueIDHasher> getloc_requests;
  vector<Request> send_queue;
  vector<Request> receive_queue;

  void GetLocationsResult(ray::Status status,
                          const ObjectID &object_id,
                          const UniqueID &id,
                          const vector<DBClientID>& v);

  void ExecutePull(const ObjectID &object_id,
                   const DBClientID &dbclient_id,
                   const UniqueID &invocation_id);

  void ExecutePush(const ObjectID &object_id,
                   const DBClientID &dbclient_id,
                   const UniqueID &invocation_id);

};

} // end namespace

#endif // RAY_OBJECTMANAGER_H
