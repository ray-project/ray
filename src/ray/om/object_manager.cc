#include "object_directory.h"
#include "object_manager.h"

using namespace std;

namespace ray {

ObjectManager::ObjectManager() {
  this->od = unique_ptr<ObjectDirectory>(new ObjectDirectory());
}

ObjectManager::ObjectManager(shared_ptr<ObjectDirectoryInterface> od) {
  this->od = od;
};

Status ObjectManager::SubscribeObjAdded(void (*callback)(const ObjectID&)) {
  return Status::OK();
};

Status ObjectManager::SubscribeObjDeleted(void (*callback)(const ObjectID&)) {
  return Status::OK();
};

UniqueID ObjectManager::Push(const ObjectID &object_id,
                             const DBClientID &dbclient_id,
                             const TransferCallback &callback) {
  return UniqueID().from_random();
};

UniqueID ObjectManager::Pull(const ObjectID &object_id,
                             const TransferCallback &callback) {
  // TODO(hme): Implement object lookup mechanism.
  using Callback = function<void(ray::Status, const vector<ray::DBClientID>&)>;
  Callback f = [](ray::Status status, const vector<DBClientID>& v){};
  UniqueID id = this->od->GetLocations(object_id, f);
  return id;
};

UniqueID ObjectManager::Pull(const ObjectID &object_id,
                             DBClientID &dbclient_id,
                             const TransferCallback &callback) {
  UniqueID id = UniqueID().from_random();
  return id;
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
