#include "object_directory.h"

namespace ray {

  // TODO(hme): Implement using GCS for object lookup.
  ObjectDirectory::ObjectDirectory(){
    this->init_gcs();
  }

  void ObjectDirectory::init_gcs(){

  };

  UniqueID ObjectDirectory::GetLocations(const ObjectID &object_id,
                                         const Callback &callback,
                                         UniqueID *id) {
    if (id == nullptr) {
      *id = UniqueID().from_random();
    }
    vector<DBClientID> v;
    v.push_back(DBClientID().from_random());
    // TODO: Asynchronously obtain set of DBClientID that contain ObjectID.
    // callback(Status::OK(), object_id, id, v);
    return *id;
  };

  Status ObjectDirectory::Cancel(const UniqueID &callback_id) {
    // TODO: Cancel GetLocations request.
    return Status::OK();
  };

  void ObjectDirectory::Terminate(){};

} // namespace ray
