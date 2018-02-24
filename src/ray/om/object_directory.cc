#include "ray/om/object_directory.h"

namespace ray {

  // TODO(hme): Implement using GCS for object lookup.
  ObjectDirectory::ObjectDirectory(){
    this->InitGCS();
  }

  void ObjectDirectory::InitGCS(){

  };

  void ObjectDirectory::GetLocations(const ObjectID &object_id,
                                     const SuccessCallback &success_cb,
                                     const FailureCallback &fail_cb) {
    if(info_cache.count(object_id) > 0){
      // TODO(hme): Disable cache once GCS is implemented.
      success_cb(info_cache[object_id], object_id);
    } else if (existing_requests.count(object_id) == 0) {
      existing_requests[object_id] = ODCallbacks({success_cb, fail_cb});;
      ExecuteGetLocations(object_id);
    } else {
      // Do nothing. A request is in progress.
    }
  };

  void ObjectDirectory::ExecuteGetLocations(const ObjectID &object_id){
//    vector<ODRemoteConnectionInfo> v;
//    ODRemoteConnectionInfo info = ODRemoteConnectionInfo();
//    info.port = "123";
//    info.ip = "127.0.0.1";
//    v.push_back(info);
    // TODO: Asynchronously obtain remote connection info about nodes that contain ObjectID.
  };

  void ObjectDirectory::GetLocationsComplete(Status status,
                                             const ObjectID &object_id,
                                             vector<ODRemoteConnectionInfo> v){
    bool success = status.ok();
    if (success) {
      info_cache[object_id] = v;
    }
    // Only invoke a callback if the request was not cancelled.
    if (existing_requests.count(object_id) > 0) {
      ODCallbacks cbs = existing_requests[object_id];
      if (success) {
        cbs.success_cb(v, object_id);
      } else {
        cbs.fail_cb(Status::IOError("Something went wrong."), object_id);
      }
    }
  }

  void ObjectDirectory::Cancel(const ObjectID &object_id) {
    existing_requests.erase(object_id);
  };

  void ObjectDirectory::Terminate(){

  };

} // namespace ray
