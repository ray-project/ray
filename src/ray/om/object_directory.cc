#include "ray/om/object_directory.h"

using namespace std;

namespace ray {

  // TODO(hme): Implement using GCS for object lookup.
  ObjectDirectory::ObjectDirectory(){
    this->InitGcs();
  }

  void ObjectDirectory::InitGcs(){

  };

  ray::Status ObjectDirectory::GetLocations(const ObjectID &object_id,
                                       const SuccessCallback &success_cb,
                                       const FailureCallback &fail_cb) {
    if(info_cache_.count(object_id) > 0){
      // TODO(hme): Disable cache once GCS is implemented.
      success_cb(info_cache_[object_id], object_id);
    } else if (existing_requests_.count(object_id) == 0) {
      existing_requests_[object_id] = ODCallbacks({success_cb, fail_cb});;
      ExecuteGetLocations(object_id);
    } else {
      // Do nothing. A request is in progress.
    }
    return ray::Status::OK();
  };

  ray::Status ObjectDirectory::ExecuteGetLocations(const ObjectID &object_id){
//    vector<ODRemoteConnectionInfo> v;
//    ODRemoteConnectionInfo info = ODRemoteConnectionInfo();
//    info.port = "123";
//    info.ip = "127.0.0.1";
//    v.push_back(info);
    // TODO: Asynchronously obtain remote connection info about nodes that contain ObjectID.
    return ray::Status::OK();
  };

  ray::Status ObjectDirectory::GetLocationsComplete(ray::Status status,
                                               const ObjectID &object_id,
                                               const std::vector<ODRemoteConnectionInfo> &v){

    bool success = status.ok();
    if (success) {
      info_cache_[object_id] = v;
    }
    // Only invoke a callback if the request was not cancelled.
    if (existing_requests_.count(object_id) > 0) {
      ODCallbacks cbs = existing_requests_[object_id];
      if (success) {
        cbs.success_cb(v, object_id);
      } else {
        cbs.fail_cb(ray::Status::IOError("Something went wrong."), object_id);
      }
    }
    return status;
  };

  ray::Status ObjectDirectory::Cancel(const ObjectID &object_id) {
    existing_requests_.erase(object_id);
    return ray::Status::OK();
  };

  ray::Status ObjectDirectory::Terminate(){
    return ray::Status::OK();
  };

} // namespace ray
