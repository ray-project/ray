#include "ray/object_manager/pull_manager.h"

namespace ray {

  PullManager::PullManager(std::unordered_map<ObjectID, LocalObjectInfo> *local_objects,
                           std::unordered_map<ObjectID, PullRequest> *pull_requests
                           ) :
                           local_objects_(local_objects),
                             pull_requests_(pull_requests)
                           {

}

Status PullManager::Pull(const ObjectID &object_id,
                                const rpc::Address &owner_address) {

  RAY_LOG(DEBUG) << "Pull " << " of object " << object_id;
  // Check if object is already local.
  if (local_objects_->count(object_id) != 0) {
    RAY_LOG(ERROR) << object_id << " attempted to pull an object that's already local.";
    return ray::Status::OK();
  }
  if (pull_requests_.find(object_id) != pull_requests_.end()) {
    RAY_LOG(DEBUG) << object_id << " has inflight pull_requests, skipping.";
    return ray::Status::OK();
  }


  return Status::OK();
}


}
