#include "ray/gcs/gcs_server/object_locator.h"

namespace ray {

namespace gcs {

ObjectLocationInfo::ObjectLocationInfo(const ObjectID &object_id)
    : object_id_(object_id) {}

ObjectLocationInfo::~ObjectLocationInfo() {}

void ObjectLocationInfo::AddLocation(const ClientID &node_id) {
  locations_.emplace(node_id);
  RAY_LOG(DEBUG) << "Add object " << object_id_ << " location " << node_id;
}

size_t ObjectLocationInfo::RemoveLocation(const ClientID &node_id) {
  auto it = locations_.find(node_id);
  if (it != locations_.end()) {
    locations_.erase(it);
    RAY_LOG(DEBUG) << "Remove object " << object_id_ << " location " << node_id;
  }
  return locations_.size();
}

std::unordered_set<ClientID> ObjectLocationInfo::GetLocations() const {
  return locations_;
}

NodeHoldObjectInfo::NodeHoldObjectInfo(const ClientID &node_id) : node_id_(node_id) {}

NodeHoldObjectInfo::~NodeHoldObjectInfo() {}

void NodeHoldObjectInfo::AddObject(const ObjectID &object_id) {
  object_ids_.emplace(object_id);
  RAY_LOG(DEBUG) << "Finished adding object " << object_id << " to node " << node_id_;
}

void NodeHoldObjectInfo::AddObjects(const std::unordered_set<ObjectID> &object_ids) {
  object_ids_.insert(object_ids.begin(), object_ids.end());
}

std::unordered_set<ObjectID> NodeHoldObjectInfo::GetObjects() { return object_ids_; }

size_t NodeHoldObjectInfo::RemoveObject(const ObjectID &object_id) {
  auto it = object_ids_.find(object_id);
  if (it != object_ids_.end()) {
    object_ids_.erase(it);
    RAY_LOG(DEBUG) << "Remove object " << object_id << " from node " << node_id_;
  }
  return object_ids_.size();
}

ObjectLocator::ObjectLocator() {}

ObjectLocator::~ObjectLocator() {}

void ObjectLocator::AddObjectsLocation(const ClientID &node_id,
                                       const std::unordered_set<ObjectID> &object_ids) {
  // TODO(micafan) Optimize the lock when necessary.
  // Maybe use read/write lock. Or reduce the granularity of the lock.
  absl::MutexLock lock(&mutex_);

  auto node_hold_info = GetNodeHoldObjectInfo(node_id, /* create_if_not_exist */ true);
  // Add object ids to NodeLoadObejectInfo.
  node_hold_info->AddObjects(object_ids);

  for (const auto &object_id : object_ids) {
    auto object_location_info =
        GetObjectLocationInfo(object_id, /* create_if_not_exist */ true);
    // Add node id to ObjectLocationInfo.
    object_location_info->AddLocation(node_id);
  }
}

void ObjectLocator::AddObjectLocation(const ObjectID &object_id,
                                      const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto node_hold_info = GetNodeHoldObjectInfo(node_id, /* create_if_not_exist */ true);
  // Add object id to NodeLoadObejectInfo.
  node_hold_info->AddObject(object_id);

  auto object_location_info =
      GetObjectLocationInfo(object_id, /* create_if_not_exist */ true);
  // Add node id to ObjectLocationInfo.
  object_location_info->AddLocation(node_id);
}

std::unordered_set<ClientID> ObjectLocator::GetObjectLocations(
    const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);

  auto object_location_info = DeleteObjectLocationInfo(object_id);
  if (object_location_info) {
    return object_location_info->GetLocations();
  }
  return std::unordered_set<ClientID>{};
}

void ObjectLocator::RemoveNode(const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto node_hold_info = DeleteNodeHoldObjectInfo(node_id);
  if (node_hold_info == nullptr) {
    return;
  }

  // Remove node_id from ObjectLocationInfo.
  std::unordered_set<ObjectID> object_ids = node_hold_info->GetObjects();
  for (const auto &object_id : object_ids) {
    auto object_location_info = GetObjectLocationInfo(object_id);
    if (object_location_info != nullptr) {
      size_t cur_size = object_location_info->RemoveLocation(node_id);
      if (cur_size == 0) {
        DeleteObjectLocationInfo(object_id);
      }
    }
  }
}

void ObjectLocator::RemoveObjectLocation(const ObjectID &object_id,
                                         const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto object_location_info = GetObjectLocationInfo(object_id);
  if (object_location_info) {
    size_t cur_size = object_location_info->RemoveLocation(node_id);
    if (cur_size == 0) {
      DeleteObjectLocationInfo(object_id);
    }
  }

  auto node_hold_info = GetNodeHoldObjectInfo(node_id);
  if (node_hold_info) {
    size_t cur_size = node_hold_info->RemoveObject(object_id);
    if (cur_size == 0) {
      DeleteNodeHoldObjectInfo(node_id);
    }
  }
}

std::shared_ptr<ObjectLocationInfo> ObjectLocator::GetObjectLocationInfo(
    const ObjectID &object_id, bool create_if_not_exist) {
  std::shared_ptr<ObjectLocationInfo> object_location_info;

  auto it = object_to_locations_.find(object_id);
  if (it != object_to_locations_.end()) {
    // ObjectLocationInfo for object_id already exists.
    object_location_info = it->second;
  } else if (create_if_not_exist) {
    // ObjectLocationInfo for object_id dose't exist, need to create one.
    object_location_info = std::make_shared<ObjectLocationInfo>(object_id);
    object_to_locations_[object_id] = object_location_info;
  }

  return object_location_info;
}

std::shared_ptr<ObjectLocationInfo> ObjectLocator::DeleteObjectLocationInfo(
    const ObjectID &object_id) {
  std::shared_ptr<ObjectLocationInfo> object_location_info;

  auto it = object_to_locations_.find(object_id);
  if (it != object_to_locations_.end()) {
    // ObjectLocationInfo for object_id already exists.
    object_location_info = it->second;
    object_to_locations_.erase(it);
  }
  return object_location_info;
}

std::shared_ptr<NodeHoldObjectInfo> ObjectLocator::GetNodeHoldObjectInfo(
    const ClientID &node_id, bool create_if_not_exist) {
  std::shared_ptr<NodeHoldObjectInfo> node_hold_info;

  auto it = node_to_objects_.find(node_id);
  if (it != node_to_objects_.end()) {
    // NodeLoadObejectInfo for node_id already exists.
    node_hold_info = it->second;
  } else if (create_if_not_exist) {
    // NodeLoadObejectInfo for node_id dose't exist, need to create one.
    node_hold_info = std::make_shared<NodeHoldObjectInfo>(node_id);
    node_to_objects_[node_id] = node_hold_info;
  }
  return node_hold_info;
}

std::shared_ptr<NodeHoldObjectInfo> ObjectLocator::DeleteNodeHoldObjectInfo(
    const ClientID &node_id) {
  std::shared_ptr<NodeHoldObjectInfo> node_hold_info;

  auto it = node_to_objects_.find(node_id);
  if (it != node_to_objects_.end()) {
    // NodeLoadObejectInfo for node_id already exists.
    node_hold_info = it->second;
    node_to_objects_.erase(it);
  }

  return node_hold_info;
}

}  // namespace gcs

}  // namespace ray
