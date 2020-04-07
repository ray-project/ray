#include "ray/gcs/gcs_server/object_locator.h"

namespace ray {

namespace gcs {

ObjectLocator::ObjectLocator() {}

ObjectLocator::~ObjectLocator() {}

void ObjectLocator::AddObjectsLocation(const ClientID &node_id,
                                       const std::unordered_set<ObjectID> &object_ids) {
  // TODO(micafan) Optimize the lock when necessary.
  // Maybe use read/write lock. Or reduce the granularity of the lock.
  absl::MutexLock lock(&mutex_);

  auto *node_hold_objects = GetNodeHoldObjectSet(node_id, /* create_if_not_exist */ true);
  node_hold_objects->insert(object_ids.begin(), object_ids.end());

  for (const auto &object_id : object_ids) {
    auto *object_locations =
        GetObjectLocationSet(object_id, /* create_if_not_exist */ true);
    object_locations->emplace(node_id);
  }
}

void ObjectLocator::AddObjectLocation(const ObjectID &object_id,
                                      const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto *node_hold_objects = GetNodeHoldObjectSet(node_id, /* create_if_not_exist */ true);
  node_hold_objects->emplace(object_id);

  auto *object_locations =
      GetObjectLocationSet(object_id, /* create_if_not_exist */ true);
  object_locations->emplace(node_id);
}

std::unordered_set<ClientID> ObjectLocator::GetObjectLocations(
    const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);

  auto *object_locations = GetObjectLocationSet(object_id);
  if (object_locations) {
    return *object_locations;
  }
  return std::unordered_set<ClientID>{};
}

void ObjectLocator::RemoveNode(const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  ObjectSet node_hold_objects;
  auto it = node_to_objects_.find(node_id);
  if (it != node_to_objects_.end()) {
    node_hold_objects.swap(it->second);
    node_to_objects_.erase(it);
  }

  if (node_hold_objects.empty()) {
    return;
  }

  for (const auto &object_id : node_hold_objects) {
    auto *object_locations = GetObjectLocationSet(object_id);
    if (object_locations) {
      object_locations->erase(node_id);
      if (object_locations->empty()) {
        object_to_locations_.erase(object_id);
      }
    }
  }
}

void ObjectLocator::RemoveObjectLocation(const ObjectID &object_id,
                                         const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto *object_locations = GetObjectLocationSet(object_id);
  if (object_locations) {
    object_locations->erase(node_id);
    if (object_locations->empty()) {
      object_to_locations_.erase(object_id);
    }
  }

  auto *node_hold_objects = GetNodeHoldObjectSet(node_id);
  if (node_hold_objects) {
    node_hold_objects->erase(object_id);
    if (node_hold_objects->empty()) {
      node_to_objects_.erase(node_id);
    }
  }
}

ObjectLocator::LocationSet *ObjectLocator::GetObjectLocationSet(
    const ObjectID &object_id, bool create_if_not_exist) {
  LocationSet *object_locations = nullptr;

  auto it = object_to_locations_.find(object_id);
  if (it != object_to_locations_.end()) {
    object_locations = &it->second;
  } else if (create_if_not_exist) {
    auto ret = object_to_locations_.emplace(std::make_pair(object_id, LocationSet{}));
    RAY_CHECK(ret.second);
    object_locations = &(ret.first->second);
  }

  return object_locations;
}

ObjectLocator::ObjectSet *ObjectLocator::GetNodeHoldObjectSet(const ClientID &node_id,
                                                              bool create_if_not_exist) {
  ObjectSet *node_hold_objects = nullptr;

  auto it = node_to_objects_.find(node_id);
  if (it != node_to_objects_.end()) {
    node_hold_objects = &it->second;
  } else if (create_if_not_exist) {
    auto ret = node_to_objects_.emplace(std::make_pair(node_id, ObjectSet{}));
    RAY_CHECK(ret.second);
    node_hold_objects = &(ret.first->second);
  }
  return node_hold_objects;
}

}  // namespace gcs

}  // namespace ray
