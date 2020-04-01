#ifndef GCS_GCS_SERVER_OBJECT_LOCATOR_H
#define GCS_GCS_SERVER_OBJECT_LOCATOR_H

#include <unordered_map>
#include <unordered_set>
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class ObjectLocationInfo
/// This class is used to access the object's locations.
class ObjectLocationInfo {
 public:
  ObjectLocationInfo(const ObjectID &object_id);

  ~ObjectLocationInfo();

  /// Add location of object.
  ///
  /// \param node_id The object location that will be added.
  void AddLocation(const ClientID &node_id);

  /// Remove location of object.
  ///
  /// \param node_id The object location that will be removed.
  /// \return The number of nodes left after remove.
  size_t RemoveLocation(const ClientID &node_id);

  /// Get object's locations.
  ///
  /// \return Object locations.
  std::unordered_set<ClientID> GetLocations() const;

 private:
  ObjectID object_id_;

  std::unordered_set<ClientID> locations_;
};

/// \class NodeHoldObjectInfo
/// This class is used to access the object ids which held by node.
class NodeHoldObjectInfo {
 public:
  NodeHoldObjectInfo(const ClientID &node_id);

  ~NodeHoldObjectInfo();

  /// Add the id of the object that the node holds.
  ///
  /// \param object_id The id of the object that will be added.
  void AddObject(const ObjectID &object_id);

  /// Add the id of the object that the node holds.
  ///
  /// \param object_ids The ids of objects that will be added.
  void AddObjects(const std::unordered_set<ObjectID> &object_ids);

  /// Get all object of the node .
  std::unordered_set<ObjectID> GetObjects();

  /// Remove the id of the object that the node no longer holds.
  ///
  /// \param object_id The id of the object that will be removed.
  /// \return The number of objects after remove.
  size_t RemoveObject(const ObjectID &object_id);

 private:
  ClientID node_id_;

  std::unordered_set<ObjectID> object_ids_;
};

class ObjectLocator {
 public:
  ObjectLocator();

  ~ObjectLocator();

  /// Add location of objects.
  ///
  /// \param node_id The object location that will be added.
  /// \param object_ids The ids of objects which location will be added.
  void AddObjectsLocation(const ClientID &node_id,
                          const std::unordered_set<ObjectID> &object_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Add location of an object.
  ///
  /// \param object_id The id of object which location will be added.
  /// \param node_id The object location that will be added.
  void AddObjectLocation(const ObjectID &object_id, const ClientID &node_id)
      LOCKS_EXCLUDED(mutex_);

  /// Get object's locations.
  ///
  /// \param object_id The id of object to lookup.
  /// \return Object locations.
  std::unordered_set<ClientID> GetObjectLocations(const ObjectID &object_id)
      LOCKS_EXCLUDED(mutex_);

  /// Remove the location from objects.
  ///
  /// \param node_id The location that will be removed.
  void RemoveNode(const ClientID &node_id) LOCKS_EXCLUDED(mutex_);

  /// Remove object's location.
  ///
  /// \param object_id The id of the object which location will be removed.
  /// \param node_id The location that will be removed.
  void RemoveObjectLocation(const ObjectID &object_id, const ClientID &node_id)
      LOCKS_EXCLUDED(mutex_);

 private:
  /// Get ObjectLocationInfo by object id from map.
  /// Will create it if not exist and the flag create_if_not_exist is set to true.
  ///
  /// \param object_id The id of object to lookup.
  /// \param create_if_not_exist Whether to create a new one if not found.
  /// \return std::shared_ptr<ObjectLocationInfo>
  std::shared_ptr<ObjectLocationInfo> GetObjectLocationInfo(
      const ObjectID &object_id, bool create_if_not_exist = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Get NodeHoldObjectInfo by node id from map.
  /// Will create it if not exist and the flag create_if_not_exist is set to true.
  ///
  /// \param node_id The id of node to lookup.
  /// \param create_if_not_exist Whether to create a new one if not found.
  /// \return std::shared_ptr<NodeHoldObjectInfo>
  std::shared_ptr<NodeHoldObjectInfo> GetNodeHoldObjectInfo(
      const ClientID &node_id, bool create_if_not_exist = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  mutable absl::Mutex mutex_;

  /// Mapping from Object id to object locations.
  std::unordered_map<ObjectID, std::shared_ptr<ObjectLocationInfo>> object_to_locations_
      GUARDED_BY(mutex_);

  /// Mapping from node id to objects held by node.
  std::unordered_map<ClientID, std::shared_ptr<NodeHoldObjectInfo>> node_to_objects_
      GUARDED_BY(mutex_);
};

}  // namespace gcs

}  // namespace ray

#endif  // GCS_GCS_SERVER_OBJECT_LOCATOR_H
