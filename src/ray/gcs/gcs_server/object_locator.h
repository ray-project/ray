#ifndef GCS_GCS_SERVER_OBJECT_LOCATOR_H
#define GCS_GCS_SERVER_OBJECT_LOCATOR_H

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class ObjectLocator {
 public:
  ObjectLocator();

  ~ObjectLocator();

  /// Add a location of objects.
  ///
  /// \param node_id The object location that will be added.
  /// \param object_ids The ids of objects which location will be added.
  void AddObjectsLocation(const ClientID &node_id,
                          const std::unordered_set<ObjectID> &object_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Add a location of an object.
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

  /// Remove a node.
  ///
  /// \param node_id The node that will be removed.
  void RemoveNode(const ClientID &node_id) LOCKS_EXCLUDED(mutex_);

  /// Remove object's location.
  ///
  /// \param object_id The id of the object which location will be removed.
  /// \param node_id The location that will be removed.
  void RemoveObjectLocation(const ObjectID &object_id, const ClientID &node_id)
      LOCKS_EXCLUDED(mutex_);

 private:
  typedef std::unordered_set<ClientID> LocationSet;
  typedef std::unordered_set<ObjectID> ObjectSet;

  /// Get object locations by object id from map.
  /// Will create it if not exist and the flag create_if_not_exist is set to true.
  ///
  /// \param object_id The id of object to lookup.
  /// \param create_if_not_exist Whether to create a new one if not exist.
  /// \return LocationSet *
  ObjectLocator::LocationSet *GetObjectLocationSet(const ObjectID &object_id,
                                                   bool create_if_not_exist = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Get objects by node id from map.
  /// Will create it if not exist and the flag create_if_not_exist is set to true.
  ///
  /// \param node_id The id of node to lookup.
  /// \param create_if_not_exist Whether to create a new one if not exist.
  /// \return ObjectSet *
  ObjectLocator::ObjectSet *GetNodeHoldObjectSet(const ClientID &node_id,
                                                 bool create_if_not_exist = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  mutable absl::Mutex mutex_;

  /// Mapping from object id to object locations.
  std::unordered_map<ObjectID, LocationSet> object_to_locations_ GUARDED_BY(mutex_);

  /// Mapping from node id to objects that held by the node.
  std::unordered_map<ClientID, ObjectSet> node_to_objects_ GUARDED_BY(mutex_);
};

}  // namespace gcs

}  // namespace ray

#endif  // GCS_GCS_SERVER_OBJECT_LOCATOR_H
