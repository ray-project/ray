#ifndef GCS_GCS_SERVER_OBJECT_LOCATOR_H
#define GCS_GCS_SERVER_OBJECT_LOCATOR_H

#include <unordered_map>
#include "absl/base/optimization.h"
#include "common/id.h"

namespace ray {

namespace gcs {

class ObjectLocations {
 public:
  void AddLocation(const NodeID &node_id);

  bool RemoveLocation(const NodeID &node_id);

 private:
  mutable absl::Mutex mutex_;

  std::unordered_set<NodeID> locations_ GUARDED_BY(mutex_);
};

class NodeLoadInfo {
 public:
  void AddObject(const &ObjectID &object_id);

  bool RemoveObject(const &ObjectID &object_id);

 private:
  mutable absl::Mutex mutex_;

  std::unordered_set<ObjectID> objects_ GUARDED_BY(mutex_);
};

class ObjectLocator {
 public:
  ObjectLocator();

  ~ObjectLocator();

  void AddLocations(const NodeID &node_id, const std::vector<ObjectID> &object_ids);

  void AddLocations(const ObjectID &object_id,
                    const std::unordered_set<NodeID> &locations);

  void GetLocations(const ObejctID &object_id, std::unordered_set<NodeID> *locations);

  Status RemoveObject(const ObjectID &object_id);

  Status RemoveLocation(const NodeID &node_id);

  Status RemoveLocation(const ObjectID &object_id, const NodeID &node_id);

 private:
  mutable absl::Mutex mutex_;

  std::unordered_map<ObjectID, std::shared_ptr<ObjectLocations>> object_to_locations_ GUARDED_BY(mutex_);

  std::unordered_map<NodeID, std::shared_ptr<NodeLoadInfo>> node_to_objects_ GUARDED_BY(mutex_);
};

}  // namespace gcs

}  // namespace ray

#endif  // GCS_GCS_SERVER_OBJECT_LOCATOR_H
