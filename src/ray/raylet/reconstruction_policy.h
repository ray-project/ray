#ifndef RAY_RAYLET_RECONSTRUCTION_POLICY_H
#define RAY_RAYLET_RECONSTRUCTION_POLICY_H

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "ray/gcs/format/gcs_generated.h"
#include "ray/gcs/tables.h"
#include "ray/id.h"

namespace ray {

namespace raylet {

// TODO(swang): Use std::function instead of boost.

class ReconstructionPolicy {
 public:
  using ReconstructionCallback = std::function<void(const TaskID &)>;
  /// Create the reconstruction policy.
  ///
  /// \param reconstruction_handler The handler to call if a task needs to be
  /// re-executed.
  // TODO(swang): This requires at minimum references to the Raylet's lineage
  // cache and GCS client.
  ReconstructionPolicy(
      boost::asio::io_service &io_service,
      gcs::LogInterface<TaskID, TaskReconstructionData> &task_reconstruction_log,
      const ReconstructionCallback &reconstruction_handler,
      uint64_t reconstruction_timeout_ms)
      : task_reconstruction_log_(task_reconstruction_log),
        reconstruction_handler_(reconstruction_handler),
        reconstruction_timeout_ms_(reconstruction_timeout_ms) {}

  /// Listen for information about this object. If no notifications arrive
  /// within the timeout, or if a notification about object eviction or failure
  /// is received, then this will trigger reconstruction.
  ///
  /// \param object_id The object to listen for and reconstruct.
  void Listen(const ObjectID &object_id);

  /// Notify that this object is pending creation on some node. If we are
  /// currently listening to this object, then this resets the timer.
  ///
  /// \param object_id The object that the notification is about.
  void Notify(const ObjectID &object_id);

  /// Stop listening for information about this object. Reconstruction will not
  /// be triggered for this object unless `Listen` is called on it again.
  ///
  /// \param object_id The object to cancel reconstruction for.
  void Cancel(const ObjectID &object_id);

 private:
  struct ObjectEntry {
    ObjectID object_id;
    int version;
    std::vector<ObjectTableDataT> location_entries;
    boost::asio::deadline_timer reconstruction_timer;
  };

  /// Handle a notification for an object's new locations. The list of new
  /// locations may be empty, which indicates a heartbeat from a node that is
  /// creating the object.
  void HandleNotification(const ObjectID &object_id,
                          const std::vector<ObjectTableDataT> new_locations);
  void Reconstruct(const ObjectID &object_id);

  gcs::LogInterface<TaskID, TaskReconstructionData> &task_reconstruction_log_;
  const ReconstructionCallback reconstruction_handler_;
  uint64_t reconstruction_timeout_ms_;
  /// The objects that we are listening for.
  std::unordered_map<ObjectID, ObjectEntry, UniqueIDHasher> listening_objects_;
  /// The objects that we have not received a notification for since the last
  /// timer reset.
  std::unordered_set<ObjectID, UniqueIDHasher> timed_out_objects_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RECONSTRUCTION_POLICY_H
