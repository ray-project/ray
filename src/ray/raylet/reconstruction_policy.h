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

/// \class ReconstructionPolicy
///
/// This class handles reconstruction attempts for any missing objects due to
/// eviction or node failure. For each of the registered objects, the
/// reconstruction policy will set a timer. If no notifications about that
/// object are received within the configured timeout, or if we are notified
/// that the object is evicted, then the policy will trigger reconstruction.
class ReconstructionPolicy {
 public:
  using ReconstructionCallback = std::function<void(const TaskID &)>;

  /// Create the reconstruction policy.
  ///
  /// \param io_service The event loop to which the reconstruction policy's
  ///        timers should attach.
  /// \param client_id This client's ID, to be written to the task
  ///        reconstruction log.
  /// \param task_reconstruction_log A storage system for the log of task
  ///        reconstructions. This log will be written to whenever a task
  ///        reconstruction is necessary.
  /// \param reconstruction_handler The handler to call if a task needs to be
  ///        re-executed.
  /// \param reconstruction_timeout_ms The base timeout to wait for before
  ///        triggering reconstruction.
  ReconstructionPolicy(
      boost::asio::io_service &io_service, ClientID client_id,
      gcs::LogInterface<TaskID, TaskReconstructionData> &task_reconstruction_log,
      const ReconstructionCallback &reconstruction_handler,
      uint64_t reconstruction_timeout_ms)
      : reconstruction_timeout_ms_(reconstruction_timeout_ms),
        reconstruction_timer_(io_service),
        client_id_(client_id),
        task_reconstruction_log_(task_reconstruction_log),
        reconstruction_handler_(reconstruction_handler) {
    // Start the reconstruction timer.
    Tick();
  }

  /// Fire the reconstruction timer once then again at every period. This
  /// cancels any previous calls to Tick.
  void Tick();

  /// Listen for information about an object. If no notifications arrive within
  /// the timeout, or if a notification about object eviction or failure is
  /// received, then reconstruction will be triggered for that object.
  ///
  /// \param object_id The object to listen for and reconstruct.
  void Listen(const ObjectID &object_id);

  /// Handle a notification for an object's new locations.
  ///
  /// The list of new locations may be empty, which indicates a heartbeat from
  /// a node that is creating the object. If the notification is for a new
  /// location or a heartbeat, then this resets the timer for that object.
  /// Else, the notification is for eviction or failure, so reconstruction is
  /// triggered.
  ///
  /// \param object_id The object that the notification is about.
  /// \param new_location_entries The new locations that have been broadcasted
  ///        for this object. Each location entry represents either an addition
  ///        or deletion.
  void HandleNotification(const ObjectID &object_id,
                          const std::vector<ObjectTableDataT> new_location_entries);

  /// Stop listening for information about this object. Reconstruction will not
  /// be triggered for this object unless `Listen` is called on it again.
  ///
  /// \param object_id The object to cancel reconstruction for.
  void Cancel(const ObjectID &object_id);

 private:
  /// Information for an object that we are listening for.
  struct ObjectEntry {
    /// The object's ID.
    ObjectID object_id;
    /// The number of times we believe this object has been reconstructed
    /// before. This is incremented every time we attempt to reconstruct this
    /// object by adding an entry to the task reconstruction log.
    int num_reconstructions;
    /// All known locations of the object, computed by applying the entries in
    /// a cached copy of the object table log. This may be stale.
    std::unordered_set<ClientID, UniqueIDHasher> locations;
    /// The number of reconstruction timer ticks that must pass before
    /// reconstruction for this object will be attempted.
    int num_ticks;
  };

  /// Handle the callback for a possibly failed append operation to the task
  /// reconstruction log.
  void HandleTaskLogAppend(const TaskID &task_id,
                           std::shared_ptr<TaskReconstructionDataT> data, bool appended);
  /// Attempt to reconstruct an object by appending an entry for the task that
  /// created it to the reconstruction log.
  void Reconstruct(const ObjectID &object_id);

  /// The reconstruction timer.
  uint64_t reconstruction_timeout_ms_;
  /// How often the reconstruction timer should fire.
  boost::asio::deadline_timer reconstruction_timer_;
  /// The client ID for this node. This will be added to the task
  /// reconstruction log when a task needs to be re-executed.
  ClientID client_id_;
  /// The storage system for the task reconstruction log.
  gcs::LogInterface<TaskID, TaskReconstructionData> &task_reconstruction_log_;
  /// The handler to call when reconstruction is required.
  const ReconstructionCallback reconstruction_handler_;
  /// The objects that we are listening for.
  std::unordered_map<ObjectID, ObjectEntry, UniqueIDHasher> listening_objects_;
  /// The objects that we are attempting to reconstruct.
  std::unordered_map<TaskID, std::vector<ObjectID>, UniqueIDHasher> reconstructing_tasks_;
  /// The objects that we have not received a notification for since the last
  /// timer reset.
  std::unordered_map<ObjectID, int, UniqueIDHasher> object_ticks_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RECONSTRUCTION_POLICY_H
