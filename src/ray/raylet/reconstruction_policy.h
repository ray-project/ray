#ifndef RAY_RAYLET_RECONSTRUCTION_POLICY_H
#define RAY_RAYLET_RECONSTRUCTION_POLICY_H

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include <boost/asio.hpp>

#include "ray/common/id.h"
#include "ray/gcs/tables.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/raylet/task.h"
#include "ray/util/util.h"

#include "ray/object_manager/object_directory.h"

namespace ray {

namespace raylet {

using rpc::TaskReconstructionData;
using rpc::TaskTableData;

class ReconstructionPolicyInterface {
 public:
  virtual void ListenAndMaybeReconstruct(const ObjectID &object_id) = 0;
  virtual void Cancel(const ObjectID &object_id) = 0;
  virtual ~ReconstructionPolicyInterface(){};
};

class ReconstructionPolicy : public ReconstructionPolicyInterface {
 public:
  /// Create the reconstruction policy.
  ///
  /// \param io_service The event loop to attach reconstruction timers to.
  /// \param reconstruction_handler The handler to call if a task needs to be
  /// re-executed.
  /// \param initial_reconstruction_timeout_ms The initial timeout within which
  /// a task lease notification must be received. Otherwise, reconstruction
  /// will be triggered.
  /// \param client_id The client ID to use when requesting notifications from
  /// the GCS.
  /// \param task_lease_pubsub The GCS pub-sub storage system to request task
  /// lease notifications from.
  ReconstructionPolicy(
      boost::asio::io_service &io_service,
      std::function<void(const TaskID &)> reconstruction_handler,
      int64_t initial_reconstruction_timeout_ms, const ClientID &client_id,
      gcs::PubsubInterface<TaskID> &task_lease_pubsub,
      const gcs::TableInterface<TaskID, TaskTableData> &task_table,
      std::shared_ptr<ObjectDirectoryInterface> object_directory,
      gcs::LogInterface<TaskID, TaskReconstructionData> &task_reconstruction_log);

  /// Listen for task lease notifications about an object that may require
  /// reconstruction. If no notifications are received within the initial
  /// timeout, then the registered task reconstruction handler will be called
  /// for the task that created the object.
  ///
  /// \param object_id The object to check for reconstruction.
  void ListenAndMaybeReconstruct(const ObjectID &object_id);

  /// Cancel listening for an object. Notifications for the object will be
  /// ignored. This does not cancel a reconstruction attempt that is already in
  /// progress.
  ///
  /// \param object_id The object to cancel.
  void Cancel(const ObjectID &object_id);

  /// Handle a notification for a task lease. This handler should be called to
  /// indicate that a task is currently being executed, so any objects that it
  /// creates should not be reconstructed.
  ///
  /// \param task_id The task ID of the task being executed.
  /// \param lease_timeout_ms After this timeout, the task's lease is
  /// guaranteed to be expired. If a second notification is not received within
  /// this timeout, then objects that the task creates may be reconstructed.
  /// \param The actor version. For non-actor tasks, this is always 0.  For
  /// actor tasks, this is the number of times the actor restarted before
  /// the task was executed.
  void HandleTaskLeaseNotification(const TaskID &task_id, int64_t lease_timeout_ms,
                                   int64_t actor_version);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record metrics.
  void RecordMetrics() const;

 private:
  struct ReconstructionTask {
    ReconstructionTask(boost::asio::io_service &io_service)
        : expires_at(INT64_MAX),
          subscribed(false),
          reconstruction_attempt(0),
          reconstruction_timer(new boost::asio::deadline_timer(io_service)),
          // We initialize the actor version to -1 to indicate that we have not
          // yet received a task lease, so the task has not yet been executed.
          lease_actor_version(-1) {}

    // The objects created by this task that we are listening for notifications for.
    std::unordered_set<ObjectID> created_objects;
    // The time at which the timer for this task expires, according to this
    // node's steady clock.
    int64_t expires_at;
    // Whether we are subscribed to lease notifications for this task.
    bool subscribed;
    // The number of times we've attempted reconstructing this task so far.
    int reconstruction_attempt;
    // The task's reconstruction timer. If this expires before a lease
    // notification is received, then the task will be reconstructed.
    std::unique_ptr<boost::asio::deadline_timer> reconstruction_timer;
    // The task information from the GCS, if we have any.
    std::unique_ptr<Task> task;
    // The actor version of the task that was executed, according to the task
    // lease table. This is -1 if the task has not yet been executed and 0 if
    // the task is not for an actor.
    int64_t lease_actor_version;
  };

  /// Set the reconstruction timer for a task. If no task lease notifications
  /// are received within the timeout, then reconstruction will be triggered.
  /// If the timer was previously set, this method will cancel it and reset the
  /// timer to the new timeout.
  void SetTaskTimeout(std::unordered_map<TaskID, ReconstructionTask>::iterator task_it,
                      int64_t timeout_ms);

  /// Attempt to re-execute a task to reconstruct the required object.
  ///
  /// \param task_id The task to attempt to re-execute.
  /// \param required_object_id The object created by the task that requires
  /// reconstruction.
  /// \param reconstruction_attempt What number attempt this is at
  /// reconstructing the task. This is used to suppress duplicate
  /// reconstructions of the same task (e.g., if a task creates two objects
  /// that both require reconstruction).
  void AttemptReconstruction(const TaskID &task_id, const ObjectID &required_object_id,
                             int reconstruction_attempt);

  /// Returns true if the given task failed. Assumes that the task's lease has
  /// expired.
  bool CheckExpiredTask(const Task &task, int64_t lease_actor_version);

  /// Handle expiration of a task lease.
  void HandleTaskLeaseExpired(const TaskID &task_id);

  /// Handle the response for an attempt at adding an entry to the task
  /// reconstruction log.
  void HandleReconstructionLogAppend(const TaskID &task_id, bool success);

  /// The event loop.
  boost::asio::io_service &io_service_;
  /// The handler to call for tasks that require reconstruction.
  const std::function<void(const TaskID &)> reconstruction_handler_;
  /// The initial timeout within which a task lease notification must be
  /// received. Otherwise, reconstruction will be triggered.
  const int64_t initial_reconstruction_timeout_ms_;
  /// The client ID to use when requesting notifications from the GCS.
  const ClientID client_id_;
  /// The GCS pub-sub storage system to request task lease notifications from.
  gcs::PubsubInterface<TaskID> &task_lease_pubsub_;
  /// The GCS task table to request task information from.
  /// TODO(swang): This is only needed because we treat actor and non-actor
  /// tasks differently and we need the actor ID for actor tasks. If we could
  /// embed the actor ID inside task IDs, we could remove this.
  const gcs::TableInterface<TaskID, TaskTableData> &task_table_;
  /// The object directory used to lookup object locations.
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;
  gcs::LogInterface<TaskID, TaskReconstructionData> &task_reconstruction_log_;
  /// The tasks that we are currently subscribed to in the GCS.
  std::unordered_map<TaskID, ReconstructionTask> listening_tasks_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RECONSTRUCTION_POLICY_H
