#ifndef RAY_RAYLET_RECONSTRUCTION_POLICY_H
#define RAY_RAYLET_RECONSTRUCTION_POLICY_H

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include <boost/asio.hpp>

#include "ray/gcs/tables.h"
#include "ray/id.h"

namespace ray {

namespace raylet {

class ReconstructionPolicyInterface {
 public:
  virtual void Listen(const ObjectID &object_id, int64_t reconstruction_timeout_ms) = 0;
  virtual void Cancel(const ObjectID &object_id) = 0;
  virtual ~ReconstructionPolicyInterface(){};
};

class ReconstructionPolicy : public ReconstructionPolicyInterface {
 public:
  /// Create the reconstruction policy.
  ///
  /// \param reconstruction_handler The handler to call if a task needs to be
  /// re-executed.
  // TODO(swang): This requires at minimum references to the Raylet's lineage
  // cache and GCS client.
  ReconstructionPolicy(std::function<void(const TaskID &)> reconstruction_handler,
                       boost::asio::io_service &io_service, const ClientID &client_id,
                       gcs::PubsubInterface<TaskID> &task_lease_pubsub);

  /// Listen for task lease notifications about an object that may require
  /// reconstruction. If there is no currently active task lease and no
  /// notifications are received within the given timeout, then the registered
  /// task reconstruction handler will be called for the task that created the
  /// object.
  ///
  /// \param object_id The object to check for reconstruction.
  /// \param reconstruction_timeout_ms The amount of time to wait for a task
  /// lease notification. This parameter is ignored if there is already an
  /// active task lease.
  void Listen(const ObjectID &object_id, int64_t reconstruction_timeout_ms);

  /// Cancel listening for an object. Notifications for the object will be
  /// ignored. This does not cancel a reconstruction attempt that is already in
  /// progress.
  ///
  /// \param object_id The object to cancel.
  void Cancel(const ObjectID &object_id);

  /// Handle a notification for a task lease. This handler should be called to
  /// indicate that a task currently being executed, so any objects that it
  /// creates should not be reconstructed.
  ///
  /// \param task_id The task ID of the task being executed.
  /// \param expires_at The time at which the task's lease expires. If a second
  /// notification is not received within this timeout, then objects that the
  /// task creates may be reconstructed.
  void HandleTaskLeaseNotification(const TaskID &task_id, int64_t expires_at);

 private:
  struct ReconstructionTask {
    ReconstructionTask(boost::asio::io_service &io_service)
        : created_objects(),
          expires_at(INT64_MAX),
          subscribed(false),
          reconstruction_timer(new boost::asio::deadline_timer(io_service)) {}

    // The objects created by this task that we are listening for notifications for.
    std::unordered_set<ObjectID> created_objects;
    // The time at which the current lease for this task expires.
    int64_t expires_at;
    // Whether we are subscribed to lease notifications for this task.
    bool subscribed;
    // The task's reconstruction timer. If this expires before a lease
    // notification is received, then the task will be reconstructed.
    std::unique_ptr<boost::asio::deadline_timer> reconstruction_timer;
  };

  void Reconstruct(const TaskID &task_id);

  void HandleReconstructionTimeout(const TaskID &task_id);

  std::function<void(const TaskID &)> reconstruction_handler_;
  boost::asio::io_service &io_service_;
  const ClientID client_id_;
  gcs::PubsubInterface<TaskID> &task_lease_pubsub_;
  int64_t initial_reconstruction_timeout_ms_;
  std::unordered_map<TaskID, ReconstructionTask> listening_tasks_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RECONSTRUCTION_POLICY_H
