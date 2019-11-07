#ifndef RAY_CORE_WORKER_ACTOR_HANDLE_H
#define RAY_CORE_WORKER_ACTOR_HANDLE_H

#include <gtest/gtest_prod.h>
#include "absl/types/optional.h"

#include "ray/common/id.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/gcs/tables.h"
#include "ray/protobuf/core_worker.pb.h"

namespace ray {

class ActorHandle {
 public:
  ActorHandle(ray::rpc::ActorHandle inner)
      : inner_(inner), actor_cursor_(ObjectID::FromBinary(inner_.actor_cursor())) {}

  // Constructs a new ActorHandle as part of the actor creation process.
  ActorHandle(const ActorID &actor_id, const JobID &job_id,
              const ObjectID &initial_cursor, const Language actor_language,
              bool is_direct_call,
              const std::vector<std::string> &actor_creation_task_function_descriptor);

  /// Constructs an ActorHandle from a serialized string.
  ActorHandle(const std::string &serialized);

  /// Whether the actor is dead. If yes, then the actor can never become alive
  /// again.
  bool IsDead() const {
    return state_.has_value() ? *state_ == gcs::ActorTableData::DEAD : false;
  }

  ClientID NodeId() const { return node_id_; }

  ActorID GetActorID() const { return ActorID::FromBinary(inner_.actor_id()); };

  /// ID of the job that created the actor (it is possible that the handle
  /// exists on a job with a different job ID).
  JobID CreationJobID() const { return JobID::FromBinary(inner_.creation_job_id()); };

  Language ActorLanguage() const { return inner_.actor_language(); };

  std::vector<std::string> ActorCreationTaskFunctionDescriptor() const {
    return VectorFromProtobuf(inner_.actor_creation_task_function_descriptor());
  };

  bool IsDirectCallActor() const { return inner_.is_direct_call(); }

  void SetActorTaskSpec(TaskSpecBuilder &builder, const TaskTransportType transport_type,
                        const ObjectID new_cursor);

  void Serialize(std::string *output);

  /// Update the actor's location.
  ///
  /// \param[in] node_id The ID of the actor's new node location.
  void UpdateLocation(const ClientID &node_id);

  /// Mark the actor as failed.
  ///
  /// TODO(swang): Remove reset_task_counter flag once we remove raylet codepath.
  ///
  /// \param[in] reset_task_counter Whether to reset the task counter. This
  /// should only be set to false for the raylet codepath.
  void MarkFailed(bool reset_task_counter);

  /// Mark the actor as dead. The actor should never become alive again.
  void MarkDead();

 private:
  // Protobuf-defined persistent state of the actor handle.
  const ray::rpc::ActorHandle inner_;

  /// The actor's state (alive, dead, reconstructing, or unknown).
  absl::optional<gcs::ActorTableData::ActorState> state_;
  /// Actor node location. Nil if we do not know whether the actor is alive or not.
  ClientID node_id_;

  /// The unique id of the dummy object returned by the previous task.
  /// TODO: This can be removed once we schedule actor tasks by task counter
  /// only.
  // TODO: Save this state in the core worker.
  ObjectID actor_cursor_;
  // Number of tasks that have been submitted on this handle.
  uint64_t task_counter_ = 0;

  /// Guards actor_cursor_ and task_counter_.
  std::mutex mutex_;

  FRIEND_TEST(ZeroNodeTest, TestActorHandle);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_HANDLE_H
