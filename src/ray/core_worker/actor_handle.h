#ifndef RAY_CORE_WORKER_ACTOR_HANDLE_H
#define RAY_CORE_WORKER_ACTOR_HANDLE_H

#include <gtest/gtest_prod.h>

#include "ray/common/id.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/protobuf/core_worker.pb.h"
#include "ray/protobuf/gcs.pb.h"

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

  /// Reset the handle state next task submitted.
  ///
  /// This should be called whenever the actor is restarted, since the new
  /// instance of the actor does not have the previous sequence number.
  /// TODO: We should also move the other actor state (status and IP) inside
  /// ActorHandle and reset them in this method.
  void Reset();

  // Mark the actor handle as dead.
  void MarkDead() { state_ = rpc::ActorTableData::DEAD; }

  // Returns whether the actor is known to be dead.
  bool IsDead() const { return state_ == rpc::ActorTableData::DEAD; }

 private:
  // Protobuf-defined persistent state of the actor handle.
  const ray::rpc::ActorHandle inner_;

  /// The actor's state (alive or dead). This defaults to ALIVE. Once marked
  /// DEAD, the actor handle can never go back to being ALIVE.
  rpc::ActorTableData::ActorState state_ = rpc::ActorTableData::ALIVE;

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
