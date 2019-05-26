#ifndef RAY_CORE_WORKER_TASK_INTERFACE_H
#define RAY_CORE_WORKER_TASK_INTERFACE_H

#include "common.h"
#include "ray/common/buffer.h"
#include "ray/id.h"
#include "ray/status.h"

namespace ray {

class CoreWorker;

/// Options of a task call.
struct CallOptions {
  /// Number of returns of this task.
  const int num_returns = 1;
  /// Resources required by this task.
  const std::unordered_map<std::string, double> resources;
};

/// Options of an actor creation task.
struct ActorCreationOptions {
  /// Maximum number of times that the actor should be reconstructed when it dies
  /// unexpectedly. It must be non-negative. If it's 0, the actor won't be reconstructed.
  const uint64_t max_reconstructions = 0;
  /// Resources required by the whole lifetime of this actor.
  const std::unordered_map<std::string, double> resources;
};

/// A handle to an actor.
class ActorHandle {
 public:
  ActorHandle(const ActorID &actor_id, const ActorHandleID &actor_handle_id)
      : actor_id_(actor_id), actor_handle_id_(actor_handle_id){};

  /// ID of the actor.
  const ActorID &ActorID() const { return actor_id_; };

  /// ID of this actor handle.
  const ActorHandleID &ActorHandleID() const { return actor_handle_id_; };

 private:
  const class ActorID actor_id_;
  const class ActorHandleID actor_handle_id_;
};

/// The interface that contains all `CoreWorker` methods that are related to task
/// submission.
class CoreWorkerTaskInterface {
 public:
  CoreWorkerTaskInterface(CoreWorker *core_worker) { core_worker_ = core_worker; }

  /// Call a normal task.
  ///
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] call_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status.
  Status CallTask(const RayFunction &function, const std::vector<Arg> args,
                  const CallOptions &call_options, std::vector<ObjectID> *return_ids);

  /// Create an actor.
  ///
  /// \param[in] function The remote function that generates the actor object.
  /// \param[in] args Arguments of this task.
  /// \param[in] actor_creation_options Options for this actor creation task.
  /// \param[out] actor_handle Handle to the actor.
  /// \return Status.
  Status CreateActor(const RayFunction &function, const std::vector<Arg> args,
                     const ActorCreationOptions &actor_creation_options,
                     ActorHandle *actor_handle);

  /// Call an actor task.
  ///
  /// \param[in] actor_handle Handle to the actor.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] call_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status.
  Status CallActorTask(ActorHandle &actor_handle, const RayFunction &function,
                       const std::vector<Arg> args, const CallOptions &call_options,
                       std::vector<ObjectID> *return_ids);

 private:
  const CoreWorker *core_worker_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_INTERFACE_H
