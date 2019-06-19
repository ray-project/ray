#ifndef RAY_CORE_WORKER_TASK_INTERFACE_H
#define RAY_CORE_WORKER_TASK_INTERFACE_H

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/protobuf/core_worker.pb.h"
#include "ray/raylet/task.h"

namespace ray {

class CoreWorker;

/// Options of a non-actor-creation task.
struct TaskOptions {
  TaskOptions() {}
  TaskOptions(int num_returns, const std::unordered_map<std::string, double> &resources)
      : num_returns(num_returns), resources(resources) {}

  /// Number of returns of this task.
  const int num_returns = 1;
  /// Resources required by this task.
  const std::unordered_map<std::string, double> resources;
};

/// Options of an actor creation task.
struct ActorCreationOptions {
  ActorCreationOptions() {}
  ActorCreationOptions(uint64_t max_reconstructions,
                       const std::unordered_map<std::string, double> &resources)
      : max_reconstructions(max_reconstructions), resources(resources) {}

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
      : actor_id_(actor_id),
        actor_handle_id_(actor_handle_id),
        actor_cursor_(ObjectID::FromBinary(actor_id.Binary())),
        task_counter_(0),
        num_forks_(0) {}

  ActorHandle(const ActorID &actor_id, const ActorHandleID &actor_handle_id,
              const ObjectID &actor_cursor, int task_counter, int num_forks)
      : actor_id_(actor_id),
        actor_handle_id_(actor_handle_id),
        actor_cursor_(actor_cursor),
        task_counter_(task_counter),
        num_forks_(num_forks) {}

  /// ID of the actor.
  const ray::ActorID &ActorID() const { return actor_id_; };

  /// ID of this actor handle.
  const ray::ActorHandleID &ActorHandleID() const { return actor_handle_id_; };

  /// The unique id of the last return of the last task.
  /// It's used as a dependency for the next task.
  const ObjectID &ActorCursor() const { return actor_cursor_; };

  /// The number of tasks that have been invoked on this actor.
  const int TaskCounter() const { return task_counter_; };

  /// The number of times that this actor handle has been forked.
  /// It's used to make sure ids of actor handles are unique.
  const int NumForks() const { return num_forks_; };

  std::unique_ptr<ActorHandle> Fork() {
    auto new_handle = std::unique_ptr<ActorHandle>(new ActorHandle(
        actor_id_, ComputeNextActorHandleId(actor_handle_id_, ++num_forks_)));
    new_handle->actor_cursor_ = actor_cursor_;
    new_actor_handles_.push_back(new_handle->actor_handle_id_);
    return new_handle;
  }

  void Serialize(std::string *output) const {
    ray::rpc::ActorHandle temp;
    temp.set_actor_id(actor_id_.Binary());
    temp.set_actor_handle_id(actor_handle_id_.Binary());
    temp.set_actor_cursor(actor_cursor_.Binary());
    temp.set_task_counter(task_counter_);
    temp.set_num_forks(num_forks_);
    temp.SerializeToString(output);
  }

  static std::unique_ptr<ActorHandle> Deserialize(const std::string &data) {
    ray::rpc::ActorHandle temp;
    temp.ParseFromString(data);
    return std::unique_ptr<ActorHandle>(
        new ActorHandle(ray::ActorID::FromBinary(temp.actor_id()),
                        ray::ActorHandleID::FromBinary(temp.actor_handle_id()),
                        ray::ObjectID::FromBinary(temp.actor_cursor()),
                        temp.task_counter(), temp.num_forks()));
  }

 private:
  /// Set actor cursor.
  void SetActorCursor(const ObjectID &actor_cursor) { actor_cursor_ = actor_cursor; };

  /// Increase task counter.
  int IncreaseTaskCounter() { return task_counter_++; }

  std::vector<ray::ActorHandleID> GetNewActorHandles() { return new_actor_handles_; }

  void ClearNewActorHandles() { new_actor_handles_.clear(); }

 private:
  /// ID of the actor.
  const ray::ActorID actor_id_;
  /// ID of this actor handle.
  const ray::ActorHandleID actor_handle_id_;
  /// The unique id of the last return of the last task.
  /// It's used as a dependency for the next task.
  ObjectID actor_cursor_;
  /// The number of tasks that have been invoked on this actor.
  int task_counter_;
  /// The number of times that this actor handle has been forked.
  /// It's used to make sure ids of actor handles are unique.
  int num_forks_;
  /// The new actor handles that were created from this handle
  /// since the last task on this handle was submitted. This is
  /// used to garbage-collect dummy objects that are no longer
  /// necessary in the backend.
  std::vector<ray::ActorHandleID> new_actor_handles_;

  friend class CoreWorkerTaskInterface;
};

/// The interface that contains all `CoreWorker` methods that are related to task
/// submission.
class CoreWorkerTaskInterface {
 public:
  CoreWorkerTaskInterface(CoreWorker &core_worker);

  /// Submit a normal task.
  ///
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status.
  Status SubmitTask(const RayFunction &function, const std::vector<TaskArg> &args,
                    const TaskOptions &task_options, std::vector<ObjectID> *return_ids);

  /// Create an actor.
  ///
  /// \param[in] function The remote function that generates the actor object.
  /// \param[in] args Arguments of this task.
  /// \param[in] actor_creation_options Options for this actor creation task.
  /// \param[out] actor_handle Handle to the actor.
  /// \return Status.
  Status CreateActor(const RayFunction &function, const std::vector<TaskArg> &args,
                     const ActorCreationOptions &actor_creation_options,
                     std::unique_ptr<ActorHandle> *actor_handle);

  /// Submit an actor task.
  ///
  /// \param[in] actor_handle Handle to the actor.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status.
  Status SubmitActorTask(ActorHandle &actor_handle, const RayFunction &function,
                         const std::vector<TaskArg> &args,
                         const TaskOptions &task_options,
                         std::vector<ObjectID> *return_ids);

 private:
  /// Reference to the parent CoreWorker instance.
  CoreWorker &core_worker_;

 private:
  /// Build the arguments for a task spec.
  ///
  /// \param[in] args Arguments of a task.
  /// \return Arguments as required by task spec.
  std::vector<std::shared_ptr<raylet::TaskArgument>> BuildTaskArguments(
      const std::vector<TaskArg> &args);

  /// All the task submitters supported.
  std::unordered_map<int, std::unique_ptr<CoreWorkerTaskSubmitter>> task_submitters_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_INTERFACE_H
