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
  ActorHandle(const ActorID &actor_id, const ActorHandleID &actor_handle_id,
              const ray::rpc::Language actor_language,
              const std::vector<std::string> &actor_creation_task_function_descriptor) {
    inner_.set_actor_id(actor_id.Data(), actor_id.Size());
    inner_.set_actor_handle_id(actor_handle_id.Data(), actor_handle_id.Size());
    inner_.set_actor_language((int)actor_language);
    *inner_.mutable_actor_creation_task_function_descriptor() = {
        actor_creation_task_function_descriptor.begin(),
        actor_creation_task_function_descriptor.end()};
    inner_.set_actor_cursor(actor_id.Data(), actor_id.Size());
  }

  /// ID of the actor.
  const ray::ActorID ActorID() const { return ActorID::FromBinary(inner_.actor_id()); };

  /// ID of this actor handle.
  const ray::ActorHandleID ActorHandleID() const {
    return ActorHandleID::FromBinary(inner_.actor_handle_id());
  };

  /// Language of the actor.
  const ray::rpc::Language ActorLanguage() const {
    return (ray::rpc::Language)inner_.actor_language();
  };
  // Function descriptor of actor creation task.
  const std::vector<std::string> ActorCreationTaskFunctionDescriptor() const {
    return {inner_.actor_creation_task_function_descriptor().begin(),
            inner_.actor_creation_task_function_descriptor().end()};
  };

  /// The unique id of the last return of the last task.
  /// It's used as a dependency for the next task.
  const ObjectID ActorCursor() const {
    return ObjectID::FromBinary(inner_.actor_cursor());
  };

  /// The number of tasks that have been invoked on this actor.
  const int64_t TaskCounter() const { return inner_.task_counter(); };

  /// The number of times that this actor handle has been forked.
  /// It's used to make sure ids of actor handles are unique.
  const int64_t NumForks() const { return inner_.num_forks(); };

  std::unique_ptr<ActorHandle> Fork() {
    auto new_handle = std::unique_ptr<ActorHandle>(new ActorHandle());
    std::unique_lock<std::mutex> guard(mutex_);

    new_handle->inner_.set_actor_id(inner_.actor_id());

    inner_.set_num_forks(inner_.num_forks() + 1);
    auto next_actor_handle_id = ComputeNextActorHandleId(
        ActorHandleID::FromBinary(inner_.actor_handle_id()), inner_.num_forks());
    new_handle->inner_.set_actor_handle_id(next_actor_handle_id.Data(),
                                           next_actor_handle_id.Size());

    new_handle->inner_.set_actor_language(inner_.actor_language());

    auto &original = inner_.actor_creation_task_function_descriptor();
    *new_handle->inner_.mutable_actor_creation_task_function_descriptor() = {
        original.begin(), original.end()};

    new_handle->inner_.set_actor_cursor(inner_.actor_cursor());

    new_actor_handles_.push_back(next_actor_handle_id);
    return new_handle;
  }

  void Serialize(std::string *output) {
    std::unique_lock<std::mutex> guard(mutex_);
    inner_.SerializeToString(output);
  }

  static std::unique_ptr<ActorHandle> Deserialize(const std::string &data) {
    auto ret = std::unique_ptr<ActorHandle>(new ActorHandle());
    ret->inner_.ParseFromString(data);
    return ret;
  }

 private:
  ActorHandle() {}

  /// Set actor cursor.
  void SetActorCursor(const ObjectID &actor_cursor) {
    inner_.set_actor_cursor(actor_cursor.Binary());
  };

  /// Increase task counter.
  int64_t IncreaseTaskCounter() {
    int64_t old = inner_.task_counter();
    inner_.set_task_counter(old + 1);
    return old;
  }

  std::vector<ray::ActorHandleID> GetNewActorHandles() { return new_actor_handles_; }

  void ClearNewActorHandles() { new_actor_handles_.clear(); }

 private:
  /// Protobuf defined ActorHandle.
  ray::rpc::ActorHandle inner_;
  /// The new actor handles that were created from this handle
  /// since the last task on this handle was submitted. This is
  /// used to garbage-collect dummy objects that are no longer
  /// necessary in the backend.
  std::vector<ray::ActorHandleID> new_actor_handles_;

  /// Mutex to protect mutable fields.
  std::mutex mutex_;

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
