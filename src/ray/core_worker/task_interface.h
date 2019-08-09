#ifndef RAY_CORE_WORKER_TASK_INTERFACE_H
#define RAY_CORE_WORKER_TASK_INTERFACE_H

#include "ray/common/buffer.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/protobuf/core_worker.pb.h"

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
  ActorCreationOptions(uint64_t max_reconstructions, bool is_direct_call,
                       const std::unordered_map<std::string, double> &resources)
      : max_reconstructions(max_reconstructions),
        is_direct_call(is_direct_call),
        resources(resources) {}

  /// Maximum number of times that the actor should be reconstructed when it dies
  /// unexpectedly. It must be non-negative. If it's 0, the actor won't be reconstructed.
  const uint64_t max_reconstructions = 0;
  /// Whether to use direct actor call. If this is set to true, callers will submit
  /// tasks directly to the created actor without going through raylet.
  const bool is_direct_call = false;
  /// Resources required by the whole lifetime of this actor.
  const std::unordered_map<std::string, double> resources;
};

/// A handle to an actor.
class ActorHandle {
 public:
  ActorHandle(const ActorID &actor_id, const ActorHandleID &actor_handle_id,
              const Language actor_language, bool is_direct_call,
              const std::vector<std::string> &actor_creation_task_function_descriptor);

  ActorHandle(const ActorHandle &other);

  /// ID of the actor.
  ray::ActorID ActorID() const;

  /// ID of this actor handle.
  ray::ActorHandleID ActorHandleID() const;

  /// Language of the actor.
  Language ActorLanguage() const;

  // Function descriptor of actor creation task.
  std::vector<std::string> ActorCreationTaskFunctionDescriptor() const;

  /// The unique id of the last return of the last task.
  /// It's used as a dependency for the next task.
  ObjectID ActorCursor() const;

  /// The number of tasks that have been invoked on this actor.
  int64_t TaskCounter() const;

  /// The number of times that this actor handle has been forked.
  /// It's used to make sure ids of actor handles are unique.
  int64_t NumForks() const;

  /// Whether direct call is used. If this is true, then the tasks
  /// are submitted directly to the actor without going through raylet.
  bool IsDirectCallActor() const;

  ActorHandle Fork();

  void Serialize(std::string *output);

  static ActorHandle Deserialize(const std::string &data);

 private:
  ActorHandle();

  /// Set actor cursor.
  void SetActorCursor(const ObjectID &actor_cursor);

  /// Increase task counter.
  int64_t IncreaseTaskCounter();

  std::vector<ray::ActorHandleID> NewActorHandles() const;

  void ClearNewActorHandles();

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
  CoreWorkerTaskInterface(WorkerContext &worker_context,
                          std::unique_ptr<RayletClient> &raylet_client,
                          CoreWorkerObjectInterface &object_interface,
                          boost::asio::io_service &io_service,
                          gcs::RedisGcsClient &gcs_client);

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
  /// Build common attributes of the task spec, and compute return ids.
  ///
  /// \param[in] builder Builder to build a `TaskSpec`.
  /// \param[in] task_id The ID of this task.
  /// \param[in] task_index The task index used to build this task.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] num_returns Number of returns.
  /// \param[in] required_resources Resources required by this task.
  /// \param[in] required_placement_resources Resources required by placing this task on a
  /// node.
  /// \param[out] return_ids Return IDs.
  /// \return Void.
  void BuildCommonTaskSpec(
      TaskSpecBuilder &builder, const TaskID &task_id, const int task_index,
      const RayFunction &function, const std::vector<TaskArg> &args, uint64_t num_returns,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources,
      std::vector<ObjectID> *return_ids);

  /// Reference to the parent CoreWorker's context.
  WorkerContext &worker_context_;

  /// All the task submitters supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskSubmitter>>
      task_submitters_;

  friend class CoreWorkerTest;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_INTERFACE_H
