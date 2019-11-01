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
#include "ray/rpc/client_call.h"
#include "ray/rpc/worker/direct_actor_client.h"

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

  absl::optional<gcs::ActorTableData::ActorState> ActorState() const {
    if (data_ == nullptr) {
      return absl::optional<gcs::ActorTableData::ActorState>();
    } else {
      return data_->state();
    }
  }

  std::shared_ptr<rpc::DirectActorClient> &GetRpcClient() { return rpc_client_; }

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

  /// Connect to the actor.
  ///
  /// If the actor dies, disconnect via ActorHandle::Reset.
  ///
  /// \param[in] data The actor's data in the GCS.
  /// \param[in] client_call_manager The service used to send RPCs to the actor.
  void Connect(const gcs::ActorTableData &data,
               rpc::ClientCallManager &client_call_manager);

  /// Reset the handle state after the actor has died.
  ///
  /// If the actor is restarted, connect via ActorHandle::Connect.
  ///
  /// This should be called whenever the actor is restarted, since the new
  /// instance of the actor does not have the previous sequence number.
  void Reset(const gcs::ActorTableData &data, bool reset_task_counter);

 private:
  // Protobuf-defined persistent state of the actor handle.
  const ray::rpc::ActorHandle inner_;
  /// Actor's state (e.g. alive, dead, reconstrucing) and location.
  std::unique_ptr<gcs::ActorTableData> data_;
  /// GRPC client to the actor. This must be a shared pointer so that the
  /// client is kept alive if there are pending callbacks after when the client
  /// disconnects.
  std::shared_ptr<rpc::DirectActorClient> rpc_client_;

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
