#ifndef RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
#define RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H

#include <list>
#include <set>

#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/worker/direct_actor_client.h"
#include "ray/rpc/worker/direct_actor_server.h"

namespace ray {

/// In direct actor call task submitter and receiver, a task is directly submitted
/// to the actor that will execute it.

/// The state data for an actor.
struct ActorStateData {
  ActorStateData(gcs::ActorTableData::ActorState state, const std::string &ip, int port)
      : state_(state), location_(std::make_pair(ip, port)) {}

  /// Actor's state (e.g. alive, dead, reconstrucing).
  gcs::ActorTableData::ActorState state_;

  /// IP address and port that the actor is listening on.
  std::pair<std::string, int> location_;
};

class CoreWorkerDirectActorTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerDirectActorTaskSubmitter(
      boost::asio::io_service &io_service, gcs::RedisGcsClient &gcs_client,
      std::unique_ptr<CoreWorkerStoreProvider> store_provider);

  /// Submit a task to an actor for execution.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  Status SubmitTask(const TaskSpecification &task_spec) override;

 private:
  /// Subscribe to updates of an actor.
  Status SubscribeActorUpdates(const ActorID &actor_id);

  /// Push a task to a remote actor via the given client.
  /// Note, this function doesn't return any error status code. If an error occurs while
  /// sending the request, this task will be treated as failed.
  ///
  /// \param[in] client The RPC client to send tasks to an actor.
  /// \param[in] request The request to send.
  /// \param[in] actor_id Actor ID.
  /// \param[in] task_id The ID of a task.
  /// \param[in] num_returns Number of return objects.
  /// \return Void.
  void PushTask(rpc::DirectActorClient &client, const rpc::PushTaskRequest &request,
                const ActorID &actor_id, const TaskID &task_id, int num_returns);

  /// Treat a task as failed.
  ///
  /// \param[in] task_id The ID of a task.
  /// \param[in] num_returns Number of return objects.
  /// \param[in] error_type The type of the specific error.
  /// \return Void.
  void TreatTaskAsFailed(const TaskID &task_id, int num_returns,
                         const rpc::ErrorType &error_type);

  /// Create connection to actor and send all pending tasks.
  /// Note that this function doesn't take lock, the caller is expected to hold
  /// `mutex_` before calling this function.
  ///
  /// \param[in] actor_id Actor ID.
  /// \param[in] ip_address The ip address of the node that the actor is running on.
  /// \param[in] port The port that the actor is listening on.
  /// \return Void.
  void ConnectAndSendPendingTasks(const ActorID &actor_id, std::string ip_address,
                                  int port);

  /// Whether the specified actor is alive.
  ///
  /// \param[in] actor_id The actor ID.
  /// \return Whether this actor is alive.
  bool IsActorAlive(const ActorID &actor_id);

  /// The IO event loop.
  boost::asio::io_service &io_service_;

  /// Gcs client.
  gcs::RedisGcsClient &gcs_client_;

  /// The `ClientCallManager` object that is shared by all `DirectActorClient`s.
  rpc::ClientCallManager client_call_manager_;

  /// Mutex to proect the various maps below.
  mutable std::mutex mutex_;

  /// Map from actor id to actor state. This only includes actors that we send tasks to.
  std::unordered_map<ActorID, ActorStateData> actor_states_;

  /// Map from actor id to rpc client. This only includes actors that we send tasks to.
  std::unordered_map<ActorID, std::unique_ptr<rpc::DirectActorClient>> rpc_clients_;

  /// Map from actor id to the actor's pending requests.
  std::unordered_map<ActorID, std::list<std::unique_ptr<rpc::PushTaskRequest>>>
      pending_requests_;

  /// Map from actor id to the tasks that are waiting for reply.
  std::unordered_map<ActorID, std::unordered_map<TaskID, int>> waiting_reply_tasks_;

  /// The set of actors which are subscribed for further updates.
  std::unordered_set<ActorID> subscribed_actors_;

  /// The store provider.
  std::unique_ptr<CoreWorkerStoreProvider> store_provider_;

  friend class CoreWorkerTest;
};

class CoreWorkerDirectActorTaskReceiver : public CoreWorkerTaskReceiver,
                                          public rpc::DirectActorHandler {
 public:
  CoreWorkerDirectActorTaskReceiver(WorkerContext &worker_context,
                                    CoreWorkerObjectInterface &object_interface,
                                    boost::asio::io_service &io_service,
                                    rpc::GrpcServer &server,
                                    const TaskHandler &task_handler);

  /// Handle a `PushTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  void HandlePushTask(const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override;

 private:
  // Worker context.
  WorkerContext &worker_context_;
  // Object interface.
  CoreWorkerObjectInterface &object_interface_;
  /// The rpc service for `DirectActorService`.
  rpc::DirectActorGrpcService task_service_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
