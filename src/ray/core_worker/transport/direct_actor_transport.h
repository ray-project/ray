#ifndef RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
#define RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H

#include <list>

#include "ray/core_worker/transport/transport.h"
#include "ray/raylet/raylet_client.h"
#include  "ray/gcs/gcs_client.h"
#include "ray/rpc/worker/direct_actor_client.h"
#include "ray/rpc/worker/direct_actor_server.h"
#include "ray/core_worker/object_interface.h"

namespace ray {

/// In raylet task submitter and receiver, a task is submitted to raylet, and possibly
/// gets forwarded to another raylet on which node the task should be executed, and
/// then a worker on that node gets this task and starts executing it.

class CoreWorkerDirectActorTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerDirectActorTaskSubmitter(
      boost::asio::io_service &io_service,
      std::unique_ptr<gcs::GcsClient> &gcs_client,
      const std::string &store_socket);

  /// Submit a task for execution to raylet.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpec &task) override;

 private:
  /// Subscribe to actor table.
  Status SubscribeActorTable();

  Status PushTask(rpc::DirectActorClient &client, const rpc::PushTaskRequest &request);

  /// The IO event loop.
  boost::asio::io_service &io_service_;

  /// Gcs client.
  std::unique_ptr<gcs::GcsClient> &gcs_client_;

  /// The `ClientCallManager` object that is shared by all `DirectActorClient`s.
  rpc::ClientCallManager client_call_manager_;

  /// Mutex to proect `rpc_clients_` below.
  std::mutex rpc_clients_mutex_;

  /// Map from actor ids to direct actor call rpc clients.
  std::unordered_map<ActorID, std::unique_ptr<rpc::DirectActorClient>> rpc_clients_;

  /// Pending requests to send out on a per-actor basis.
  std::unordered_map<ActorID, std::list<std::unique_ptr<rpc::PushTaskRequest>>> pending_requests_;

  int counter_;

  plasma::PlasmaClient store_client_;

  Status Put(const Buffer &buffer, const ObjectID &object_id);
};

class CoreWorkerDirectActorTaskReceiver
    : public CoreWorkerTaskReceiver,
      public rpc::DirectActorHandler {
 public:
  CoreWorkerDirectActorTaskReceiver(
    boost::asio::io_service &io_service,
    rpc::GrpcServer &server);

  /// Handle a `PushTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  void HandlePushTask(const rpc::PushTaskRequest &request,
                      rpc::PushTaskReply *reply,
                      rpc::RequestDoneCallback done_callback) override;

  Status SetTaskHandler(const TaskHandler &callback) override;                                 

 private:

  TaskHandler task_handler_;

  rpc::DirectActorGrpcService task_service_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
