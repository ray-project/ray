#ifndef RAY_CORE_WORKER_TRANSPORT_LAYER_H
#define RAY_CORE_WORKER_TRANSPORT_LAYER_H

#include <list>

#include "ray/core_worker/store_provider/store_provider_layer.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/worker/direct_actor_client.h"
#include "ray/rpc/worker/direct_actor_server.h"
#include "ray/util/ordered_set.h"

namespace ray {

/// This class encapsulates all the supported transports, including
class CoreWorkerTaskSubmitterLayer {
 public:
  CoreWorkerTaskSubmitterLayer(boost::asio::io_service &io_service,
                               std::unique_ptr<rpc::RayletClient> &raylet_client,
                               gcs::RedisGcsClient &gcs_client,
                               CoreWorkerStoreProviderLayer &store_provider_layer);

  /// Submit a task for execution.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  Status SubmitTask(TaskTransportType type, const TaskSpecification &task_spec);

  /// Check if a task has finished.
  ///
  /// \param[in] task_id The ID of the task.
  /// \return If the task has finished.
  bool ShouldWaitTask(TaskTransportType type, const TaskID &task_id) const;

  /// Get the store provider type for return objects.
  ///
  /// \return Store provider type used.
  StoreProviderType GetStoreProviderTypeForReturnObject(TaskTransportType type) const;

 private:
  /// All the task submitters supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskSubmitter>>
      task_submitters_;

  friend class CoreWorkerTest;
};

/// This class encapsulates all the supported transports, including
class CoreWorkerTaskReceiverLayer {
 public:
  CoreWorkerTaskReceiverLayer(std::unique_ptr<rpc::RayletClient> &raylet_client,
                              CoreWorkerStoreProviderLayer &store_provider_layer,
                              CoreWorkerTaskReceiver::TaskHandler executor_func);

  void Run();

  int GetRpcServerPort() const;

 private:
  /// All the task task receivers supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskReceiver>>
      task_receivers_;

  /// The RPC server.
  rpc::GrpcServer worker_server_;

  /// Event loop where tasks are processed.
  boost::asio::io_service main_service_;

  /// The asio work to keep main_service_ alive.
  boost::asio::io_service::work main_work_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TRANSPORT_LAYER_H
