#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/mock_store_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/transport/mock_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorker::CoreWorker(
    const ::Language language, std::shared_ptr<WorkerContext> worker_context,
    std::shared_ptr<CoreWorkerTaskInterface> task_interface,
    std::shared_ptr<CoreWorkerObjectInterface> object_interface,
    std::shared_ptr<CoreWorkerTaskExecutionInterface> task_execution_interface)
    : worker_type_(worker_context->GetWorkerType()),
      language_(language),
      worker_context_(worker_context),
      task_interface_(task_interface),
      object_interface_(object_interface),
      task_execution_interface_(task_execution_interface) {}

CoreWorker CoreWorker::CreateForClusterMode(const enum WorkerType worker_type,
                                            const ::Language language,
                                            const std::string &store_socket,
                                            std::shared_ptr<RayletClient> raylet_client,
                                            const WorkerID &worker_id,
                                            const JobID &job_id) {
  auto worker_context = std::make_shared<WorkerContext>(worker_type, worker_id, job_id);
  // TODO(zhijunfu): currently RayletClient would crash in its constructor
  // if it cannot connect to Raylet after a number of retries, this needs
  // to be changed so that the worker (java/python .etc) can retrieve and
  // handle the error instead of crashing.
  auto task_interface = std::make_shared<CoreWorkerTaskInterface>(
      worker_context, std::make_shared<CoreWorkerRayletTaskSubmitter>(raylet_client));
  auto object_interface = std::make_shared<CoreWorkerObjectInterface>(
      worker_context,
      std::make_shared<CoreWorkerPlasmaStoreProvider>(store_socket, raylet_client));
  auto task_execution_interface = std::make_shared<CoreWorkerTaskExecutionInterface>(
      worker_context, object_interface,
      std::make_shared<CoreWorkerRayletTaskReceiver>(raylet_client));
  return CoreWorker(language, worker_context, task_interface, object_interface,
                    task_execution_interface);
}

// TODO (kfstorm): should be able to reset store and task pool if runtime shutdown.
/// Protects intialization for single process mode.
static std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool;
static std::shared_ptr<CoreWorkerMockStoreProvider> mock_store_provider;
static std::atomic<bool> single_process_initialized;

CoreWorker CoreWorker::CreateForSingleProcessMode(const enum WorkerType worker_type,
                                                  const ::Language language,
                                                  const WorkerID &worker_id,
                                                  const JobID &job_id) {
  auto worker_context = std::make_shared<WorkerContext>(worker_type, worker_id, job_id);

  if (!single_process_initialized.exchange(true)) {
    mock_task_pool = std::make_shared<CoreWorkerMockTaskPool>();
    mock_store_provider = std::make_shared<CoreWorkerMockStoreProvider>();
    mock_task_pool->SetMockStoreProvider(mock_store_provider);
    mock_store_provider->SetMockTaskPool(mock_task_pool);
  }

  auto task_interface = std::make_shared<CoreWorkerTaskInterface>(
      worker_context, std::make_shared<CoreWorkerMockTaskSubmitter>(mock_task_pool));
  auto object_interface =
      std::make_shared<CoreWorkerObjectInterface>(worker_context, mock_store_provider);
  auto task_execution_interface = std::make_shared<CoreWorkerTaskExecutionInterface>(
      worker_context, object_interface,
      std::make_shared<CoreWorkerMockTaskReceiver>(worker_context, mock_task_pool));
  return CoreWorker(language, worker_context, task_interface, object_interface,
                    task_execution_interface);
}

}  // namespace ray
