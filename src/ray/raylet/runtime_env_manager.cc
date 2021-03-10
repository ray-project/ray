#include "ray/raylet/runtime_env_manager.h"

namespace ray {
namespace raylet {

void RuntimeEnvManager::DeleteURI(const std::string &uri) {
  io_worker_pool_.PopRuntimeEnvWorker(
      [uri, this](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::RuntimeEnvCleanupRequest request;
        request.set_uri(uri);
        io_worker->rpc_client()->RuntimeEnvCleanup(
            request, [io_worker, this](const ray::Status &status,
                                       const rpc::RuntimeEnvCleanupReply &) {
              io_worker_pool_.PushRuntimeEnvWorker(io_worker);
            });
      });
}

}  // namespace raylet
}  // namespace ray
