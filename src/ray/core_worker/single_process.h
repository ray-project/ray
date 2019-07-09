#ifndef RAY_CORE_WORKER_SINGLE_PROCESS_H
#define RAY_CORE_WORKER_SINGLE_PROCESS_H

#include "ray/core_worker/store_provider/mock_store_provider.h"
#include "ray/core_worker/transport/mock_transport.h"

namespace ray {

// TODO (kfstorm): should be able to reset store and task pool if runtime shutdown.
class SingleProcess final {
 public:
  static SingleProcess &Instance();
  std::shared_ptr<CoreWorkerMockStoreProvider> StoreProvider();
  std::shared_ptr<CoreWorkerMockTaskPool> TaskPool();

 private:
  SingleProcess();
  ~SingleProcess() = default;
  SingleProcess(const SingleProcess &) = delete;
  SingleProcess &operator=(const SingleProcess &) = delete;

  std::shared_ptr<CoreWorkerMockStoreProvider> store_provider_;
  std::shared_ptr<CoreWorkerMockTaskPool> task_pool_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_SINGLE_PROCESS_H
