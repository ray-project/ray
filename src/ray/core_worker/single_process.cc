#include "ray/core_worker/single_process.h"

namespace ray {

SingleProcess::SingleProcess() {
  task_pool_ = std::make_shared<CoreWorkerMockTaskPool>();
  store_provider_ = std::make_shared<CoreWorkerMockStoreProvider>();
  store_provider_->SetMockTaskPool(task_pool_);
  task_pool_->SetMockStoreProvider(store_provider_);
}

SingleProcess &SingleProcess::Instance() {
  static SingleProcess instance;
  return instance;
}

std::shared_ptr<CoreWorkerMockStoreProvider> SingleProcess::StoreProvider() {
  return store_provider_;
}

std::shared_ptr<CoreWorkerMockTaskPool> SingleProcess::TaskPool() { return task_pool_; }

}  // namespace ray