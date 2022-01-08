
#include "./api.h"
#include "./config_internal.h"
#include "./process_helper.h"


namespace ray {

static bool is_init_ = false;
void SetConfigToWorker() {
  ray::internal::ConfigInternal::Instance().worker_type = ray::core::WorkerType::WORKER;
}

bool ShouldInitWithCallback() {
  return !(internal::ConfigInternal::Instance().run_mode ==
               internal::RunMode::SINGLE_PROCESS ||
           core::CoreWorkerProcess::IsInitialized());
}

void Init(ray::RayConfig &config, core::CoreWorkerOptions::TaskExecutionCallback callback,
          int argc, char **argv) {
  if (!IsInitialized()) {
    internal::ConfigInternal::Instance().Init(config, argc, argv);
    if (ShouldInitWithCallback()) {
      internal::ProcessHelper::GetInstance().RayStart(callback);
    }
    is_init_ = true;
  }
}

void StartWorkerWithCallback(core::CoreWorkerOptions::TaskExecutionCallback callback,
                             int argc, char **argv) {
  RAY_LOG(INFO) << "RUST default worker started.";
  ray::internal::ConfigInternal::Instance().worker_type = core::WorkerType::WORKER;
  ray::RayConfig config;
  ray::Init(config, callback, argc, argv);
  core::CoreWorkerProcess::RunTaskExecutionLoop();
}

void Init(ray::RayConfig &config, int argc, char **argv) {
  if (!IsInitialized()) {
    internal::ConfigInternal::Instance().Init(config, argc, argv);
    // if (ShouldInitWithCallback()) {
    //   internal::ProcessHelper::GetInstance().RayStart(
    //       internal::TaskExecutor::ExecuteTask);
    // }
    is_init_ = true;
  }
}

void Init(ray::RayConfig &config) { Init(config, 0, nullptr); }

void Init() {
  RayConfig config;
  Init(config, 0, nullptr);
}

bool IsInitialized() { return is_init_; }

void Shutdown() {
  // TODO(SongGuyang): Clean the ray runtime.
  if (internal::ConfigInternal::Instance().run_mode == internal::RunMode::CLUSTER) {
    internal::ProcessHelper::GetInstance().RayStop();
  }
  is_init_ = false;
}
}
