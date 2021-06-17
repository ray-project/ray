
#include <ray/api.h>
#include <ray/util/logging.h>
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"

#include "../config_internal.h"

using namespace ::ray::api;

int main(int argc, char **argv) {
  RAY_LOG(INFO) << "CPP default worker started.";
  ConfigInternal::Instance().worker_type = ray::WorkerType::WORKER;
  ::ray::api::RayConfig config;
  Ray::Init(config, &argc, &argv);
  ::ray::CoreWorkerProcess::RunTaskExecutionLoop();
  return 0;
}
