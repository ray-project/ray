
#include <ray/api.h>
#include <ray/util/logging.h>
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"

#include "../config_internal.h"

int main(int argc, char **argv) {
  RAY_LOG(INFO) << "CPP default worker started.";
  ray::api::ConfigInternal::Instance().worker_type = ray::WorkerType::WORKER;
  ray::api::RayConfig config;
  ray::api::Ray::Init(config, &argc, &argv);
  ::ray::CoreWorkerProcess::RunTaskExecutionLoop();
  return 0;
}
