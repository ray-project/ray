
#include <ray/api.h>
#include <ray/util/logging.h>
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"

#include "../ray_config_internal.h"

using namespace ::ray::api;

int default_worker_main(int argc, char **argv) {
  RAY_LOG(INFO) << "CPP default worker started";
  RAY_CHECK(argc == 7);

  auto config = ray::api::RayConfigInternal::GetInstance();
  config->run_mode = RunMode::CLUSTER;
  config->worker_type = ray::WorkerType::WORKER;
  config->store_socket = std::string(argv[1]);
  config->raylet_socket = std::string(argv[2]);
  config->node_manager_port = std::stoi(std::string(argv[3]));
  std::string redis_address = std::string(std::string(argv[4]));
  config->SetRedisAddress(redis_address);
  config->redis_password = std::string(std::string(argv[5]));
  config->session_dir = std::string(std::string(argv[6]));

  Ray::Init();

  ::ray::CoreWorkerProcess::RunTaskExecutionLoop();
  return 0;
}

int main(int argc, char **argv) {
  default_worker_main(argc, argv);
  return 0;
}
