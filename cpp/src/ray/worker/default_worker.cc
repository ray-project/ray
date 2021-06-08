
#include <ray/api.h>
#include <ray/util/logging.h>
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"

#include "../config_internal.h"

using namespace ::ray::api;

int default_worker_main(int argc, char **argv) {
  RAY_LOG(INFO) << "CPP default worker started";
  RAY_CHECK(argc == 8);

  ConfigInternal::Instance().run_mode = RunMode::CLUSTER;
  ConfigInternal::Instance().worker_type = ray::WorkerType::WORKER;
  ConfigInternal::Instance().plasma_store_socket_name = std::string(argv[1]);
  ConfigInternal::Instance().raylet_socket_name = std::string(argv[2]);
  ConfigInternal::Instance().node_manager_port = std::stoi(std::string(argv[3]));
  std::string redis_address = std::string(std::string(argv[4]));
  ConfigInternal::Instance().SetRedisAddress(redis_address);
  ConfigInternal::Instance().redis_password = std::string(std::string(argv[5]));
  ConfigInternal::Instance().session_dir = std::string(std::string(argv[6]));
  ConfigInternal::Instance().logs_dir = std::string(std::string(argv[7]));

  Ray::Init();

  ::ray::CoreWorkerProcess::RunTaskExecutionLoop();
  return 0;
}

int main(int argc, char **argv) {
  default_worker_main(argc, argv);
  return 0;
}
