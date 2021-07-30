#include "go_worker.h"

#include <iostream>

#include "ray/core_worker/core_worker.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"

using namespace std;

__attribute__((visibility("default"))) void go_worker_Initialize(
    int workerMode, char *store_socket, char *raylet_socket, char *log_dir,
    char *node_ip_address, int node_manager_port, char *raylet_ip_address,
    char *driver_name, int jobId) {
  SayHello((char *)"have_fun friends!");
  std::string serialized_job_config = "";
  ray::CoreWorkerOptions options;
  options.worker_type = static_cast<ray::WorkerType>(workerMode);
  options.language = ray::Language::GOLANG;
  options.store_socket = store_socket;
  options.raylet_socket = raylet_socket;
  options.job_id = ray::JobID::FromInt(jobId);
  //  options.gcs_options = ToGcsClientOptions(env, gcsClientOptions);
  options.enable_logging = true;
  options.log_dir = log_dir;
  // TODO (kfstorm): JVM would crash if install_failure_signal_handler was set to true
  options.install_failure_signal_handler = false;
  options.node_ip_address = node_ip_address;
  options.node_manager_port = node_manager_port;
  options.raylet_ip_address = raylet_ip_address;
  options.driver_name = driver_name;
  //  options.task_execution_callback = task_execution_callback;
  //  options.on_worker_shutdown = on_worker_shutdown;
  //  options.gc_collect = gc_collect;
  options.ref_counting_enabled = true;
  options.num_workers = 1;
  options.serialized_job_config = serialized_job_config;
  options.metrics_agent_port = -1;
  ray::CoreWorkerProcess::Initialize(options);
}

__attribute__((visibility("default"))) void *go_worker_CreateGlobalStateAccessor(
    char *redis_address, char *redis_password) {
  ray::gcs::GlobalStateAccessor *gcs_accessor =
      new ray::gcs::GlobalStateAccessor(redis_address, redis_password);
  return gcs_accessor;
}

__attribute__((visibility("default"))) uint32_t go_worker_GetNextJobID(void *p) {
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  const auto &job_id = gcs_accessor->GetNextJobID();
  return job_id.ToInt();
}