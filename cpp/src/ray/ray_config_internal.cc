
#include "gflags/gflags.h"

#include "ray_config_internal.h"

DEFINE_string(redis_address, "", "The ip address of redis server.");

DEFINE_string(redis_password, "", "The password of redis server.");

DEFINE_string(dynamic_library_path, "", "The local path of the dynamic library.");

DEFINE_string(job_id, "", "Assigned job id.");

DEFINE_int32(node_manager_port, 62665, "The node manager port in Raylet.");

DEFINE_string(raylet_name, "", "Local raylet socket name.");

DEFINE_string(object_store_name, "", "Local object store socket name.");

DEFINE_string(logs_dir, "", "Log dir for workers.");

namespace ray {
namespace api {

std::shared_ptr<RayConfigInternal> RayConfigInternal::config_ = nullptr;

std::shared_ptr<RayConfigInternal> RayConfigInternal::GetInstance() {
  if (config_ == nullptr) {
    config_ = std::make_shared<RayConfigInternal>();
  }
  return config_;
}

void RayConfigInternal::Init(RayConfig &config) {
  if (!config.address.empty()) {
    SetRedisAddress(config.address);
  }
  run_mode = config.local_mode ? RunMode::SINGLE_PROCESS : RunMode::CLUSTER;
  if (!config.dynamic_library_path.empty()) {
    lib_name = config.dynamic_library_path;
  }
  redis_password = config.redis_password_;
  if (config.argc_ != nullptr && config.argv_ != nullptr) {
    // Parse config from command line.
    gflags::ParseCommandLineFlags(config.argc_, config.argv_, true);

    if (!FLAGS_dynamic_library_path.empty()) {
      lib_name = FLAGS_dynamic_library_path;
    }
    if (!FLAGS_redis_address.empty()) {
      SetRedisAddress(FLAGS_redis_address);
    }
    google::CommandLineFlagInfo info;
    // Don't rewrite `redis_password` when it is not set in the command line.
    if (GetCommandLineFlagInfo("redis_password", &info) && !info.is_default) {
      redis_password = FLAGS_redis_password;
    }
    if (!FLAGS_job_id.empty()) {
      job_id = FLAGS_job_id;
    }
    node_manager_port = FLAGS_node_manager_port;
    if (!FLAGS_raylet_name.empty()) {
      raylet_socket = FLAGS_raylet_name;
    }
    if (!FLAGS_object_store_name.empty()) {
      store_socket = FLAGS_object_store_name;
    }
    if (!FLAGS_logs_dir.empty()) {
      logs_dir = FLAGS_logs_dir;
    }
    gflags::ShutDownCommandLineFlags();
  }
  RAY_CHECK(run_mode == RunMode::SINGLE_PROCESS || !lib_name.empty())
      << "Please add a local dynamic library by '--dynamic-library-path'";
};

void RayConfigInternal::SetRedisAddress(const std::string address) {
  auto pos = address.find(':');
  RAY_CHECK(pos != std::string::npos);
  redis_ip = address.substr(0, pos);
  redis_port = std::stoi(address.substr(pos + 1, address.length()));
}
}  // namespace api
}  // namespace ray