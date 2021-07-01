#include "process_helper.h"
#include "hiredis/hiredis.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace ray {
namespace api {

static std::string GetSessionDir(std::string redis_ip, int port, std::string password) {
  redisContext *context = redisConnect(redis_ip.c_str(), port);
  RAY_CHECK(context != nullptr && !context->err);
  if (!password.empty()) {
    auto auth_reply = (redisReply *)redisCommand(context, "AUTH %s", password.c_str());
    RAY_CHECK(auth_reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(auth_reply);
  }
  auto reply = (redisReply *)redisCommand(context, "GET session_dir");
  RAY_CHECK(reply->type != REDIS_REPLY_ERROR);
  std::string session_dir(reply->str);
  freeReplyObject(reply);
  redisFree(context);
  return session_dir;
}

void ProcessHelper::StartRayNode(int redis_port, std::string redis_password,
                                 int node_manager_port) {
  std::vector<std::string> cmdargs(
      {"ray", "start", "--head", "--port", std::to_string(redis_port), "--redis-password",
       redis_password, "--node-manager-port", std::to_string(node_manager_port),
       "--include-dashboard", "false"});
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true).second);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return;
}

void ProcessHelper::StopRayNode() {
  std::vector<std::string> cmdargs({"ray", "stop"});
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true).second);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  return;
}

void ProcessHelper::RayStart(CoreWorkerOptions::TaskExecutionCallback callback) {
  std::string redis_ip = ConfigInternal::Instance().redis_ip;
  if ((ray::WorkerType)ConfigInternal::Instance().worker_type ==
          ray::WorkerType::DRIVER &&
      redis_ip.empty()) {
    redis_ip = "127.0.0.1";
    StartRayNode(ConfigInternal::Instance().redis_port,
                 ConfigInternal::Instance().redis_password,
                 ConfigInternal::Instance().node_manager_port);
  }

  auto session_dir = ConfigInternal::Instance().session_dir.empty()
                         ? GetSessionDir(redis_ip, ConfigInternal::Instance().redis_port,
                                         ConfigInternal::Instance().redis_password)
                         : ConfigInternal::Instance().session_dir;

  auto store_socket = ConfigInternal::Instance().plasma_store_socket_name.empty()
                          ? session_dir + "/sockets/plasma_store"
                          : ConfigInternal::Instance().plasma_store_socket_name;

  auto raylet_socket = ConfigInternal::Instance().raylet_socket_name.empty()
                           ? session_dir + "/sockets/raylet"
                           : ConfigInternal::Instance().raylet_socket_name;

  auto log_dir = ConfigInternal::Instance().logs_dir.empty()
                     ? session_dir + "/logs"
                     : ConfigInternal::Instance().logs_dir;

  gcs::GcsClientOptions gcs_options =
      gcs::GcsClientOptions(redis_ip, ConfigInternal::Instance().redis_port,
                            ConfigInternal::Instance().redis_password);

  CoreWorkerOptions options;
  options.worker_type = ConfigInternal::Instance().worker_type;
  options.language = Language::CPP;
  options.store_socket = store_socket;
  options.raylet_socket = raylet_socket;
  if (options.worker_type == WorkerType::DRIVER) {
    if (!ConfigInternal::Instance().job_id.empty()) {
      options.job_id = JobID::FromHex(ConfigInternal::Instance().job_id);
    } else {
      /// TODO(Guyang Song): Get next job id from core worker by GCS client.
      /// Random a number to avoid repeated job ids.
      /// The repeated job ids will lead to task hang when driver connects to a existing
      /// cluster more than once.
      std::srand(std::time(nullptr));
      options.job_id = JobID::FromInt(std::rand());
    }
  }
  options.gcs_options = gcs_options;
  options.enable_logging = true;
  options.log_dir = log_dir;
  options.install_failure_signal_handler = true;
  std::string node_ip = ConfigInternal::Instance().node_ip_address.empty()
                            ? "127.0.0.1"
                            : ConfigInternal::Instance().node_ip_address;
  options.node_ip_address = node_ip;
  options.node_manager_port = ConfigInternal::Instance().node_manager_port;
  options.raylet_ip_address = node_ip;
  options.driver_name = "cpp_worker";
  options.ref_counting_enabled = true;
  options.num_workers = 1;
  options.metrics_agent_port = -1;
  options.task_execution_callback = callback;
  CoreWorkerProcess::Initialize(options);
}

void ProcessHelper::RayStop() {
  CoreWorkerProcess::Shutdown();
  if (ConfigInternal::Instance().redis_ip.empty()) {
    StopRayNode();
  }
}

}  // namespace api
}  // namespace ray
