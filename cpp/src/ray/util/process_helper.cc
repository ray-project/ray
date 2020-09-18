#include "process_helper.h"
#include "hiredis/hiredis.h"
#include "ray/core.h"
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

static void StartRayNode(int redis_port, std::string redis_password) {
  std::vector<std::string> cmdargs({"ray", "start", "--head", "--redis-port",
                                    std::to_string(redis_port), "--redis-password",
                                    redis_password});
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true).second);
  sleep(5);
  return;
}

static void StopRayNode() {
  std::vector<std::string> cmdargs({"ray", "stop"});
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true).second);
  usleep(1000 * 1000);
  return;
}

void ProcessHelper::RayStart(std::shared_ptr<RayConfig> config) {
  std::string redis_ip = config->redis_ip;
  if (redis_ip.empty()) {
    redis_ip = "127.0.0.1";
    StartRayNode(config->redis_port, config->redis_password);
  }

  auto session_dir = GetSessionDir(redis_ip, config->redis_port, config->redis_password);

  gcs::GcsClientOptions gcs_options =
      gcs::GcsClientOptions(redis_ip, config->redis_port, config->redis_password);

  CoreWorkerOptions options;
  options.worker_type = config->worker_type;
  options.language = Language::CPP;
  options.store_socket = session_dir + "/sockets/plasma_store";
  options.raylet_socket = session_dir + "/sockets/raylet";
  options.job_id = JobID::FromInt(1);
  options.gcs_options = gcs_options;
  options.enable_logging = true;
  options.install_failure_signal_handler = true;
  options.node_ip_address = "127.0.0.1";
  options.node_manager_port = config->node_manager_port;
  options.raylet_ip_address = "127.0.0.1";
  options.driver_name = "cpp_worker";
  options.ref_counting_enabled = true;
  options.num_workers = 1;
  options.metrics_agent_port = -1;
  CoreWorkerProcess::Initialize(options);
}

void ProcessHelper::RayStop(std::shared_ptr<RayConfig> config) {
  CoreWorkerProcess::Shutdown();
  if (config->redis_ip.empty()) {
    StopRayNode();
  }
}

}  // namespace api
}  // namespace ray
