#include "process_helper.h"
#include "hiredis/hiredis.h"
#include "ray/core.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace ray {
namespace api {
std::shared_ptr<ProcessHelper> ProcessHelper::ProcessHelper_ = nullptr;

static std::string GetSessionDir(std::string redis_ip, int port, std::string password) {
  redisContext *context = redisConnect(redis_ip.c_str(), port);
  RAY_CHECK(context != NULL && !context->err);
  if (!password.empty()) {
    auto auth_reply = (redisReply *)redisCommand(context, "AUTH %s", password.c_str());
    RAY_CHECK(auth_reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(auth_reply);
  }
  auto reply = (redisReply *)redisCommand(context, "GET session_dir");
  RAY_CHECK(reply->type != REDIS_REPLY_ERROR);
  auto session_dir = std::string(reply->str);
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
  sleep(3);
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

  CoreWorkerOptions options = {
      config->worker_type,                    // worker_type
      Language::CPP,                          // langauge
      session_dir + "/sockets/plasma_store",  // store_socket
      session_dir + "/sockets/raylet",        // raylet_socket
      JobID::FromInt(1),                      // job_id
      gcs_options,                            // gcs_options
      true,                                   // enable_logging
      "",                                     // log_dir
      true,                                   // install_failure_signal_handler
      "127.0.0.1",                            // node_ip_address
      config->node_manager_port,              // node_manager_port
      "127.0.0.1",                            // raylet_ip_address
      "cpp_worker",                           // driver_name
      "",                                     // stdout_file
      "",                                     // stderr_file
      nullptr,                                // task_execution_callback
      nullptr,                                // check_signals
      nullptr,                                // gc_collect
      nullptr,                                // get_lang_stack
      nullptr,                                // kill_main
      true,                                   // ref_counting_enabled
      false,                                  // is_local_mode
      1,                                      // num_workers
  };
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