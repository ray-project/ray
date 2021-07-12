#include <boost/algorithm/string.hpp>

#include "hiredis/hiredis.h"
#include "process_helper.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"
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

/// IP address by which the local node can be reached *from* the `address`.
///
/// The behavior should be the same as `node_ip_address_from_perspective` from Ray Python
/// code. See
/// https://stackoverflow.com/questions/2674314/get-local-ip-address-using-boost-asio.
///
/// TODO(kfstorm): Make this function shared code and migrate Python & Java to use this
/// function.
///
/// \param address The IP address and port of any known live service on the network
/// you care about.
/// \return The IP address by which the local node can be reached from the address.
static std::string GetNodeIpAddress(const std::string &address = "8.8.8.8:53") {
  std::vector<std::string> parts;
  boost::split(parts, address, boost::is_any_of(":"));
  RAY_CHECK(parts.size() == 2);
  try {
    boost::asio::io_service netService;
    boost::asio::ip::udp::resolver resolver(netService);
    boost::asio::ip::udp::resolver::query query(boost::asio::ip::udp::v4(), parts[0],
                                                parts[1]);
    boost::asio::ip::udp::resolver::iterator endpoints = resolver.resolve(query);
    boost::asio::ip::udp::endpoint ep = *endpoints;
    boost::asio::ip::udp::socket socket(netService);
    socket.connect(ep);
    boost::asio::ip::address addr = socket.local_endpoint().address();
    return addr.to_string();
  } catch (std::exception &e) {
    RAY_LOG(FATAL) << "Could not get the node IP address with socket. Exception: "
                   << e.what();
    return "";
  }
}

void ProcessHelper::StartRayNode(int redis_port, std::string redis_password) {
  std::vector<std::string> cmdargs({"ray", "start", "--head", "--port",
                                    std::to_string(redis_port), "--redis-password",
                                    redis_password, "--include-dashboard", "false"});
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
  if (ConfigInternal::Instance().worker_type == ray::WorkerType::DRIVER &&
      redis_ip.empty()) {
    redis_ip = "127.0.0.1";
    StartRayNode(ConfigInternal::Instance().redis_port,
                 ConfigInternal::Instance().redis_password);
  }
  if (redis_ip == "127.0.0.1") {
    redis_ip = GetNodeIpAddress();
  }

  std::string redis_address =
      redis_ip + ":" + std::to_string(ConfigInternal::Instance().redis_port);
  std::string node_ip = ConfigInternal::Instance().node_ip_address;
  if (node_ip.empty()) {
    if (!ConfigInternal::Instance().redis_ip.empty()) {
      node_ip = GetNodeIpAddress(redis_address);
    } else {
      node_ip = GetNodeIpAddress();
    }
  }

  if (ConfigInternal::Instance().worker_type == ray::WorkerType::DRIVER) {
    ray::gcs::GlobalStateAccessor global_state_accessor(
        redis_address, ConfigInternal::Instance().redis_password);
    RAY_CHECK(global_state_accessor.Connect()) << "Failed to connect to GCS.";
    std::string node_to_connect;
    auto status =
        global_state_accessor.GetNodeToConnectForDriver(node_ip, &node_to_connect);
    RAY_CHECK_OK(status);
    ray::rpc::GcsNodeInfo node_info;
    node_info.ParseFromString(node_to_connect);
    ConfigInternal::Instance().raylet_socket_name = node_info.raylet_socket_name();
    ConfigInternal::Instance().plasma_store_socket_name =
        node_info.object_store_socket_name();
    ConfigInternal::Instance().node_manager_port = node_info.node_manager_port();
  }
  RAY_CHECK(!ConfigInternal::Instance().raylet_socket_name.empty());
  RAY_CHECK(!ConfigInternal::Instance().plasma_store_socket_name.empty());
  RAY_CHECK(ConfigInternal::Instance().node_manager_port > 0);

  auto session_dir = ConfigInternal::Instance().session_dir.empty()
                         ? GetSessionDir(redis_ip, ConfigInternal::Instance().redis_port,
                                         ConfigInternal::Instance().redis_password)
                         : ConfigInternal::Instance().session_dir;

  auto log_dir = ConfigInternal::Instance().logs_dir.empty()
                     ? session_dir + "/logs"
                     : ConfigInternal::Instance().logs_dir;

  gcs::GcsClientOptions gcs_options =
      gcs::GcsClientOptions(redis_ip, ConfigInternal::Instance().redis_port,
                            ConfigInternal::Instance().redis_password);

  CoreWorkerOptions options;
  options.worker_type = ConfigInternal::Instance().worker_type;
  options.language = Language::CPP;
  options.store_socket = ConfigInternal::Instance().plasma_store_socket_name;
  options.raylet_socket = ConfigInternal::Instance().raylet_socket_name;
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
