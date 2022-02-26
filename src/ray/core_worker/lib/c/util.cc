#include "util.h"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>

#include "ray/util/process.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"
#include "ray/util/logging.h"

std::string GetNodeIpAddress(const std::string &address) {
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

void StartRayNode(const int bootstrap_port, const std::string redis_password,
                                 const std::vector<std::string> &head_args) {
  std::vector<std::string> cmdargs({"ray", "start", "--head", "--port",
                                    std::to_string(bootstrap_port), "--redis-password",
                                    redis_password, "--node-ip-address",
                                    GetNodeIpAddress(), "--include-dashboard", "true"});
  if (!head_args.empty()) {
    cmdargs.insert(cmdargs.end(), head_args.begin(), head_args.end());
  }
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  RAY_CHECK(!ray::Process::Spawn(cmdargs, true).second);
  // TODO (jon-chuang): poll raylet started every 500ms and break instead.
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return;
}

void StopRayNode() {
  std::vector<std::string> cmdargs({"ray", "stop"});
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  RAY_CHECK(!ray::Process::Spawn(cmdargs, true).second);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  return;
}

std::unique_ptr<ray::gcs::GlobalStateAccessor> CreateGlobalStateAccessor(
    const std::string &bootstrap_address, const std::string &redis_password) {
  std::vector<std::string> address;
  boost::split(address, bootstrap_address, boost::is_any_of(":"));
  RAY_CHECK(address.size() == 2);
  ray::gcs::GcsClientOptions client_options(address[0], std::stoi(address[1]),
                                            redis_password);

  auto global_state_accessor =
      std::make_unique<ray::gcs::GlobalStateAccessor>(client_options);
  RAY_CHECK(global_state_accessor->Connect()) << "Failed to connect to GCS.";
  return global_state_accessor;
}

std::unique_ptr<ray::gcs::GlobalStateAccessor> CreateGlobalStateAccessor(
    const std::string &bootstrap_address) {
  ray::gcs::GcsClientOptions client_options(bootstrap_address);

  auto global_state_accessor =
      std::make_unique<ray::gcs::GlobalStateAccessor>(client_options);
  RAY_CHECK(global_state_accessor->Connect()) << "Failed to connect to GCS.";
  return global_state_accessor;
}
