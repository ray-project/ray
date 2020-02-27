#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include <unistd.h>
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"

namespace ray {
namespace gcs {

ServiceBasedGcsClient::ServiceBasedGcsClient(const GcsClientOptions &options)
    : GcsClient(options), is_connecting_(false), reconnect_count_(0) {}

Status ServiceBasedGcsClient::Connect(boost::asio::io_service &io_service) {
  RAY_CHECK(!is_connected_);

  if (options_.server_ip_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, gcs service address is empty.";
    return Status::Invalid("gcs service address is invalid!");
  }

  // Connect to gcs
  redis_gcs_client_.reset(new RedisGcsClient(options_));
  RAY_CHECK_OK(redis_gcs_client_->Connect(io_service));

  // Get gcs service address
  GetGcsServerAddressFromRedis(redis_gcs_client_->primary_context()->sync_context(),
                               &address_);

  // Connect to gcs service
  client_call_manager_.reset(new rpc::ClientCallManager(io_service));
  absl::MutexLock lock(&mutex_);
  gcs_rpc_client_.reset(
      new rpc::GcsRpcClient(address_.first, address_.second, *client_call_manager_));

  job_accessor_.reset(new ServiceBasedJobInfoAccessor(this));
  actor_accessor_.reset(new ServiceBasedActorInfoAccessor(this));
  node_accessor_.reset(new ServiceBasedNodeInfoAccessor(this));
  task_accessor_.reset(new ServiceBasedTaskInfoAccessor(this));
  object_accessor_.reset(new ServiceBasedObjectInfoAccessor(this));
  stats_accessor_.reset(new ServiceBasedStatsInfoAccessor(this));
  error_accessor_.reset(new ServiceBasedErrorInfoAccessor(this));
  worker_accessor_.reset(new ServiceBasedWorkerInfoAccessor(this));

  // Create event loop for reconnect gcs service
  work_thread_.reset(new std::thread([this] {
    std::unique_ptr<boost::asio::io_service::work> work(
        new boost::asio::io_service::work(io_service_));
    io_service_.run();
  }));

  is_connected_ = true;

  RAY_LOG(INFO) << "ServiceBasedGcsClient Connected.";
  return Status::OK();
}

void ServiceBasedGcsClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  io_service_.stop();
  work_thread_->join();
  work_thread_.reset();
  RAY_LOG(INFO) << "ServiceBasedGcsClient Disconnected.";
}

uint64_t ServiceBasedGcsClient::Reconnect(uint64_t reconnect_count) {
  absl::MutexLock lock(&mutex_);
  if (reconnect_count_ >= reconnect_count && !is_connecting_) {
    if (RayConfig::instance().gcs_service_rpc_auto_reconnect_enabled()) {
      is_connecting_ = true;
      io_service_.post([this] {
        // Get gcs service address
        std::pair<std::string, int> address;
        GetGcsServerAddressFromRedis(redis_gcs_client_->primary_context()->sync_context(),
                                     &address);
        absl::MutexLock lock(&mutex_);
        if (address != address_) {
          // Connect to gcs service
          gcs_rpc_client_.reset(new rpc::GcsRpcClient(address.first, address.second,
                                                      *client_call_manager_));
          is_connected_ = true;
          RAY_LOG(INFO) << "ServiceBasedGcsClient reconnect success.";
        }
        ++reconnect_count_;
        is_connecting_ = false;
      });
    }
  }

  return reconnect_count_;
}

void ServiceBasedGcsClient::GetGcsServerAddressFromRedis(
    redisContext *context, std::pair<std::string, int> *address) {
  // Get gcs server address.
  int num_attempts = 0;
  redisReply *reply = nullptr;
  while (num_attempts < RayConfig::instance().gcs_service_connect_retries()) {
    reply = reinterpret_cast<redisReply *>(redisCommand(context, "GET GcsServerAddress"));
    if (reply->type != REDIS_REPLY_NIL) {
      break;
    }

    // Sleep for a little, and try again if the entry isn't there yet.
    freeReplyObject(reply);
    usleep(RayConfig::instance().internal_gcs_service_connect_wait_milliseconds() * 1000);
    num_attempts++;
  }
  RAY_CHECK(num_attempts < RayConfig::instance().gcs_service_connect_retries())
      << "No entry found for GcsServerAddress";
  RAY_CHECK(reply->type == REDIS_REPLY_STRING)
      << "Expected string, found Redis type " << reply->type << " for GcsServerAddress";
  std::string result(reply->str);
  freeReplyObject(reply);

  RAY_CHECK(!result.empty()) << "Gcs service address is empty";
  size_t pos = result.find(':');
  RAY_CHECK(pos != std::string::npos)
      << "Gcs service address format is erroneous: " << result;
  address->first = result.substr(0, pos);
  address->second = std::stoi(result.substr(pos + 1));
}

}  // namespace gcs
}  // namespace ray
