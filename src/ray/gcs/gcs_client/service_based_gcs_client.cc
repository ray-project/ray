// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include <unistd.h>
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"

namespace ray {
namespace gcs {

ServiceBasedGcsClient::ServiceBasedGcsClient(const GcsClientOptions &options)
    : GcsClient(options) {}

Status ServiceBasedGcsClient::Connect(boost::asio::io_service &io_service) {
  RAY_CHECK(!is_connected_);

  if (options_.server_ip_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, gcs service address is empty.";
    return Status::Invalid("gcs service address is invalid!");
  }

  // Connect to gcs.
  redis_gcs_client_.reset(new RedisGcsClient(options_));
  RAY_CHECK_OK(redis_gcs_client_->Connect(io_service));

  // Init gcs pub sub instance.
  gcs_pub_sub_.reset(new GcsPubSub(redis_gcs_client_->GetRedisClient()));

  // Get gcs service address.
  get_server_address_func_ = [this]() {
    std::pair<std::string, int> address;
    GetGcsServerAddressFromRedis(redis_gcs_client_->primary_context()->sync_context(),
                                 &address);
    return address;
  };
  std::pair<std::string, int> address = get_server_address_func_();

  resubscribe_func_ = [this](bool is_pubsub_server_restarted,
                             const std::pair<std::string, int> &address) {
    // Both service discovery and RPC disconnection will call resubscribe if gcs server
    // restarts. In order to avoid repeated subscriptions, we added a check of current gcs
    // server address.
    if (address != current_gcs_server_address_) {
      current_gcs_server_address_ = address;
      job_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
      actor_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
      node_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
      task_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
      object_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
      worker_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    }
  };

  // Connect to gcs service.
  client_call_manager_.reset(new rpc::ClientCallManager(io_service));
  gcs_rpc_client_.reset(
      new rpc::GcsRpcClient(address.first, address.second, *client_call_manager_,
                            get_server_address_func_, resubscribe_func_));
  job_accessor_.reset(new ServiceBasedJobInfoAccessor(this));
  actor_accessor_.reset(new ServiceBasedActorInfoAccessor(this));
  node_accessor_.reset(new ServiceBasedNodeInfoAccessor(this));
  task_accessor_.reset(new ServiceBasedTaskInfoAccessor(this));
  object_accessor_.reset(new ServiceBasedObjectInfoAccessor(this));
  stats_accessor_.reset(new ServiceBasedStatsInfoAccessor(this));
  error_accessor_.reset(new ServiceBasedErrorInfoAccessor(this));
  worker_accessor_.reset(new ServiceBasedWorkerInfoAccessor(this));

  // Init gcs service address check timer.
  detect_timer_.reset(new boost::asio::deadline_timer(io_service));
  Tick();

  is_connected_ = true;

  RAY_LOG(INFO) << "ServiceBasedGcsClient Connected.";
  return Status::OK();
}

void ServiceBasedGcsClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  is_ready_to_exit_ = true;
  boost::system::error_code ec;
  detect_timer_->cancel(ec);
  if (ec) {
    RAY_LOG(WARNING) << "An exception occurs when Shutdown, error " << ec.message();
  }
  gcs_pub_sub_.reset();
  redis_gcs_client_->Disconnect();
  redis_gcs_client_.reset();
  RAY_LOG(DEBUG) << "ServiceBasedGcsClient Disconnected.";
}

void ServiceBasedGcsClient::GetGcsServerAddressFromRedis(
    redisContext *context, std::pair<std::string, int> *address) {
  // Get gcs server address.
  int num_attempts = 0;
  redisReply *reply = nullptr;
  while (num_attempts < RayConfig::instance().gcs_service_connect_retries()) {
    reply = reinterpret_cast<redisReply *>(redisCommand(context, "GET GcsServerAddress"));
    if (reply && reply->type != REDIS_REPLY_NIL) {
      break;
    }

    // Sleep for a little, and try again if the entry isn't there yet.
    freeReplyObject(reply);
    usleep(RayConfig::instance().internal_gcs_service_connect_wait_milliseconds() * 1000);
    num_attempts++;

    // GCS Client is ready to exit.
    if (!is_ready_to_exit_) {
      return;
    }
  }

  RAY_CHECK(num_attempts < RayConfig::instance().gcs_service_connect_retries())
      << "No entry found for GcsServerAddress";
  RAY_CHECK(reply) << "Redis did not reply to GcsServerAddress. Is redis running?";
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

void ServiceBasedGcsClient::Tick() {
  auto address = get_server_address_func_();
  resubscribe_func_(false, address);
  ScheduleTick();
}

void ServiceBasedGcsClient::ScheduleTick() {
  auto check_period = boost::posix_time::milliseconds(
      RayConfig::instance().gcs_service_address_check_interval_milliseconds());
  detect_timer_->expires_from_now(check_period);
  detect_timer_->async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      // `operation_canceled` is set when `detect_timer_` is canceled or destroyed.
      return;
    }
    RAY_CHECK(!error) << "Checking gcs server address failed with error: "
                      << error.message();
    Tick();
  });
}

}  // namespace gcs
}  // namespace ray
