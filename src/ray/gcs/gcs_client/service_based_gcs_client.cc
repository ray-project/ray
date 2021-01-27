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

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"

extern "C" {
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {

ServiceBasedGcsClient::ServiceBasedGcsClient(const GcsClientOptions &options)
    : GcsClient(options),
      last_reconnect_timestamp_ms_(0),
      last_reconnect_address_(std::make_pair("", -1)) {}

Status ServiceBasedGcsClient::Connect(boost::asio::io_service &io_service) {
  RAY_CHECK(!is_connected_);

  if (options_.server_ip_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, gcs service address is empty.";
    return Status::Invalid("gcs service address is invalid!");
  }

  // Connect to redis.
  RedisClientOptions redis_client_options(options_.server_ip_, options_.server_port_,
                                          options_.password_, options_.is_test_client_);
  redis_client_.reset(new RedisClient(redis_client_options));
  RAY_CHECK_OK(redis_client_->Connect(io_service));

  // Init gcs pub sub instance.
  gcs_pub_sub_.reset(new GcsPubSub(redis_client_));

  // Get gcs service address.
  get_server_address_func_ = [this](std::pair<std::string, int> *address) {
    return GetGcsServerAddressFromRedis(
        redis_client_->GetPrimaryContext()->sync_context(), address);
  };
  std::pair<std::string, int> address;
  RAY_CHECK(GetGcsServerAddressFromRedis(
      redis_client_->GetPrimaryContext()->sync_context(), &address,
      RayConfig::instance().gcs_service_connect_retries()))
      << "Failed to get gcs server address when init gcs client.";

  resubscribe_func_ = [this](bool is_pubsub_server_restarted) {
    job_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    actor_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    node_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    node_resource_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    task_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    object_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    worker_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
  };

  // Connect to gcs service.
  client_call_manager_.reset(new rpc::ClientCallManager(io_service));
  gcs_rpc_client_.reset(new rpc::GcsRpcClient(
      address.first, address.second, *client_call_manager_,
      [this](rpc::GcsServiceFailureType type) { GcsServiceFailureDetected(type); }));
  job_accessor_.reset(new ServiceBasedJobInfoAccessor(this));
  actor_accessor_.reset(new ServiceBasedActorInfoAccessor(this));
  node_accessor_.reset(new ServiceBasedNodeInfoAccessor(this));
  node_resource_accessor_.reset(new ServiceBasedNodeResourceInfoAccessor(this));
  task_accessor_.reset(new ServiceBasedTaskInfoAccessor(this));
  object_accessor_.reset(new ServiceBasedObjectInfoAccessor(this));
  stats_accessor_.reset(new ServiceBasedStatsInfoAccessor(this));
  error_accessor_.reset(new ServiceBasedErrorInfoAccessor(this));
  worker_accessor_.reset(new ServiceBasedWorkerInfoAccessor(this));
  placement_group_accessor_.reset(new ServiceBasedPlacementGroupInfoAccessor(this));

  // Init gcs service address check timer.
  detect_timer_.reset(new boost::asio::deadline_timer(io_service));
  PeriodicallyCheckGcsServerAddress();

  is_connected_ = true;

  RAY_LOG(DEBUG) << "ServiceBasedGcsClient connected.";
  return Status::OK();
}

void ServiceBasedGcsClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  detect_timer_->cancel();
  gcs_pub_sub_.reset();
  redis_client_->Disconnect();
  redis_client_.reset();
  RAY_LOG(DEBUG) << "ServiceBasedGcsClient Disconnected.";
}

bool ServiceBasedGcsClient::GetGcsServerAddressFromRedis(
    redisContext *context, std::pair<std::string, int> *address, int max_attempts) {
  // Get gcs server address.
  int num_attempts = 0;
  redisReply *reply = nullptr;
  while (num_attempts < max_attempts) {
    reply = reinterpret_cast<redisReply *>(redisCommand(context, "GET GcsServerAddress"));
    if (reply && reply->type != REDIS_REPLY_NIL) {
      break;
    }

    // Sleep for a little, and try again if the entry isn't there yet.
    freeReplyObject(reply);
    num_attempts++;

    if (num_attempts < max_attempts) {
      std::this_thread::sleep_for(std::chrono::milliseconds(
          RayConfig::instance().internal_gcs_service_connect_wait_milliseconds()));
    }
  }

  if (num_attempts < max_attempts) {
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
    return true;
  }
  return false;
}

void ServiceBasedGcsClient::PeriodicallyCheckGcsServerAddress() {
  std::pair<std::string, int> address;
  if (get_server_address_func_(&address)) {
    if (address != current_gcs_server_address_) {
      // If GCS server address has changed, invoke the `GcsServiceFailureDetected`
      // callback.
      current_gcs_server_address_ = address;
      GcsServiceFailureDetected(rpc::GcsServiceFailureType::GCS_SERVER_RESTART);
    }
  }

  auto check_period = boost::posix_time::milliseconds(
      RayConfig::instance().gcs_service_address_check_interval_milliseconds());
  detect_timer_->expires_from_now(check_period);
  detect_timer_->async_wait([this](const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      // `operation_aborted` is set when `detect_timer_` is canceled or destroyed.
      return;
    }
    RAY_CHECK(!error) << "Checking gcs server address failed with error: "
                      << error.message();
    PeriodicallyCheckGcsServerAddress();
  });
}

void ServiceBasedGcsClient::GcsServiceFailureDetected(rpc::GcsServiceFailureType type) {
  switch (type) {
  case rpc::GcsServiceFailureType::RPC_DISCONNECT:
    // If the GCS server address does not change, reconnect to GCS server.
    ReconnectGcsServer();
    break;
  case rpc::GcsServiceFailureType::GCS_SERVER_RESTART:
    // If GCS sever address has changed, reconnect to GCS server and redo
    // subscription.
    ReconnectGcsServer();
    // NOTE(ffbin): Currently we don't support the case where the pub-sub server restarts,
    // because we use the same Redis server for both GCS storage and pub-sub. So the
    // following flag is always false.
    resubscribe_func_(false);
    // Resend resource usage after reconnected, needed by resource view in GCS.
    node_resource_accessor_->AsyncReReportResourceUsage();
    break;
  default:
    RAY_LOG(FATAL) << "Unsupported failure type: " << type;
    break;
  }
}

void ServiceBasedGcsClient::ReconnectGcsServer() {
  std::pair<std::string, int> address;
  int index = 0;
  for (; index < RayConfig::instance().ping_gcs_rpc_server_max_retries(); ++index) {
    if (get_server_address_func_(&address)) {
      // After GCS is restarted, the gcs client will reestablish the connection. At
      // present, every failed RPC request will trigger `ReconnectGcsServer`. In order to
      // avoid repeated connections in a short period of time, we add a protection
      // mechanism: if the address does not change (meaning gcs server doesn't restart),
      // the connection can be made at most once in
      // `minimum_gcs_reconnect_interval_milliseconds` milliseconds.
      if (last_reconnect_address_ == address &&
          (current_sys_time_ms() - last_reconnect_timestamp_ms_) <
              RayConfig::instance().minimum_gcs_reconnect_interval_milliseconds()) {
        RAY_LOG(INFO)
            << "Repeated reconnection in "
            << RayConfig::instance().minimum_gcs_reconnect_interval_milliseconds()
            << "milliseconds, return directly.";
        return;
      }

      RAY_LOG(DEBUG) << "Attemptting to reconnect to GCS server: " << address.first << ":"
                     << address.second;
      if (Ping(address.first, address.second, 100)) {
        // If `last_reconnect_address_` port is -1, it means that this is the first
        // connection and no log will be printed.
        if (last_reconnect_address_.second != -1) {
          RAY_LOG(INFO) << "Reconnected to GCS server: " << address.first << ":"
                        << address.second;
        }
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().ping_gcs_rpc_server_interval_milliseconds()));
  }

  if (index < RayConfig::instance().ping_gcs_rpc_server_max_retries()) {
    gcs_rpc_client_->Reset(address.first, address.second, *client_call_manager_);
    last_reconnect_address_ = address;
    last_reconnect_timestamp_ms_ = current_sys_time_ms();
  } else {
    RAY_LOG(FATAL) << "Couldn't reconnect to GCS server. The last attempted GCS "
                      "server address was "
                   << address.first << ":" << address.second;
  }
}

}  // namespace gcs
}  // namespace ray
