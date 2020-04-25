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
  auto get_server_address = [this]() {
    std::pair<std::string, int> address;
    GetGcsServerAddressFromRedis(redis_gcs_client_->primary_context()->sync_context(),
                                 &address);
    return address;
  };
  std::pair<std::string, int> address = get_server_address();

  // Connect to gcs service.
  client_call_manager_.reset(new rpc::ClientCallManager(io_service));
  gcs_rpc_client_.reset(new rpc::GcsRpcClient(address.first, address.second,
                                              *client_call_manager_, get_server_address));

  job_accessor_.reset(new ServiceBasedJobInfoAccessor(this));
  actor_accessor_.reset(new ServiceBasedActorInfoAccessor(this));
  node_accessor_.reset(new ServiceBasedNodeInfoAccessor(this));
  task_accessor_.reset(new ServiceBasedTaskInfoAccessor(this));
  object_accessor_.reset(new ServiceBasedObjectInfoAccessor(this));
  stats_accessor_.reset(new ServiceBasedStatsInfoAccessor(this));
  error_accessor_.reset(new ServiceBasedErrorInfoAccessor(this));
  worker_accessor_.reset(new ServiceBasedWorkerInfoAccessor(this));

  is_connected_ = true;

  RAY_LOG(INFO) << "ServiceBasedGcsClient Connected.";
  return Status::OK();
}

void ServiceBasedGcsClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  RAY_LOG(INFO) << "ServiceBasedGcsClient Disconnected.";
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

}  // namespace gcs
}  // namespace ray
