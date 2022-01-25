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

#include "ray/gcs/gcs_client/gcs_client.h"

#include <utility>

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/pubsub/subscriber.h"

extern "C" {
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {
namespace {

/// Adapts GcsRpcClient to SubscriberClientInterface for making RPC calls. Thread safe.
class GcsSubscriberClient final : public pubsub::SubscriberClientInterface {
 public:
  explicit GcsSubscriberClient(const std::shared_ptr<rpc::GcsRpcClient> &rpc_client)
      : rpc_client_(rpc_client) {}

  ~GcsSubscriberClient() final = default;

  void PubsubLongPolling(
      const rpc::PubsubLongPollingRequest &request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) final;

  void PubsubCommandBatch(
      const rpc::PubsubCommandBatchRequest &request,
      const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) final;

 private:
  const std::shared_ptr<rpc::GcsRpcClient> rpc_client_;
};

void GcsSubscriberClient::PubsubLongPolling(
    const rpc::PubsubLongPollingRequest &request,
    const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) {
  rpc::GcsSubscriberPollRequest req;
  req.set_subscriber_id(request.subscriber_id());
  rpc_client_->GcsSubscriberPoll(
      req,
      [callback](const Status &status, const rpc::GcsSubscriberPollReply &poll_reply) {
        rpc::PubsubLongPollingReply reply;
        *reply.mutable_pub_messages() = poll_reply.pub_messages();
        callback(status, reply);
      });
}

void GcsSubscriberClient::PubsubCommandBatch(
    const rpc::PubsubCommandBatchRequest &request,
    const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) {
  rpc::GcsSubscriberCommandBatchRequest req;
  req.set_subscriber_id(request.subscriber_id());
  *req.mutable_commands() = request.commands();
  rpc_client_->GcsSubscriberCommandBatch(
      req, [callback](const Status &status,
                      const rpc::GcsSubscriberCommandBatchReply &batch_reply) {
        rpc::PubsubCommandBatchReply reply;
        callback(status, reply);
      });
}

}  // namespace

GcsClient::GcsClient(
    const GcsClientOptions &options,
    std::function<bool(std::pair<std::string, int> *)> get_gcs_server_address_func)
    : options_(options),
      get_server_address_func_(std::move(get_gcs_server_address_func)),
      last_reconnect_timestamp_ms_(0),
      last_reconnect_address_(std::make_pair("", -1)) {}

Status GcsClient::Connect(instrumented_io_context &io_service) {
  RAY_CHECK(!is_connected_);
  if (options_.redis_ip_.empty() && options_.gcs_address_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, server ip and gcs address both are empty.";
    return Status::Invalid("gcs service address is invalid!");
  }

  if (options_.gcs_address_.empty()) {
    // Connect to redis.
    // We don't access redis shardings in GCS client, so we set `enable_sharding_conn`
    // to false.
    RedisClientOptions redis_client_options(
        options_.redis_ip_, options_.redis_port_, options_.password_,
        /*enable_sharding_conn=*/false, options_.enable_sync_conn_,
        options_.enable_async_conn_, options_.enable_subscribe_conn_);
    redis_client_ = std::make_shared<RedisClient>(redis_client_options);
    RAY_CHECK_OK(redis_client_->Connect(io_service));
  } else {
    RAY_CHECK(::RayConfig::instance().gcs_grpc_based_pubsub())
        << "If using gcs_address to start client, gRPC based pubsub has to be enabled";
  }

  // Setup gcs server address fetcher
  if (get_server_address_func_ == nullptr) {
    if (!options_.gcs_address_.empty()) {
      get_server_address_func_ = [this](std::pair<std::string, int> *addr) {
        *addr = std::make_pair(options_.gcs_address_, options_.gcs_port_);
        return true;
      };
    } else {
      get_server_address_func_ = [this](std::pair<std::string, int> *address) {
        return GetGcsServerAddressFromRedis(
            redis_client_->GetPrimaryContext()->sync_context(), address);
      };
    }
  }

  // Get gcs address
  int i = 0;
  while (current_gcs_server_address_.first.empty() &&
         i < RayConfig::instance().gcs_service_connect_retries()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().internal_gcs_service_connect_wait_milliseconds()));
    get_server_address_func_(&current_gcs_server_address_);
    i++;
  }

  resubscribe_func_ = [this](bool is_pubsub_server_restarted) {
    job_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    actor_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    node_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    node_resource_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    worker_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
  };

  // Connect to gcs service.
  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(io_service);
  gcs_rpc_client_ = std::make_shared<rpc::GcsRpcClient>(
      current_gcs_server_address_.first, current_gcs_server_address_.second,
      *client_call_manager_,
      [this](rpc::GcsServiceFailureType type) { GcsServiceFailureDetected(type); });

  rpc::Address gcs_address;
  gcs_address.set_ip_address(current_gcs_server_address_.first);
  gcs_address.set_port(current_gcs_server_address_.second);
  /// TODO(mwtian): refactor pubsub::Subscriber to avoid faking worker ID.
  gcs_address.set_worker_id(UniqueID::FromRandom().Binary());

  std::unique_ptr<pubsub::Subscriber> subscriber;
  if (RayConfig::instance().gcs_grpc_based_pubsub()) {
    subscriber = std::make_unique<pubsub::Subscriber>(
        /*subscriber_id=*/gcs_client_id_,
        /*channels=*/
        std::vector<rpc::ChannelType>{rpc::ChannelType::GCS_ACTOR_CHANNEL,
                                      rpc::ChannelType::GCS_JOB_CHANNEL,
                                      rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
                                      rpc::ChannelType::GCS_NODE_RESOURCE_CHANNEL,
                                      rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL},
        /*max_command_batch_size*/ RayConfig::instance().max_command_batch_size(),
        /*get_client=*/
        [this](const rpc::Address &) {
          return std::make_shared<GcsSubscriberClient>(gcs_rpc_client_);
        },
        /*callback_service*/ &io_service);
  }

  // Init GCS subscriber instance.
  gcs_subscriber_ =
      std::make_unique<GcsSubscriber>(redis_client_, gcs_address, std::move(subscriber));

  job_accessor_ = std::make_unique<JobInfoAccessor>(this);
  actor_accessor_ = std::make_unique<ActorInfoAccessor>(this);
  node_accessor_ = std::make_unique<NodeInfoAccessor>(this);
  node_resource_accessor_ = std::make_unique<NodeResourceInfoAccessor>(this);
  stats_accessor_ = std::make_unique<StatsInfoAccessor>(this);
  error_accessor_ = std::make_unique<ErrorInfoAccessor>(this);
  worker_accessor_ = std::make_unique<WorkerInfoAccessor>(this);
  placement_group_accessor_ = std::make_unique<PlacementGroupInfoAccessor>(this);
  internal_kv_accessor_ = std::make_unique<InternalKVAccessor>(this);
  // Init gcs service address check timer.
  periodical_runner_ = std::make_unique<PeriodicalRunner>(io_service);
  periodical_runner_->RunFnPeriodically(
      [this] { PeriodicallyCheckGcsServerAddress(); },
      RayConfig::instance().gcs_service_address_check_interval_milliseconds(),
      "GcsClient.deadline_timer.check_gcs_service_address");

  is_connected_ = true;

  RAY_LOG(DEBUG) << "GcsClient connected.";
  return Status::OK();
}

void GcsClient::Disconnect() {
  if (!is_connected_) {
    RAY_LOG(WARNING) << "GcsClient has been disconnected.";
    return;
  }
  is_connected_ = false;
  disconnected_ = true;
  RAY_LOG(DEBUG) << "GcsClient Disconnected.";
}

std::pair<std::string, int> GcsClient::GetGcsServerAddress() {
  return current_gcs_server_address_;
}

bool GcsClient::GetGcsServerAddressFromRedis(redisContext *context,
                                             std::pair<std::string, int> *address,
                                             int max_attempts) {
  // Get gcs server address.
  int num_attempts = 0;
  redisReply *reply = nullptr;
  while (num_attempts < max_attempts) {
    reply = reinterpret_cast<redisReply *>(redisCommand(context, "GET GcsServerAddress"));
    if ((reply != nullptr) && reply->type != REDIS_REPLY_NIL) {
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

void GcsClient::PeriodicallyCheckGcsServerAddress() {
  if (disconnected_) {
    return;
  }
  std::pair<std::string, int> address;
  if (get_server_address_func_(&address)) {
    if (address != current_gcs_server_address_) {
      // If GCS server address has changed, invoke the `GcsServiceFailureDetected`
      // callback.
      current_gcs_server_address_ = address;
      GcsServiceFailureDetected(rpc::GcsServiceFailureType::GCS_SERVER_RESTART);
    }
  }
}

void GcsClient::GcsServiceFailureDetected(rpc::GcsServiceFailureType type) {
  if (disconnected_) {
    return;
  }
  switch (type) {
  case rpc::GcsServiceFailureType::RPC_DISCONNECT:
    // If the GCS server address does not change, reconnect to GCS server.
    ReconnectGcsServer();
    break;
  case rpc::GcsServiceFailureType::GCS_SERVER_RESTART:
    // If GCS sever address has changed, reconnect to GCS server and redo
    // subscription.
    ReconnectGcsServer();
    // If using GCS server for pubsub, resubscribe to GCS publishers.
    resubscribe_func_(RayConfig::instance().gcs_grpc_based_pubsub());
    // Resend resource usage after reconnected, needed by resource view in GCS.
    node_resource_accessor_->AsyncReReportResourceUsage();
    break;
  default:
    RAY_LOG(FATAL) << "Unsupported failure type: " << type;
    break;
  }
}

void GcsClient::ReconnectGcsServer() {
  std::pair<std::string, int> address;
  auto timeout_s =
      absl::Seconds(RayConfig::instance().gcs_rpc_server_reconnect_timeout_s());
  auto start = absl::Now();
  auto reconnected = false;
  while (absl::Now() - start < timeout_s) {
    if (disconnected_) {
      return;
    }
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
        RAY_LOG(DEBUG)
            << "Repeated reconnection in "
            << RayConfig::instance().minimum_gcs_reconnect_interval_milliseconds()
            << " milliseconds, return directly.";
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
        reconnected = true;
        break;
      }
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(kGCSReconnectionRetryIntervalMs));
  }

  if (reconnected) {
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
