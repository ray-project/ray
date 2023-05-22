#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {
namespace rpc {
grpc::ChannelArguments GetGcsRpcClientArguments() {
  grpc::ChannelArguments arguments = CreateDefaultChannelArguments();
  arguments.SetInt(GRPC_ARG_MAX_MESSAGE_LENGTH, 512 * 1024 * 1024);
  arguments.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS,
                   ::RayConfig::instance().gcs_grpc_max_reconnect_backoff_ms());
  arguments.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS,
                   ::RayConfig::instance().gcs_grpc_min_reconnect_backoff_ms());
  arguments.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS,
                   ::RayConfig::instance().gcs_grpc_initial_reconnect_backoff_ms());
  return arguments;
}

std::shared_ptr<grpc::Channel> GcsRpcClient::GetDefaultChannel(const std::string &address, int port) {
  static std::shared_ptr<grpc::Channel> channel_;
  static std::mutex mu_;
  static std::string address_;
  static int port_;

  std::lock_guard<std::mutex> guard(mu_);
  if (channel_ == nullptr) {
    address_ = address;
    port_ = port;
    channel_ = BuildChannel(address_, port_, GetGcsRpcClientArguments());
  }

  if(address_ == address && port_ == port) {
    return channel_;
  } else {
    RAY_LOG(INFO) << "Generate a new GCS channel: " << address << ":" << port;
    return BuildChannel(address_, port_, GetGcsRpcClientArguments());
  }
}

}
}
