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

#include "ray/core_worker/gcs_server_address_updater.h"

namespace ray {
namespace core {

GcsServerAddressUpdater::GcsServerAddressUpdater(
    const std::string raylet_ip_address, const int port,
    std::function<void(std::string, int)> update_func)
    : update_func_(update_func) {
  // Init updater thread and run its io service.
  updater_thread_.reset(new std::thread([this] {
    SetThreadName("gcs_address_updater");
    /// The asio work to keep io_service_ alive.
    boost::asio::io_service::work io_service_work_(updater_io_service_);
    updater_io_service_.run();
  }));
  client_call_manager_.reset(new rpc::ClientCallManager(updater_io_service_));
  auto grpc_client =
      rpc::NodeManagerWorkerClient::make(raylet_ip_address, port, *client_call_manager_);
  raylet_client_ = std::make_shared<raylet::RayletClient>(grpc_client);
  // Init updater runner.
  updater_runner_.reset(new PeriodicalRunner(updater_io_service_));
  // Start updating gcs server address.
  updater_runner_->RunFnPeriodically(
      [this] { UpdateGcsServerAddress(); },
      RayConfig::instance().gcs_service_address_check_interval_milliseconds());
}

GcsServerAddressUpdater::~GcsServerAddressUpdater() {
  updater_runner_.reset();
  updater_io_service_.stop();
  if (updater_thread_->joinable()) {
    updater_thread_->join();
  }
  updater_thread_.reset();
  raylet_client_.reset();
}

void GcsServerAddressUpdater::UpdateGcsServerAddress() {
  RAY_LOG(DEBUG) << "Getting gcs server address from raylet.";
  raylet_client_->GetGcsServerAddress([this](const Status &status,
                                             const rpc::GetGcsServerAddressReply &reply) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to get gcs server address from Raylet: " << status;
      failed_ping_count_ += 1;
      if (failed_ping_count_ == RayConfig::instance().ping_gcs_rpc_server_max_retries()) {
        RAY_LOG(FATAL) << "Failed to receive the GCS address from the raylet for "
                       << failed_ping_count_ << " times. Killing itself.";
      }
    } else {
      failed_ping_count_ = 0;
      update_func_(reply.ip(), reply.port());
    }
  });
}

}  // namespace core
}  // namespace ray
