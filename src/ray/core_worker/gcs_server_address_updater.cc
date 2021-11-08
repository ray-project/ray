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
      failed_ping_count_ += 1;
      auto warning_threshold =
          RayConfig::instance().ping_gcs_rpc_server_max_retries() / 2;
      RAY_LOG_EVERY_N(WARNING, warning_threshold)
          << "Failed to get the gcs server address from raylet " << failed_ping_count_
          << " times in a row. If it keeps failing to obtain the address, "
             "the worker might crash. Connection status "
          << status;
      if (failed_ping_count_ == RayConfig::instance().ping_gcs_rpc_server_max_retries()) {
        std::stringstream os;
        os << "Failed to receive the GCS address for " << failed_ping_count_
           << " times without success. The worker will exit ungracefully. It is because ";
        if (IsRayletFailed(RayConfig::instance().RAYLET_PID())) {
          RAY_LOG(WARNING) << os.str()
                           << "raylet has died, and it couldn't obtain the GCS address "
                              "from the raylet anymore. Please check the log from "
                              "raylet.err on this address.";
        } else {
          RAY_LOG(ERROR)
              << os.str()
              << "GCS has died. It could be because there was an issue that "
                 "kills GCS, such as high memory usage triggering OOM killer "
                 "to kill GCS. Cluster will be highly likely unavailable if you see "
                 "this log. Please check the log from gcs_server.err.";
        }
        QuickExit();
      }
    } else {
      failed_ping_count_ = 0;
      update_func_(reply.ip(), reply.port());
    }
  });
}

}  // namespace core
}  // namespace ray
