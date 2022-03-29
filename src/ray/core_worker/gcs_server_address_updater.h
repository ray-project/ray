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

#pragma once

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/raylet_client/raylet_client.h"

namespace ray {
namespace core {

class GcsServerAddressUpdater {
 public:
  /// Create a updater for gcs server address.
  ///
  /// \param raylet_ip_address Raylet ip address.
  /// \param port Port to connect raylet.
  /// \param address to store gcs server address.
  GcsServerAddressUpdater(const std::string raylet_ip_address,
                          const int port,
                          std::function<void(std::string, int)> update_func);

  ~GcsServerAddressUpdater();

 private:
  /// Update gcs server address.
  void UpdateGcsServerAddress();

  rpc::ClientCallManager client_call_manager_;
  /// A client connection to the raylet.
  raylet::RayletClient raylet_client_;
  std::function<void(std::string, int)> update_func_;
  instrumented_io_context updater_io_service_;
  PeriodicalRunner updater_runner_;
  std::thread updater_thread_;
  int32_t failed_ping_count_ = 0;
};

}  // namespace core
}  // namespace ray
