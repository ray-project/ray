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

#include <functional>
#include <utility>

#include "ray/asio/instrumented_io_context.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray::raylet {

/// Agent monitor threads may invoke shutdown. Copy the death info and always post
/// the callback so teardown never runs inline on a monitor thread.
inline std::function<void(const rpc::NodeDeathInfo &)> MakeRayletShutdownCallback(
    instrumented_io_context &io_service,
    std::function<void(const rpc::NodeDeathInfo &)> shutdown) {
  return [&io_service, shutdown = std::move(shutdown)](
             const rpc::NodeDeathInfo &death_info) {
    io_service.post([shutdown, death_info] { shutdown(death_info); },
                    "Raylet.Shutdown");
  };
}

}  // namespace ray::raylet
