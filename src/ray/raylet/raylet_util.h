// Copyright 2023 The Ray Authors.
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

#include <csignal>

namespace ray {
namespace raylet {

inline void ShutdownRayletGracefully() {
    // Implementation note: When raylet is shutdown by ray stop, the CLI sends a
    // sigterm. Raylet knows how to gracefully shutdown when it receives a sigterm. Here,
    // we raise a sigterm to itself so that it can re-use the same graceful shutdown code
    // path. The sigterm is handled in the entry point (raylet/main.cc)'s signal handler.

    RAY_LOG(INFO) << "Sending SIGTERM to gracefully shutdown raylet";
    // raise return 0 if succeeds. If it fails to gracefully shutdown, it kills itself
    // forcefully.
    RAY_CHECK_EQ(std::raise(SIGTERM), 0)
        << "There was a failure while sending a sigterm to itself. Raylet will not "
           "gracefully shutdown.";
}

}
}