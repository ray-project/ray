// Copyright 2024 The Ray Authors.
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
//
// This file defines a few constants on ray syncer.

#pragma once

#include <functional>

#include "ray/common/id.h"
#include "src/ray/protobuf/ray_syncer.grpc.pb.h"
#include "src/ray/protobuf/ray_syncer.pb.h"

namespace ray::syncer {

inline constexpr size_t kComponentArraySize =
    static_cast<size_t>(ray::rpc::syncer::MessageType_ARRAYSIZE);

// TODO(hjiang): As of now, only ray syncer uses it so we put it under `ray_syncer`
// folder, better to place it into other common folders if uses elsewhere.
//
// A callback, which is called whenever a rpc succeeds (at rpc communication level)
// between the current node and the remote node.
using RpcCompletionCallback = std::function<void(const NodeID &)>;

}  // namespace ray::syncer
