// Copyright 2020 The Ray Authors.
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
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/stats/metric.h"

// This header is used to warp some internal code so we can reduce suspicious
// symbols export.
namespace ray {
namespace internal {

using ray::core::RayFunction;

/// Send buffer internal
/// \param[in] buffer buffer to be sent.
/// \param[in] function the function descriptor of peer's function.
/// \param[in] return_num return value number of the call.
/// \param[out] return_ids return ids from SubmitActorTask.
std::vector<rpc::ObjectReference> SendInternal(const ActorID &peer_actor_id,
                                               std::shared_ptr<LocalMemoryBuffer> buffer,
                                               RayFunction &function,
                                               int return_num);

const stats::TagKeyType TagRegister(const std::string tag_name);

/// Get current actor id via internal.
const ActorID &GetCurrentActorID();

/// Get core worker initialization flag via internal.
bool IsInitialized();
}  // namespace internal
}  // namespace ray
