// Copyright 2025 The Ray Authors.
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

#include "ray/gcs_rpc_client/default_accessor_factory.h"

#include "ray/gcs_rpc_client/accessors/actor_info_accessor.h"

namespace ray {
namespace gcs {

std::unique_ptr<ActorInfoAccessorInterface>
DefaultAccessorFactory::CreateActorInfoAccessor(GcsClientContext *context) {
  return std::make_unique<ActorInfoAccessor>(context);
}

}  // namespace gcs
}  // namespace ray
