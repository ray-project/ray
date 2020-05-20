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

#include "ray/common/ray_exception.h"

namespace ray {
std::shared_ptr<RayException> RayErrorBuilder::FromProto(rpc::RayException message) {
  switch (message.error_type()) {
  case rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE:
    return std::shared_ptr<RayException>(new RayObjectException(std::move(message)));
  case rpc::ErrorType::ACTOR_DIED:
    return std::shared_ptr<RayException>(new RayActorException(std::move(message)));
  case rpc::ErrorType::WORKER_DIED:
    return std::shared_ptr<RayException>(new RayWorkerException(std::move(message)));
  case rpc::ErrorType::TASK_EXECUTION_EXCEPTION:
    return std::shared_ptr<RayException>(new RayTaskException(std::move(message)));
  default:
    break;
  }
  RAY_LOG(FATAL) << "Unsupported error type: "
                 << rpc::ErrorType_Name(message.error_type());
  return std::shared_ptr<RayException>();
}

std::shared_ptr<RayException> RayErrorBuilder::Deserialize(
    const std::string &serialized_binary) {
  rpc::RayException message;
  message.ParseFromString(serialized_binary);
  return RayErrorBuilder::FromProto(std::move(message));
}
}  // namespace ray
