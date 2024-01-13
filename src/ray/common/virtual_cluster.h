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

#include "ray/common/bundle_spec.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

class VirtualClusterSpecification : public MessageWrapper<rpc::VirtualClusterSpec> {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit VirtualClusterSpecification(rpc::VirtualClusterSpec message)
      : MessageWrapper(std::move(message)) {}
  /// Construct from a protobuf message shared_ptr.
  ///
  /// \param message The protobuf message.
  explicit VirtualClusterSpecification(std::shared_ptr<rpc::VirtualClusterSpec> message)
      : MessageWrapper(message) {}
  /// Return the virtual cluster id.
  VirtualClusterID VirtualClusterId() const;
};

// TODO: make a VirtualClusterSpecBuilder to make sure a VC Spec is valid.

}  // namespace ray
