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

#pragma once

#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {
namespace rpc {

/// Abstract client interface for object manager clients.
class ObjectManagerClientInterface {
 public:
  virtual ~ObjectManagerClientInterface() = default;

  /// Push object to remote object manager
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server
  virtual void Push(const PushRequest &request,
                    const ClientCallback<PushReply> &callback) = 0;

  /// Pull object from remote object manager
  ///
  /// \param request The request message
  /// \param callback The callback function that handles reply from server
  virtual void Pull(const PullRequest &request,
                    const ClientCallback<PullReply> &callback) = 0;

  /// Tell remote object manager to free objects
  ///
  /// \param request The request message
  /// \param callback The callback function that handles reply
  virtual void FreeObjects(const FreeObjectsRequest &request,
                           const ClientCallback<FreeObjectsReply> &callback) = 0;
};

}  // namespace rpc
}  // namespace ray
