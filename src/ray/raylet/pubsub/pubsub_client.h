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

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

using EmptyCallback = std::function<void()>;
using ObjectIdSubscriptionCallback = std::function<void(const ObjectID &)>
using StatusCallback = std::function<void(const Status &)>

enum class ChannelName : uint32_t {
  OBJECT_EVICTION = 0
  // TODO(sang): Add the object directory channel.
};

// TODO(sang): Add a mechanism for ordering if needed.
struct Channel {
  Channel(const ChannelName &channel_name, const rpc::Address address)
    : channel_name_(channel_name),
      address_(address) {}

  const ChannelName channel_name_;
  const rpc::Address address_;
};

// class PubsubClient {
//  public:
//   bool ConnectIfNeeded(const Channel &channel, const EmptyCallback &connect_callback);
//   // Subscribe
//   void Subscribe(const Channel &channel, const ObjectID &object_id, const ObjectIdSubscriptionCallback &subscription_callback, const StatusCallback &callback);
// };

} // namespace ray
