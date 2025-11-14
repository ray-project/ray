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

#include <functional>

#include "ray/common/status.h"

namespace ray {
namespace rpc {

/// Represents the callback function to be called when a `ServiceHandler` finishes
/// handling a request.
/// \param status The status would be returned to client.
/// \param success Success callback which will be invoked when the reply is successfully
/// sent to the client.
/// \param failure Failure callback which will be invoked when the reply fails to be
/// sent to the client.
using SendReplyCallback = std::function<void(
    Status status, std::function<void()> success, std::function<void()> failure)>;

/// Represents the client callback function of a particular rpc method.
///
/// \tparam Reply Type of the reply message.
template <class Reply>
using ClientCallback = std::function<void(const Status &status, Reply &&reply)>;

/// This callback is used to notify when a write/subscribe to a rpc server completes.
/// \param status Status indicates whether the write/subscribe was successful.
using StatusCallback = std::function<void(Status status)>;

/// This callback is used to receive one item from a rpc server when a read completes.
/// \param status Status indicates whether the read was successful.
/// \param result The item returned by the rpc server. If the item to read doesn't exist,
/// this optional object is empty.
template <typename Data>
using OptionalItemCallback =
    std::function<void(Status status, std::optional<Data> result)>;

/// This callback is used to receive multiple items from a rpc server when a read
/// completes.
/// \param status Status indicates whether the read was successful.
/// \param result The items returned by the rpc server.
template <typename Data>
using MultiItemCallback = std::function<void(Status status, std::vector<Data> result)>;

/// This callback is used to receive notifications of the subscribed items in a rpc
/// server.
/// \param id The id of the item.
/// \param result The notification message.
template <typename ID, typename Data>
using SubscribeCallback = std::function<void(const ID &id, Data &&result)>;

/// This callback is used to receive a single item from a rpc server.
/// \param result The item returned by the rpc server.
template <typename Data>
using ItemCallback = std::function<void(Data &&result)>;

}  // namespace rpc
}  // namespace ray
