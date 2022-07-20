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

#include <boost/optional/optional.hpp>
#include <vector>

#include "ray/common/status.h"

namespace ray {

namespace gcs {

/// This callback is used to notify when a operation completes.
using EmptyCallback = std::function<void()>;

/// This callback is used to notify when a write/subscribe to GCS completes.
/// \param status Status indicates whether the write/subscribe was successful.
using StatusCallback = std::function<void(Status status)>;

/// This callback is used to receive one item from GCS when a read completes.
/// \param status Status indicates whether the read was successful.
/// \param result The item returned by GCS. If the item to read doesn't exist,
/// this optional object is empty.
template <typename Data>
using OptionalItemCallback =
    std::function<void(Status status, const boost::optional<Data> &result)>;

/// This callback is used to receive multiple items from GCS when a read completes.
/// \param status Status indicates whether the read was successful.
/// \param result The items returned by GCS.
template <typename Data>
using MultiItemCallback = std::function<void(Status status, std::vector<Data> &&result)>;

/// This callback is used to receive notifications of the subscribed items in the GCS.
/// \param id The id of the item.
/// \param result The notification message.
template <typename ID, typename Data>
using SubscribeCallback = std::function<void(const ID &id, const Data &result)>;

/// This callback is used to receive a single item from GCS.
/// \param result The item returned by GCS.
template <typename Data>
using ItemCallback = std::function<void(const Data &result)>;

/// This callback is used to receive multiple key-value items from GCS.
/// \param result The key-value items returned by GCS.
template <typename Key, typename Value>
using MapCallback = std::function<void(absl::flat_hash_map<Key, Value> &&result)>;

}  // namespace gcs

}  // namespace ray
