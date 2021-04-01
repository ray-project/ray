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

#include "ray/raylet/pubsub/subscriber.h"

namespace ray {

void Subscriber::SubcribeObject(
    const rpc::Address &owner_address, const ObjectID &object_id,
    SubscriptionCallback subscription_callback,
    SubscriptionFailureCallback subscription_failure_callback) {
  rpc::Address subscriber_address;
  subscriber_address.set_raylet_id(self_node_id_.Binary());
  subscriber_address.set_ip_address(self_node_address_);
  subscriber_address.set_port(self_node_port_);
  // Make a long polling connection if we never made the one with this owner for pubsub
  // operations.
  const auto owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
  auto subscription_it = subscription_map_.find(owner_worker_id);
  if (subscription_it == subscription_map_.end()) {
    subscription_it =
        subscription_map_.emplace(owner_worker_id, SubscriptionInfo(owner_address)).first;
    MakeLongPollingPubsubConnection(owner_address, subscriber_address);
  }
  RAY_CHECK(subscription_it != subscription_map_.end());
  RAY_CHECK(
      subscription_it->second.subscription_callback_map_
          .emplace(object_id, std::make_pair(std::move(subscription_callback),
                                             std::move(subscription_failure_callback)))
          .second);

  // Send a subscription message.
  auto owner_client = owner_client_pool_.GetOrConnect(owner_address);
  rpc::SubscribeForObjectEvictionRequest wait_request;
  wait_request.set_object_id(object_id.Binary());
  wait_request.set_intended_worker_id(owner_address.worker_id());
  wait_request.mutable_subscriber_address()->CopyFrom(subscriber_address);
  owner_client->SubscribeForObjectEviction(
      wait_request,
      [this, owner_address, object_id](
          Status status, const rpc::SubscribeForObjectEvictionReply &reply) {
        if (!status.ok()) {
          auto maybe_failure_callback = GetFailureCallback(owner_address, object_id);
          if (maybe_failure_callback.has_value()) {
            const auto &failure_callback = maybe_failure_callback.value();
            // If the object id is still subscribed, invoke a failure callback.
            failure_callback(object_id);
            RAY_CHECK(UnsubscribeObject(owner_address, object_id));
          }
        }
      });
}

bool Subscriber::UnsubscribeObject(const rpc::Address &owner_address,
                                   const ObjectID &object_id) {
  const auto owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
  RAY_LOG(DEBUG) << "Unsubscribing an object " << object_id << " from "
                 << owner_worker_id;
  auto subscription_it = subscription_map_.find(owner_worker_id);
  if (subscription_it == subscription_map_.end()) {
    return false;
  }
  auto &subscription_callback_map = subscription_it->second.subscription_callback_map_;
  auto subscription_callback_it = subscription_callback_map.find(object_id);
  if (subscription_callback_it == subscription_callback_map.end()) {
    return false;
  }
  subscription_callback_map.erase(subscription_callback_it);
  return true;
}

void Subscriber::MakeLongPollingPubsubConnection(const rpc::Address &owner_address,
                                                 const rpc::Address &subscriber_address) {
  const auto owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
  RAY_LOG(DEBUG) << "Make a long polling request to " << owner_worker_id;

  auto owner_client = owner_client_pool_.GetOrConnect(owner_address);
  rpc::PubsubLongPollingRequest long_polling_request;
  long_polling_request.mutable_subscriber_address()->CopyFrom(subscriber_address);

  owner_client->PubsubLongPolling(
      long_polling_request, [this, owner_address, subscriber_address](
                                Status status, const rpc::PubsubLongPollingReply &reply) {
        HandleLongPollingResponse(owner_address, subscriber_address, status, reply);
      });
}

void Subscriber::HandleLongPollingResponse(const rpc::Address &owner_address,
                                           const rpc::Address &subscriber_address,
                                           const Status &status,
                                           const rpc::PubsubLongPollingReply &reply) {
  const auto owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
  RAY_LOG(DEBUG) << "Long polling request has replied from " << owner_worker_id;

  auto subscription_it = subscription_map_.find(owner_worker_id);
  RAY_CHECK(subscription_it != subscription_map_.end());

  if (!status.ok()) {
    // If status is not okay, we treat that the owner is dead. Release all objects
    // that are subscribed by this owner.
    RAY_LOG(INFO) << "A worker is dead. Release all objects from the owner: "
                  << owner_worker_id;
    std::vector<ObjectID> objects_to_unsubscribe;
    auto &subscription_callback_map = subscription_it->second.subscription_callback_map_;
    for (const auto &object_id_it : subscription_callback_map) {
      const auto &object_id = object_id_it.first;
      objects_to_unsubscribe.push_back(object_id);

      auto maybe_failure_callback = GetFailureCallback(owner_address, object_id);
      if (maybe_failure_callback.has_value()) {
        // If the object id is still subscribed, invoke a failure callback.
        const auto &failure_callback = maybe_failure_callback.value();
        failure_callback(object_id);
      }
    }

    for (const auto &object_id_to_unsubscribe : objects_to_unsubscribe) {
      // If the owner is failed, we automatically unsubscribe objects from this owner.
      // If the failure callback called UnsubscribeObject, this will raise check failures.
      RAY_CHECK(UnsubscribeObject(owner_address, object_id_to_unsubscribe))
          << "Calling UnsubscribeObject inside a failure callback is not allowed.";
    }
  } else {
    // Otherwise, release objects that are reported from the long polling
    // connection.
    for (const auto &object_id_binary : reply.object_ids()) {
      const auto object_id = ObjectID::FromBinary(object_id_binary);
      RAY_LOG(DEBUG) << "Object id " << object_id << " information was published from "
                     << owner_worker_id << ". Releasing the object.";
      auto maybe_subscription_callback =
          GetSubscriptionCallback(owner_address, object_id);
      if (maybe_subscription_callback.has_value()) {
        // If the object id is still subscribed, invoke a subscription callback.
        const auto &subscription_callback = maybe_subscription_callback.value();
        subscription_callback(object_id);
      }
    }
  }
  subscription_it = subscription_map_.find(owner_worker_id);
  if (subscription_it->second.subscription_callback_map_.size() == 0) {
    // If there's no more subscription, Erase the entry. We erase the connection entry
    // here instead of Unsubscribe method because there could be in-flight long polling
    // request when all of objects are unsubscribed. Since the long polling request should
    // be sent in flight if there are still subscriptions, we can guarantee it will
    // properly clean up metadata.
    // TODO(sang): Send a RPC to unregister the subscriber.
    subscription_map_.erase(subscription_it);
  } else {
    // If there are still subscriptions, make another long polling request.
    MakeLongPollingPubsubConnection(owner_address, subscriber_address);
  }
}

inline absl::optional<SubscriptionCallback> Subscriber::GetSubscriptionCallback(
    const rpc::Address &owner_address, const ObjectID &object_id) const {
  const auto owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
  auto subscription_it = subscription_map_.find(owner_worker_id);
  if (subscription_it == subscription_map_.end()) {
    return absl::nullopt;
  }
  auto callback_it = subscription_it->second.subscription_callback_map_.find(object_id);
  bool exist = callback_it != subscription_it->second.subscription_callback_map_.end();
  if (!exist) {
    return absl::nullopt;
  }
  auto subscription_callback = callback_it->second.first;
  return absl::optional<SubscriptionCallback>{subscription_callback};
}

inline absl::optional<SubscriptionCallback> Subscriber::GetFailureCallback(
    const rpc::Address &owner_address, const ObjectID &object_id) const {
  const auto owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
  auto subscription_it = subscription_map_.find(owner_worker_id);
  if (subscription_it == subscription_map_.end()) {
    return absl::nullopt;
  }
  auto callback_it = subscription_it->second.subscription_callback_map_.find(object_id);
  bool exist = callback_it != subscription_it->second.subscription_callback_map_.end();
  if (!exist) {
    return absl::nullopt;
  }
  auto subscription_callback = callback_it->second.second;
  return absl::optional<SubscriptionCallback>{subscription_callback};
}

bool Subscriber::AssertNoLeak() const {
  for (const auto &subscription : subscription_map_) {
    if (subscription.second.subscription_callback_map_.size() != 0) {
      return false;
    }
  }
  return subscription_map_.size() == 0;
}

}  // namespace ray
