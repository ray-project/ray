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

#include "ray/pubsub/gcs_subscriber.h"

namespace ray {
namespace pubsub {

Status GcsSubscriber::SubscribeAllVirtualClusters(
    const gcs::SubscribeCallback<VirtualClusterID, rpc::VirtualClusterTableData>
        &subscribe,
    const gcs::StatusCallback &done) {
  // GCS subscriber.
  auto subscribe_item_callback = [subscribe](rpc::PubMessage &&msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::RAY_VIRTUAL_CLUSTER_CHANNEL);
    const VirtualClusterID id = VirtualClusterID::FromBinary(msg.key_id());
    subscribe(id, std::move(*msg.mutable_virtual_cluster_message()));
  };
  auto subscription_failure_callback = [](const std::string &, const Status &status) {
    RAY_LOG(WARNING) << "Subscription to virtual cluster channel failed: "
                     << status.ToString();
  };
  subscriber_->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::RAY_VIRTUAL_CLUSTER_CHANNEL,
      gcs_address_,
      /*key_id=*/std::nullopt,
      [done](const Status &status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscribe_item_callback),
      std::move(subscription_failure_callback));

  return Status::OK();
}

}  // namespace pubsub
}  // namespace ray
