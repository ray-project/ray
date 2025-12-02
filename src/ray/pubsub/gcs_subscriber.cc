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

#include <memory>
#include <string>
#include <utility>

namespace ray {
namespace pubsub {

void GcsSubscriber::SubscribeAllJobs(
    const gcs::SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
    const gcs::StatusCallback &done) {
  auto subscribe_item_callback = [subscribe](rpc::PubMessage &&msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::GCS_JOB_CHANNEL);
    const JobID id = JobID::FromBinary(msg.key_id());
    subscribe(id, std::move(*msg.mutable_job_message()));
  };
  auto subscription_failure_callback = [](const std::string &, const Status &status) {
    RAY_LOG(WARNING) << "Subscription to Job channel failed: " << status.ToString();
  };
  subscriber_->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_JOB_CHANNEL,
      gcs_address_,
      /*key_id=*/std::nullopt,
      [done](const Status &status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscribe_item_callback),
      std::move(subscription_failure_callback));
}

void GcsSubscriber::SubscribeActor(
    const ActorID &id,
    const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const gcs::StatusCallback &done) {
  auto subscription_callback = [id, subscribe](rpc::PubMessage &&msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::GCS_ACTOR_CHANNEL);
    RAY_CHECK(msg.key_id() == id.Binary());
    subscribe(id, std::move(*msg.mutable_actor_message()));
  };
  auto subscription_failure_callback = [id](const std::string &failed_id,
                                            const Status &status) {
    RAY_CHECK(failed_id == id.Binary());
    RAY_LOG(WARNING) << "Subscription to Actor " << id.Hex()
                     << " failed: " << status.ToString();
  };
  subscriber_->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_ACTOR_CHANNEL,
      gcs_address_,
      /*key_id=*/id.Binary(),
      [done](const Status &status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscription_callback),
      std::move(subscription_failure_callback));
}

void GcsSubscriber::UnsubscribeActor(const ActorID &id) {
  subscriber_->Unsubscribe(
      rpc::ChannelType::GCS_ACTOR_CHANNEL, gcs_address_, id.Binary());
}

bool GcsSubscriber::IsActorUnsubscribed(const ActorID &id) {
  return !subscriber_->IsSubscribed(
      rpc::ChannelType::GCS_ACTOR_CHANNEL, gcs_address_, id.Binary());
}

void GcsSubscriber::SubscribeAllNodeInfo(
    const gcs::ItemCallback<rpc::GcsNodeInfo> &subscribe,
    const gcs::StatusCallback &done) {
  auto subscribe_item_callback = [subscribe](rpc::PubMessage &&msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
    subscribe(std::move(*msg.mutable_node_info_message()));
  };
  auto subscription_failure_callback = [](const std::string &, const Status &status) {
    RAY_LOG(WARNING) << "Subscription to NodeInfo channel failed: " << status.ToString();
  };
  subscriber_->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
      gcs_address_,
      /*key_id=*/std::nullopt,
      [done](const Status &status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscribe_item_callback),
      std::move(subscription_failure_callback));
}

void GcsSubscriber::SubscribeAllNodeAddressAndLiveness(
    const gcs::ItemCallback<rpc::GcsNodeAddressAndLiveness> &subscribe,
    const gcs::StatusCallback &done) {
  auto subscribe_item_callback = [subscribe](rpc::PubMessage &&msg) {
    RAY_CHECK(msg.channel_type() ==
              rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL);
    subscribe(std::move(*msg.mutable_node_address_and_liveness_message()));
  };
  auto subscription_failure_callback = [](const std::string &, const Status &status) {
    RAY_LOG(ERROR) << "Subscription to NodeAddressAndLiveness channel failed: "
                   << status.ToString();
  };
  subscriber_->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL,
      gcs_address_,
      /*key_id=*/std::nullopt,
      [done](const Status &status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscribe_item_callback),
      std::move(subscription_failure_callback));
}

void GcsSubscriber::SubscribeAllWorkerFailures(
    const gcs::ItemCallback<rpc::WorkerDeltaData> &subscribe,
    const gcs::StatusCallback &done) {
  auto subscribe_item_callback = [subscribe](rpc::PubMessage &&msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL);
    subscribe(std::move(*msg.mutable_worker_delta_message()));
  };
  auto subscription_failure_callback = [](const std::string &, const Status &status) {
    RAY_LOG(WARNING) << "Subscription to WorkerDelta channel failed: "
                     << status.ToString();
  };
  // Ignore if the subscription already exists, because the resubscription is intentional.
  subscriber_->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL,
      gcs_address_,
      /*key_id=*/std::nullopt,
      /*subscribe_done_callback=*/
      [done](const Status &status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscribe_item_callback),
      std::move(subscription_failure_callback));
}

}  // namespace pubsub
}  // namespace ray
