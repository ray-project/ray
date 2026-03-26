// Copyright 2026 The Ray Authors.
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

#include "ray/gcs/pubsub_handler.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "ray/common/asio/fake_periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/pubsub/publisher.h"

namespace ray {
namespace gcs {

class PubSubHandlerTest : public ::testing::Test {
 public:
  PubSubHandlerTest() {
    fake_periodical_runner_ = std::make_unique<FakePeriodicalRunner>();

    // Create a publisher with specific channels (matching GCS server setup).
    auto inner_publisher = std::make_unique<pubsub::Publisher>(
        /*channels=*/
        std::vector<rpc::ChannelType>{
            rpc::ChannelType::GCS_ACTOR_CHANNEL,
            rpc::ChannelType::GCS_JOB_CHANNEL,
            rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
            rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL,
            rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL,
            rpc::ChannelType::RAY_ERROR_INFO_CHANNEL,
            rpc::ChannelType::RAY_LOG_CHANNEL,
            rpc::ChannelType::RAY_NODE_RESOURCE_USAGE_CHANNEL},
        /*periodical_runner=*/*fake_periodical_runner_,
        /*get_time_ms=*/[]() { return 0.0; },
        /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
        /*publish_batch_size_=*/RayConfig::instance().publish_batch_size(),
        /*publisher_id=*/NodeID::FromRandom());

    gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(std::move(inner_publisher));

    pubsub_handler_ =
        std::make_unique<InternalPubSubHandler>(io_service_, *gcs_publisher_);
  }

 protected:
  std::unique_ptr<InternalPubSubHandler> pubsub_handler_;

 private:
  instrumented_io_context io_service_;
  std::unique_ptr<FakePeriodicalRunner> fake_periodical_runner_;
  std::unique_ptr<pubsub::GcsPublisher> gcs_publisher_;
};

TEST_F(PubSubHandlerTest, HandleGcsSubscriberCommandBatchInvalidChannelType) {
  // Test that HandleGcsSubscriberCommandBatch returns InvalidArgument for an invalid
  // channel type. Using a worker-specific channel that was not registered with the
  // GCS publisher should return InvalidArgument.
  const auto subscriber_id = UniqueID::FromRandom();
  const auto key_id = ObjectID::FromRandom();

  rpc::GcsSubscriberCommandBatchRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  auto *command = request.add_commands();
  command->set_channel_type(rpc::ChannelType::WORKER_OBJECT_EVICTION);
  command->set_key_id(key_id.Binary());
  command->mutable_subscribe_message();

  rpc::GcsSubscriberCommandBatchReply reply;
  Status received_status;

  pubsub_handler_->HandleGcsSubscriberCommandBatch(
      request,
      &reply,
      [&received_status](const Status &status,
                         std::function<void()>,
                         std::function<void()>) { received_status = status; });

  ASSERT_FALSE(received_status.ok());
  ASSERT_TRUE(received_status.IsInvalidArgument());
  EXPECT_TRUE(received_status.message().find("Invalid channel type") !=
              std::string::npos);
}

TEST_F(PubSubHandlerTest, HandleGcsSubscriberCommandBatchValidChannelType) {
  const auto subscriber_id = UniqueID::FromRandom();
  const auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);

  rpc::GcsSubscriberCommandBatchRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  auto *command = request.add_commands();
  // Use GCS_ACTOR_CHANNEL which is registered with the GCS publisher.
  command->set_channel_type(rpc::ChannelType::GCS_ACTOR_CHANNEL);
  command->set_key_id(actor_id.Binary());
  command->mutable_subscribe_message();

  rpc::GcsSubscriberCommandBatchReply reply;
  Status received_status;

  pubsub_handler_->HandleGcsSubscriberCommandBatch(
      request,
      &reply,
      [&received_status](const Status &status,
                         std::function<void()>,
                         std::function<void()>) { received_status = status; });

  ASSERT_TRUE(received_status.ok()) << received_status.message();
}

TEST_F(
    PubSubHandlerTest,
    HandleGcsSubscriberCommandBatchMissingSubscribeOrUnsubscribeReturnsInvalidArgument) {
  const auto subscriber_id = UniqueID::FromRandom();
  const auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);

  rpc::GcsSubscriberCommandBatchRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  auto *command = request.add_commands();
  command->set_channel_type(rpc::ChannelType::GCS_ACTOR_CHANNEL);
  command->set_key_id(actor_id.Binary());

  rpc::GcsSubscriberCommandBatchReply reply;
  Status received_status;

  pubsub_handler_->HandleGcsSubscriberCommandBatch(
      request,
      &reply,
      [&received_status](const Status &status,
                         std::function<void()>,
                         std::function<void()>) { received_status = status; });

  ASSERT_TRUE(received_status.IsInvalidArgument());
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
