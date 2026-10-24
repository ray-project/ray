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

#include "ray/asio/fake_periodical_runner.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/pubsub/publisher.h"
#include "ray/util/clock.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

class PubSubHandlerTest : public ::testing::Test {
 public:
  PubSubHandlerTest() {
    fake_periodical_runner_ = std::make_unique<FakePeriodicalRunner>();
    fake_obs_periodical_runner_ = std::make_unique<FakePeriodicalRunner>();

    // Create a publisher with specific channels (matching GCS server setup).
    auto inner_publisher = std::make_unique<pubsub::Publisher>(
        /*channels=*/
        std::vector<rpc::ChannelType>{
            rpc::ChannelType::GCS_ACTOR_CHANNEL,
            rpc::ChannelType::GCS_JOB_CHANNEL,
            rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
            rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL,
            rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL},
        /*periodical_runner=*/*fake_periodical_runner_,
        /*clock=*/clock_,
        /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
        /*publish_batch_size_=*/RayConfig::instance().publish_batch_size(),
        /*publisher_id=*/NodeID::FromRandom());

    publisher_ = inner_publisher.get();
    gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(std::move(inner_publisher));

    auto obs_inner = std::make_unique<pubsub::Publisher>(
        /*channels=*/
        std::vector<rpc::ChannelType>{rpc::ChannelType::RAY_ERROR_INFO_CHANNEL,
                                      rpc::ChannelType::RAY_LOG_CHANNEL,
                                      rpc::ChannelType::RAY_NODE_RESOURCE_USAGE_CHANNEL},
        /*periodical_runner=*/*fake_obs_periodical_runner_,
        /*clock=*/clock_,
        /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
        /*publish_batch_size_=*/RayConfig::instance().publish_batch_size(),
        /*publisher_id=*/NodeID::FromRandom());

    observability_publisher_ =
        std::make_unique<pubsub::ObservabilityPublisher>(std::move(obs_inner));

    pubsub_handler_ =
        std::make_unique<ControlPlanePubSubHandler>(io_service_, *gcs_publisher_);
    observability_pubsub_handler_ = std::make_unique<ObservabilityPubSubHandler>(
        io_service_, *observability_publisher_);
  }

 protected:
  std::unique_ptr<ControlPlanePubSubHandler> pubsub_handler_;
  std::unique_ptr<ObservabilityPubSubHandler> observability_pubsub_handler_;
  pubsub::Publisher *publisher_ = nullptr;

  instrumented_io_context io_service_;
  // Declared before the publishers so it outlives the Publishers that hold a
  // ClockInterface& to it.
  ray::FakeClock clock_;
  std::unique_ptr<FakePeriodicalRunner> fake_periodical_runner_;
  std::unique_ptr<FakePeriodicalRunner> fake_obs_periodical_runner_;
  std::unique_ptr<pubsub::GcsPublisher> gcs_publisher_;
  std::unique_ptr<pubsub::ObservabilityPublisher> observability_publisher_;
};

TEST_F(PubSubHandlerTest, StrictPollRejectsUnknownSubscriberWithoutCreatingState) {
  rpc::GcsSubscriberPollRequest request;
  request.set_subscriber_id(UniqueID::FromRandom().Binary());
  request.set_require_subscriber(true);

  // Both polls must fail immediately. A rejected poll must not create state.
  for (int i = 0; i < 2; ++i) {
    rpc::GcsSubscriberPollReply reply;
    int replies = 0;
    pubsub_handler_->HandleGcsSubscriberPoll(
        request,
        &reply,
        [&replies](const Status &status, std::function<void()>, std::function<void()>) {
          ++replies;
          EXPECT_TRUE(status.IsNotFound());
          EXPECT_EQ(RayStatusToGrpcStatus(status).error_code(),
                    grpc::StatusCode::ABORTED);
        });
    ASSERT_EQ(replies, 1);
    EXPECT_TRUE(reply.publisher_id().empty());
    EXPECT_TRUE(reply.pub_messages().empty());
  }
}

TEST_F(PubSubHandlerTest, LegacyPollBeforeRegistrationReceivesMessages) {
  const auto subscriber_id = UniqueID::FromRandom();
  rpc::GcsSubscriberPollRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  rpc::GcsSubscriberPollReply reply;
  int replies = 0;
  pubsub_handler_->HandleGcsSubscriberPoll(
      request,
      &reply,
      [&replies](const Status &status, std::function<void()>, std::function<void()>) {
        ++replies;
        EXPECT_TRUE(status.ok());
      });
  EXPECT_EQ(replies, 0);

  rpc::GcsSubscriberCommandBatchRequest command_request;
  command_request.set_subscriber_id(subscriber_id.Binary());
  auto *command = command_request.add_commands();
  command->set_channel_type(rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
  command->mutable_subscribe_message();
  rpc::GcsSubscriberCommandBatchReply command_reply;
  pubsub_handler_->HandleGcsSubscriberCommandBatch(
      command_request,
      &command_reply,
      [](const Status &status, std::function<void()>, std::function<void()>) {
        EXPECT_TRUE(status.ok());
      });

  rpc::PubMessage message;
  message.set_channel_type(rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
  message.mutable_node_info_message()->set_node_id(NodeID::FromRandom().Binary());
  gcs_publisher_->GetPublisher().Publish(message);
  ASSERT_EQ(replies, 1);
  ASSERT_EQ(reply.pub_messages_size(), 1);
  EXPECT_EQ(reply.pub_messages(0).node_info_message().node_id(),
            message.node_info_message().node_id());
}

TEST_F(PubSubHandlerTest, StrictPollDetectsEvictionAndAcceptsRegistrationAgain) {
  const auto subscriber_id = UniqueID::FromRandom();
  rpc::GcsSubscriberCommandBatchRequest command_request;
  command_request.set_subscriber_id(subscriber_id.Binary());
  auto *command = command_request.add_commands();
  command->set_channel_type(rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
  command->mutable_subscribe_message();

  rpc::GcsSubscriberPollRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  request.set_require_subscriber(true);

  // Exercise both initial registration and recovery using the same subscriber ID.
  for (int i = 0; i < 2; ++i) {
    rpc::GcsSubscriberCommandBatchReply command_reply;
    pubsub_handler_->HandleGcsSubscriberCommandBatch(
        command_request,
        &command_reply,
        [](const Status &status, std::function<void()>, std::function<void()>) {
          EXPECT_TRUE(status.ok());
        });

    rpc::GcsSubscriberPollReply reply;
    int replies = 0;
    pubsub_handler_->HandleGcsSubscriberPoll(
        request,
        &reply,
        [&replies](const Status &status, std::function<void()>, std::function<void()>) {
          ++replies;
          EXPECT_TRUE(status.ok());
        });
    EXPECT_EQ(replies, 0);
    rpc::PubMessage message;
    message.set_channel_type(rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
    message.mutable_node_info_message()->set_node_id(NodeID::FromRandom().Binary());
    gcs_publisher_->GetPublisher().Publish(message);
    ASSERT_EQ(replies, 1);
    ASSERT_EQ(reply.pub_messages_size(), 1);
    EXPECT_EQ(reply.pub_messages(0).node_info_message().node_id(),
              message.node_info_message().node_id());

    clock_.AdvanceTime(absl::Milliseconds(RayConfig::instance().subscriber_timeout_ms()));
    publisher_->CheckDeadSubscribers();

    rpc::GcsSubscriberPollReply expired_reply;
    int expired_replies = 0;
    pubsub_handler_->HandleGcsSubscriberPoll(
        request,
        &expired_reply,
        [&expired_replies](
            const Status &status, std::function<void()>, std::function<void()>) {
          ++expired_replies;
          EXPECT_TRUE(status.IsNotFound());
        });
    ASSERT_EQ(expired_replies, 1);
    EXPECT_TRUE(expired_reply.pub_messages().empty());
  }
}

TEST_F(PubSubHandlerTest, HandleGcsSubscriberCommandBatchInvalidChannelType) {
  // Test that HandleGcsSubscriberCommandBatch returns InvalidArgument for an invalid
  // channel type. Using a worker-specific channel that was not registered with the
  // GCS publisher should return InvalidArgument.
  const auto subscriber_id = UniqueID::FromRandom();
  const auto key_id = ObjectID::FromRandom();

  rpc::GcsSubscriberCommandBatchRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  auto *command = request.add_commands();
  command->set_channel_type(rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL);
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

TEST_F(PubSubHandlerTest, HandleReportJobErrorPublishesToSubscribedErrorChannel) {
  const auto subscriber_id = UniqueID::FromRandom();
  const JobID job_id = JobID::FromInt(42);

  rpc::GcsSubscriberCommandBatchRequest sub_req;
  sub_req.set_subscriber_id(subscriber_id.Binary());
  auto *cmd = sub_req.add_commands();
  cmd->set_channel_type(rpc::ChannelType::RAY_ERROR_INFO_CHANNEL);
  cmd->mutable_subscribe_message();

  rpc::GcsSubscriberCommandBatchReply sub_reply;
  Status sub_status;
  observability_pubsub_handler_->HandleGcsSubscriberCommandBatch(
      sub_req,
      &sub_reply,
      [&sub_status](const Status &status, std::function<void()>, std::function<void()>) {
        sub_status = status;
      });
  ASSERT_TRUE(sub_status.ok()) << sub_status;

  rpc::ReportJobErrorRequest err_req;
  err_req.mutable_job_error()->set_job_id(job_id.Binary());
  err_req.mutable_job_error()->set_error_message("test job error");

  rpc::ReportJobErrorReply err_reply;
  Status err_status;
  observability_pubsub_handler_->HandleReportJobError(
      err_req,
      &err_reply,
      [&err_status](const Status &status, std::function<void()>, std::function<void()>) {
        err_status = status;
      });
  ASSERT_TRUE(err_status.ok()) << err_status;
  EXPECT_EQ(StatusCode(err_reply.status().code()), StatusCode::OK);

  rpc::GcsSubscriberPollRequest poll_req;
  poll_req.set_subscriber_id(subscriber_id.Binary());
  poll_req.set_max_processed_sequence_id(0);
  rpc::GcsSubscriberPollReply poll_reply;
  Status poll_status;
  observability_pubsub_handler_->HandleGcsSubscriberPoll(
      poll_req,
      &poll_reply,
      [&poll_status](const Status &status, std::function<void()>, std::function<void()>) {
        poll_status = status;
      });
  ASSERT_TRUE(poll_status.ok()) << poll_status;
  ASSERT_EQ(poll_reply.pub_messages_size(), 1);
  const auto &msg = poll_reply.pub_messages(0);
  EXPECT_EQ(msg.channel_type(), rpc::ChannelType::RAY_ERROR_INFO_CHANNEL);
  EXPECT_EQ(msg.key_id(), job_id.Hex());
  EXPECT_EQ(msg.error_info_message().error_message(), "test job error");
}

TEST_F(PubSubHandlerTest, HandleReportJobErrorRejectsInvalidJobIdLength) {
  rpc::ReportJobErrorRequest err_req;
  err_req.mutable_job_error()->set_job_id("");
  err_req.mutable_job_error()->set_error_message("x");

  rpc::ReportJobErrorReply err_reply;
  Status err_status;
  observability_pubsub_handler_->HandleReportJobError(
      err_req,
      &err_reply,
      [&err_status](const Status &status, std::function<void()>, std::function<void()>) {
        err_status = status;
      });
  ASSERT_TRUE(err_status.IsInvalidArgument()) << err_status;

  err_req.mutable_job_error()->set_job_id("short");
  observability_pubsub_handler_->HandleReportJobError(
      err_req,
      &err_reply,
      [&err_status](const Status &status, std::function<void()>, std::function<void()>) {
        err_status = status;
      });
  ASSERT_TRUE(err_status.IsInvalidArgument()) << err_status;
}

}  // namespace gcs
}  // namespace ray
