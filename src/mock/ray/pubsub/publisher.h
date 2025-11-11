// Copyright 2021 The Ray Authors.
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

#include "gmock/gmock.h"
#include "ray/pubsub/publisher_interface.h"

namespace ray {
namespace pubsub {

class MockPublisher : public PublisherInterface {
 public:
  MOCK_METHOD(void,
              ConnectToSubscriber,
              (const rpc::PubsubLongPollingRequest &request,
               std::string *publisher_id,
               google::protobuf::RepeatedPtrField<rpc::PubMessage> *pub_messages,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              RegisterSubscription,
              (const rpc::ChannelType channel_type,
               const UniqueID &subscriber_id,
               const std::optional<std::string> &key_id),
              (override));
  MOCK_METHOD(void, Publish, (rpc::PubMessage pub_message), (override));
  MOCK_METHOD(void,
              PublishFailure,
              (const rpc::ChannelType channel_type, const std::string &key_id),
              (override));
  MOCK_METHOD(void,
              UnregisterSubscription,
              (const rpc::ChannelType channel_type,
               const UniqueID &subscriber_id,
               const std::optional<std::string> &key_id),
              (override));
  MOCK_METHOD(void, UnregisterSubscriber, (const UniqueID &subscriber_id), (override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));
};

}  // namespace pubsub
}  // namespace ray
