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

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/pubsub/gcs_publisher.h"

namespace ray {
namespace gcs {

/// This is the implementation class of `InternalPubsubHandler`.
/// It supports subscribing updates from GCS with long poll, and registering /
/// de-registering subscribers.
class InternalPubSubHandler : public rpc::InternalPubSubGcsServiceHandler {
 public:
  InternalPubSubHandler(instrumented_io_context &io_service,
                        pubsub::GcsPublisher &gcs_publisher);

  void HandleGcsPublish(rpc::GcsPublishRequest request,
                        rpc::GcsPublishReply *reply,
                        rpc::SendReplyCallback send_reply_callback) final;

  void HandleGcsSubscriberPoll(rpc::GcsSubscriberPollRequest request,
                               rpc::GcsSubscriberPollReply *reply,
                               rpc::SendReplyCallback send_reply_callback) final;

  void HandleGcsSubscriberCommandBatch(rpc::GcsSubscriberCommandBatchRequest request,
                                       rpc::GcsSubscriberCommandBatchReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) final;

  /// This function is only for external callers. Internally, can just erase from
  /// sender_to_subscribers_ and everything should be on the Publisher's io_service_.
  void AsyncRemoveSubscriberFrom(const std::string &sender_id);

 private:
  /// Not owning the io service, to allow sharing it with pubsub::Publisher.
  instrumented_io_context &io_service_;
  pubsub::GcsPublisher &gcs_publisher_;
  absl::flat_hash_map<std::string, absl::flat_hash_set<UniqueID>> sender_to_subscribers_;
};

}  // namespace gcs
}  // namespace ray
