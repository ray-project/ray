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
#include "ray/pubsub/publisher_interface.h"

namespace ray {
namespace gcs {

/// Shared implementation for Internal and Observability GCS pubsub handlers
/// (publish, subscriber poll, subscribe/unsubscribe batch).
class PubSubHandlerBase {
 public:
  PubSubHandlerBase(instrumented_io_context &io_service,
                    pubsub::PublisherInterface &publisher);

  virtual ~PubSubHandlerBase() = default;

  /// For external callers when a sender disconnects; work runs on the publisher io
  /// context.
  void AsyncRemoveSubscriberFrom(const std::string &sender_id);

 protected:
  void HandleGcsPublish(rpc::GcsPublishRequest request,
                        rpc::GcsPublishReply *reply,
                        rpc::SendReplyCallback send_reply_callback);

  void HandleGcsSubscriberPoll(rpc::GcsSubscriberPollRequest request,
                               rpc::GcsSubscriberPollReply *reply,
                               rpc::SendReplyCallback send_reply_callback);

  void HandleGcsSubscriberCommandBatch(rpc::GcsSubscriberCommandBatchRequest request,
                                       rpc::GcsSubscriberCommandBatchReply *reply,
                                       rpc::SendReplyCallback send_reply_callback);

  instrumented_io_context &io_service_;
  pubsub::PublisherInterface &publisher_;
  absl::flat_hash_map<std::string, absl::flat_hash_set<UniqueID>> sender_to_subscribers_;
};

/// Implementation of `InternalPubSubGcsServiceHandler`: long-poll subscribe and
/// register / unregister subscribers against a `GcsPublisher`.
class InternalPubSubHandler : public PubSubHandlerBase,
                              public rpc::InternalPubSubGcsServiceHandler {
 public:
  InternalPubSubHandler(instrumented_io_context &io_service,
                        pubsub::GcsPublisher &gcs_publisher);

  using PubSubHandlerBase::AsyncRemoveSubscriberFrom;

  void HandleGcsPublish(rpc::GcsPublishRequest request,
                        rpc::GcsPublishReply *reply,
                        rpc::SendReplyCallback send_reply_callback) final {
    PubSubHandlerBase::HandleGcsPublish(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGcsSubscriberPoll(rpc::GcsSubscriberPollRequest request,
                               rpc::GcsSubscriberPollReply *reply,
                               rpc::SendReplyCallback send_reply_callback) final {
    PubSubHandlerBase::HandleGcsSubscriberPoll(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGcsSubscriberCommandBatch(rpc::GcsSubscriberCommandBatchRequest request,
                                       rpc::GcsSubscriberCommandBatchReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) final {
    PubSubHandlerBase::HandleGcsSubscriberCommandBatch(
        std::move(request), reply, std::move(send_reply_callback));
  }
};

/// Observability pubsub (`ObservabilityPubSubGcsService`): same publish/subscriber
/// behavior as internal pubsub, plus `ReportJobError`, on an `ObservabilityPublisher`.
class ObservabilityPubSubHandler : public PubSubHandlerBase,
                                   public rpc::ObservabilityPubSubGcsServiceHandler {
 public:
  ObservabilityPubSubHandler(instrumented_io_context &io_service,
                             pubsub::ObservabilityPublisher &observability_publisher);

  using PubSubHandlerBase::AsyncRemoveSubscriberFrom;

  void HandleGcsPublish(rpc::GcsPublishRequest request,
                        rpc::GcsPublishReply *reply,
                        rpc::SendReplyCallback send_reply_callback) final {
    PubSubHandlerBase::HandleGcsPublish(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleReportJobError(rpc::ReportJobErrorRequest request,
                            rpc::ReportJobErrorReply *reply,
                            rpc::SendReplyCallback send_reply_callback) final;

  void HandleGcsSubscriberPoll(rpc::GcsSubscriberPollRequest request,
                               rpc::GcsSubscriberPollReply *reply,
                               rpc::SendReplyCallback send_reply_callback) final {
    PubSubHandlerBase::HandleGcsSubscriberPoll(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGcsSubscriberCommandBatch(rpc::GcsSubscriberCommandBatchRequest request,
                                       rpc::GcsSubscriberCommandBatchReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) final {
    PubSubHandlerBase::HandleGcsSubscriberCommandBatch(
        std::move(request), reply, std::move(send_reply_callback));
  }

 private:
  pubsub::ObservabilityPublisher &observability_publisher_;
};

}  // namespace gcs
}  // namespace ray
