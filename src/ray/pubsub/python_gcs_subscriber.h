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

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/status.h"
#include "ray/util/visibility.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

// Use forward declarations to avoid exposing heavyweight gRPC headers.
namespace grpc {

class Channel;
class ClientContext;

}  // namespace grpc

namespace ray {
namespace pubsub {

// This client is only supposed to be used from Cython / Python
class RAY_EXPORT PythonGcsSubscriber {
 public:
  PythonGcsSubscriber(const std::string &gcs_address,
                      int gcs_port,
                      rpc::ChannelType channel_type,
                      std::string subscriber_id,
                      std::string worker_id);

  /// Register a subscription for the subscriber's channel type.
  ///
  /// Before the registration, published messages in the channel
  /// will not be saved for the subscriber.
  Status Subscribe();

  /// Polls for new error message.
  /// Both key_id and data are out parameters.
  Status PollError(std::string *key_id, int64_t timeout_ms, rpc::ErrorTableData *data);

  /// Polls for new log messages.
  Status PollLogs(std::string *key_id, int64_t timeout_ms, rpc::LogBatch *data);

  /// Closes the subscriber and its active subscription.
  Status Close();

  int64_t last_batch_size();

 private:
  Status DoPoll(int64_t timeout_ms, rpc::PubMessage *message);

  mutable absl::Mutex mu_;

  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<rpc::InternalPubSubGcsService::Stub> pubsub_stub_;

  const rpc::ChannelType channel_type_;
  const std::string subscriber_id_;
  std::string publisher_id_;
  const std::string worker_id_;
  int64_t max_processed_sequence_id_ ABSL_GUARDED_BY(mu_) = 0;
  int64_t last_batch_size_ ABSL_GUARDED_BY(mu_) = 0;
  std::deque<rpc::PubMessage> queue_ ABSL_GUARDED_BY(mu_);
  bool closed_ ABSL_GUARDED_BY(mu_) = false;
  std::shared_ptr<grpc::ClientContext> current_polling_context_ ABSL_GUARDED_BY(mu_);

  // Set authentication token on a gRPC client context if token-based authentication is
  // enabled
  void SetAuthenticationToken(grpc::ClientContext &context);
};

/// Get the .lines() attribute of a LogBatch as a std::vector
/// (this is needed so it can be wrapped in Cython)
std::vector<std::string> PythonGetLogBatchLines(rpc::LogBatch log_batch);

}  // namespace pubsub
}  // namespace ray
