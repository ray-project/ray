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
#include <memory>
#include "ray/common/asio/periodical_runner.h"
#include "ray/pubsub/publisher.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// This implementation class of `PublisherHandler`
class GcsPublisherManager : public rpc::PublisherHandler {
 public:
  explicit GcsPublisherManager(instrumented_io_context &service);

  ~GcsPublisherManager();

  /// Start and stop the io service's thread.
  void Start();
  void Stop();

  void Publish(const rpc::ChannelType channel_type, const rpc::PubMessage pub_message,
               const std::string key_id_binary);

  void HandlePubsubLongPolling(const rpc::PubsubLongPollingRequest &request,
                               rpc::PubsubLongPollingReply *reply,
                               rpc::SendReplyCallback callback);
  void HandlePubsubCommandBatch(const rpc::PubsubCommandBatchRequest &request,
                                rpc::PubsubCommandBatchReply *reply,
                                rpc::SendReplyCallback callback);

 private:
  /// The thread + io service the publisher runs on.
  std::unique_ptr<std::thread> publisher_thread_;
  instrumented_io_context &publisher_service_;
  PeriodicalRunner periodical_runner_;
  /// Grpc based pubsub.
  pubsub::Publisher grpc_publisher_;
};

}  // namespace gcs
}  // namespace ray
