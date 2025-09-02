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

#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/gcs_callbacks.h"
#include "ray/pubsub/publisher_interface.h"
#include "ray/pubsub/subscriber_interface.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace gcs {

/// \class GcsPublisher
///
/// Supports publishing per-entity data and errors from GCS. Thread safe.
class GcsPublisher {
 public:
  /// Initializes GcsPublisher with GCS based publishers.
  /// Publish*() member functions below would be incrementally converted to use the GCS
  /// based publisher, if available.
  explicit GcsPublisher(std::unique_ptr<pubsub::PublisherInterface> publisher)
      : publisher_(std::move(publisher)) {
    RAY_CHECK(publisher_);
  }

  /// Returns the underlying pubsub::Publisher. Caller does not take ownership.
  pubsub::PublisherInterface &GetPublisher() const { return *publisher_; }

  /// Each publishing method below publishes to a different "channel".
  /// ID is the entity which the message is associated with, e.g. ActorID for Actor data.
  /// Subscribers receive typed messages for the ID that they subscribe to.
  ///
  /// The full stream of NodeResource and Error channels are needed by its subscribers.
  /// But for other channels, subscribers should only need the latest data.
  ///
  /// TODO: Verify GCS pubsub satisfies the streaming semantics.
  /// TODO: Implement optimization for channels where only latest data per ID is useful.

  void PublishActor(const ActorID &id, rpc::ActorTableData message);

  void PublishJob(const JobID &id, rpc::JobTableData message);

  void PublishNodeInfo(const NodeID &id, rpc::GcsNodeInfo message);

  /// Actually rpc::WorkerDeltaData is not a delta message.
  void PublishWorkerFailure(const WorkerID &id, rpc::WorkerDeltaData message);

  void PublishError(std::string id, rpc::ErrorTableData message);

  /// Prints debugging info for the publisher.
  std::string DebugString() const;

 private:
  const std::unique_ptr<pubsub::PublisherInterface> publisher_;
};

/// \class GcsSubscriber
///
/// Supports subscribing to an entity or a channel from GCS. Thread safe.
class GcsSubscriber {
 public:
  /// Initializes GcsSubscriber with GCS based GcsSubscribers.
  // TODO(mwtian): Support restarted GCS publisher, at the same or a different address.
  GcsSubscriber(rpc::Address gcs_address,
                std::unique_ptr<pubsub::SubscriberInterface> subscriber)
      : gcs_address_(std::move(gcs_address)), subscriber_(std::move(subscriber)) {}

  /// Subscribe*() member functions below would be incrementally converted to use the GCS
  /// based subscriber, if available.
  /// The `subscribe` callbacks must not be empty. The `done` callbacks can optionally be
  /// empty.

  /// Uses GCS pubsub when created with `subscriber`.
  Status SubscribeActor(const ActorID &id,
                        const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                        const StatusCallback &done);
  Status UnsubscribeActor(const ActorID &id);

  bool IsActorUnsubscribed(const ActorID &id);

  Status SubscribeAllJobs(const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
                          const StatusCallback &done);

  void SubscribeAllNodeInfo(const ItemCallback<rpc::GcsNodeInfo> &subscribe,
                            const StatusCallback &done);

  Status SubscribeAllWorkerFailures(const ItemCallback<rpc::WorkerDeltaData> &subscribe,
                                    const StatusCallback &done);

  /// Prints debugging info for the subscriber.
  std::string DebugString() const;

 private:
  const rpc::Address gcs_address_;
  const std::unique_ptr<pubsub::SubscriberInterface> subscriber_;
};

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
};

/// Get the .lines() attribute of a LogBatch as a std::vector
/// (this is needed so it can be wrapped in Cython)
std::vector<std::string> PythonGetLogBatchLines(rpc::LogBatch log_batch);

}  // namespace gcs
}  // namespace ray
