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

#include <optional>
#include <string>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/callback.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

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
  GcsPublisher(std::unique_ptr<pubsub::Publisher> publisher)
      : publisher_(std::move(publisher)) {}

  /// Test only.
  /// TODO: remove this constructor and inject mock / fake from the other constructor.
  explicit GcsPublisher() {}

  virtual ~GcsPublisher() = default;

  /// Returns the underlying pubsub::Publisher. Caller does not take ownership.
  /// Returns nullptr when RayConfig::instance().gcs_grpc_based_pubsub() is false.
  pubsub::Publisher *GetPublisher() const { return publisher_.get(); }

  /// Each publishing method below publishes to a different "channel".
  /// ID is the entity which the message is associated with, e.g. ActorID for Actor data.
  /// Subscribers receive typed messages for the ID that they subscribe to.
  ///
  /// The full stream of NodeResource and Error channels are needed by its subscribers.
  /// But for other channels, subscribers should only need the latest data.
  ///
  /// TODO: Verify GCS pubsub satisfies the streaming semantics.
  /// TODO: Implement optimization for channels where only latest data per ID is useful.

  Status PublishActor(const ActorID &id,
                      const rpc::ActorTableData &message,
                      const StatusCallback &done);

  Status PublishJob(const JobID &id,
                    const rpc::JobTableData &message,
                    const StatusCallback &done);

  virtual Status PublishNodeInfo(const NodeID &id,
                                 const rpc::GcsNodeInfo &message,
                                 const StatusCallback &done);

  /// Actually rpc::WorkerDeltaData is not a delta message.
  Status PublishWorkerFailure(const WorkerID &id,
                              const rpc::WorkerDeltaData &message,
                              const StatusCallback &done);

  virtual Status PublishError(const std::string &id,
                              const rpc::ErrorTableData &message,
                              const StatusCallback &done);

  /// TODO: remove once it is converted to GRPC-based push broadcasting.
  Status PublishResourceBatch(const rpc::ResourceUsageBatchData &message,
                              const StatusCallback &done);

  /// Prints debugging info for the publisher.
  std::string DebugString() const;

 private:
  const std::unique_ptr<pubsub::Publisher> publisher_;
};

/// \class GcsSubscriber
///
/// Supports subscribing to an entity or a channel from GCS. Thread safe.
class GcsSubscriber {
 public:
  /// Initializes GcsSubscriber with GCS based GcsSubscribers.
  // TODO: Support restarted GCS publisher, at the same or a different address.
  GcsSubscriber(const rpc::Address &gcs_address,
                std::unique_ptr<pubsub::Subscriber> subscriber)
      : gcs_address_(gcs_address), subscriber_(std::move(subscriber)) {}

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

  Status SubscribeAllNodeInfo(const ItemCallback<rpc::GcsNodeInfo> &subscribe,
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
class RAY_EXPORT PythonGcsPublisher {
 public:
  explicit PythonGcsPublisher(const std::string &gcs_address);

  /// Connect to the publisher service of the GCS.
  /// This function must be called before calling other functions.
  ///
  /// \return Status
  Status Connect();

  /// Publish error information to GCS.
  Status PublishError(const std::string &key_id,
                      const rpc::ErrorTableData &data,
                      int64_t num_retries);

  /// Publish logs to GCS.
  Status PublishLogs(const std::string &key_id, const rpc::LogBatch &log_batch);

 private:
  Status DoPublishWithRetries(const rpc::GcsPublishRequest &request,
                              int64_t num_retries,
                              int64_t timeout_ms);
  std::unique_ptr<rpc::InternalPubSubGcsService::Stub> pubsub_stub_;
  std::shared_ptr<grpc::Channel> channel_;
  std::string gcs_address_;
  int gcs_port_;
};

// This client is only supposed to be used from Cython / Python
class RAY_EXPORT PythonGcsSubscriber {
 public:
  explicit PythonGcsSubscriber(const std::string &gcs_address,
                               int gcs_port,
                               rpc::ChannelType channel_type,
                               const std::string &subscriber_id,
                               const std::string &worker_id);

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

  /// Polls for actor messages.
  Status PollActor(std::string *key_id, int64_t timeout_ms, rpc::ActorTableData *data);

  /// Closes the subscriber and its active subscription.
  Status Close();

  int64_t last_batch_size();

 private:
  Status DoPoll(int64_t timeout_ms, rpc::PubMessage *message);

  mutable absl::Mutex mu_;

  std::unique_ptr<rpc::InternalPubSubGcsService::Stub> pubsub_stub_;
  std::shared_ptr<grpc::Channel> channel_;
  const rpc::ChannelType channel_type_;
  const std::string subscriber_id_;
  std::string publisher_id_;
  const std::string worker_id_;
  int64_t max_processed_sequence_id_ ABSL_GUARDED_BY(mu_);
  int64_t last_batch_size_ ABSL_GUARDED_BY(mu_);
  std::deque<rpc::PubMessage> queue_ ABSL_GUARDED_BY(mu_);
  bool closed_ ABSL_GUARDED_BY(mu_);
  std::shared_ptr<grpc::ClientContext> current_polling_context_ ABSL_GUARDED_BY(mu_);
};

/// Get the .lines() attribute of a LogBatch as a std::vector
/// (this is needed so it can be wrapped in Cython)
std::vector<std::string> PythonGetLogBatchLines(const rpc::LogBatch &log_batch);

}  // namespace gcs
}  // namespace ray
