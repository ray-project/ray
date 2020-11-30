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

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "ray/common/constants.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/entry_change_notification.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

struct redisAsyncContext;

namespace ray {

namespace gcs {

using rpc::ActorCheckpointData;
using rpc::ActorCheckpointIdData;
using rpc::ActorTableData;
using rpc::ErrorTableData;
using rpc::GcsChangeMode;
using rpc::GcsEntry;
using rpc::GcsNodeInfo;
using rpc::HeartbeatBatchTableData;
using rpc::HeartbeatTableData;
using rpc::JobTableData;
using rpc::ObjectTableData;
using rpc::ProfileTableData;
using rpc::ResourceTableData;
using rpc::TablePrefix;
using rpc::TablePubsub;
using rpc::TaskLeaseData;
using rpc::TaskReconstructionData;
using rpc::TaskTableData;
using rpc::WorkerTableData;

class RedisContext;

class RedisGcsClient;

/// Specifies whether commands issued to a table should be regular or chain-replicated
/// (when available).
enum class CommandType { kRegular, kChain, kUnknown };

/// \class PubsubInterface
///
/// The interface for a pubsub storage system. The client of a storage system
/// that implements this interface can request and cancel notifications for
/// specific keys.
template <typename ID>
class PubsubInterface {
 public:
  virtual Status RequestNotifications(const JobID &job_id, const ID &id,
                                      const NodeID &node_id,
                                      const StatusCallback &done) = 0;
  virtual Status CancelNotifications(const JobID &job_id, const ID &id,
                                     const NodeID &node_id,
                                     const StatusCallback &done) = 0;
  virtual ~PubsubInterface(){};
};

template <typename ID, typename Data>
class LogInterface {
 public:
  using WriteCallback =
      std::function<void(RedisGcsClient *client, const ID &id, const Data &data)>;
  virtual Status Append(const JobID &job_id, const ID &id,
                        const std::shared_ptr<Data> &data, const WriteCallback &done) = 0;
  virtual Status AppendAt(const JobID &job_id, const ID &id,
                          const std::shared_ptr<Data> &data, const WriteCallback &done,
                          const WriteCallback &failure, int log_length) = 0;
  virtual ~LogInterface(){};
};

/// \class Log
///
/// A GCS table where every entry is an append-only log. This class is not
/// meant to be used directly. All log classes should derive from this class
/// and override the prefix_ member with a unique prefix for that log, and the
/// pubsub_channel_ member if pubsub is required.
///
/// Example tables backed by Log:
///   NodeTable: Stores a log of which GCS clients have been added or deleted
///                from the system.
template <typename ID, typename Data>
class Log : public LogInterface<ID, Data>, virtual public PubsubInterface<ID> {
 public:
  using Callback = std::function<void(RedisGcsClient *client, const ID &id,
                                      const std::vector<Data> &data)>;

  using NotificationCallback =
      std::function<void(RedisGcsClient *client, const ID &id,
                         const GcsChangeMode change_mode, const std::vector<Data> &data)>;

  /// The callback to call when a write to a key succeeds.
  using WriteCallback = typename LogInterface<ID, Data>::WriteCallback;
  /// The callback to call when a SUBSCRIBE call completes and we are ready to
  /// request and receive notifications.
  using SubscriptionCallback = std::function<void(RedisGcsClient *client)>;

  struct CallbackData {
    ID id;
    std::shared_ptr<Data> data;
    Callback callback;
    // An optional callback to call for subscription operations, where the
    // first message is a notification of subscription success.
    SubscriptionCallback subscription_callback;
    Log<ID, Data> *log;
    RedisGcsClient *client;
  };

  Log(const std::vector<std::shared_ptr<RedisContext>> &contexts, RedisGcsClient *client)
      : shard_contexts_(contexts),
        client_(client),
        pubsub_channel_(TablePubsub::NO_PUBLISH),
        prefix_(TablePrefix::UNUSED),
        subscribe_callback_index_(-1){};

  /// Append a log entry to a key.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data to append to the log. TODO(rkn): This can be made const,
  /// right?
  /// \param done Callback that is called once the data has been written to the
  /// GCS.
  /// \return Status
  Status Append(const JobID &job_id, const ID &id, const std::shared_ptr<Data> &data,
                const WriteCallback &done);

  /// Append a log entry to a key synchronously.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data to append to the log.
  /// \return Status
  Status SyncAppend(const JobID &job_id, const ID &id, const std::shared_ptr<Data> &data);

  /// Append a log entry to a key if and only if the log has the given number
  /// of entries.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data to append to the log.
  /// \param done Callback that is called if the data was appended to the log.
  /// \param failure Callback that is called if the data was not appended to
  /// the log because the log length did not match the given `log_length`.
  /// \param log_length The number of entries that the log must have for the
  /// append to succeed.
  /// \return Status
  Status AppendAt(const JobID &job_id, const ID &id, const std::shared_ptr<Data> &data,
                  const WriteCallback &done, const WriteCallback &failure,
                  int log_length);

  /// Lookup the log values at a key asynchronously.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is looked up in the GCS.
  /// \param lookup Callback that is called after lookup. If the callback is
  /// called with an empty vector, then there was no data at the key.
  /// \return Status
  Status Lookup(const JobID &job_id, const ID &id, const Callback &lookup);

  /// Subscribe to any Append operations to this table. The caller may choose
  /// requests notifications for. This may only be called once per Log
  ///
  /// \param job_id The ID of the job.
  /// \param node_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given node will be received. In the latter
  /// case, the node may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data at the key.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status Subscribe(const JobID &job_id, const NodeID &node_id, const Callback &subscribe,
                   const SubscriptionCallback &done);

  /// Request notifications about a key in this table.
  ///
  /// The notifications will be returned via the subscribe callback that was
  /// registered by `Subscribe`.  An initial notification will be returned for
  /// the current values at the key, if any, and a subsequent notification will
  /// be published for every following `Append` to the key. Before
  /// notifications can be requested, the caller must first call `Subscribe`,
  /// with the same `node_id`.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the key to request notifications for.
  /// \param node_id The node who is requesting notifications.
  /// \param done Callback that is called when request notifications is complete.
  /// notifications can be requested, a call to `Subscribe` to this
  /// table with the same `node_id` must complete successfully.
  /// \return Status
  Status RequestNotifications(const JobID &job_id, const ID &id, const NodeID &node_id,
                              const StatusCallback &done);

  /// Cancel notifications about a key in this table.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the key to request notifications for.
  /// \param node_id The node who originally requested notifications.
  /// \param done Callback that is called when cancel notifications is complete.
  /// \return Status
  Status CancelNotifications(const JobID &job_id, const ID &id, const NodeID &node_id,
                             const StatusCallback &done);

  /// Subscribe to any modifications to the key. The caller may choose
  /// to subscribe to all modifications, or to subscribe only to keys that it
  /// requests notifications for. This may only be called once per Log
  /// instance. This function is different from public version due to
  /// an additional parameter change_mode in NotificationCallback. Therefore this
  /// function supports notifications of remove operations.
  ///
  /// \param job_id The ID of the job.
  /// \param node_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given node will be received. In the latter
  /// case, the node may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data at the key.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status Subscribe(const JobID &job_id, const NodeID &node_id,
                   const NotificationCallback &subscribe,
                   const SubscriptionCallback &done);

  /// Delete an entire key from redis.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data to delete from the GCS.
  /// \return Void.
  void Delete(const JobID &job_id, const ID &id);

  /// Delete several keys from redis.
  ///
  /// \param job_id The ID of the job.
  /// \param ids The vector of IDs to delete from the GCS.
  /// \return Void.
  void Delete(const JobID &job_id, const std::vector<ID> &ids);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 protected:
  std::shared_ptr<RedisContext> GetRedisContext(const ID &id) {
    static std::hash<ID> index;
    return shard_contexts_[index(id) % shard_contexts_.size()];
  }

  /// The connection to the GCS.
  std::vector<std::shared_ptr<RedisContext>> shard_contexts_;
  /// The GCS client.
  RedisGcsClient *client_;
  /// The pubsub channel to subscribe to for notifications about keys in this
  /// table. If no notifications are required, this should be set to
  /// TablePubsub_NO_PUBLISH. If notifications are required, then this must be
  /// unique across all instances of Log.
  TablePubsub pubsub_channel_;
  /// The prefix to use for keys in this table. This must be unique across all
  /// instances of Log.
  TablePrefix prefix_;
  /// The index in the RedisCallbackManager for the callback that is called
  /// when we receive notifications. This is >= 0 iff we have subscribed to the
  /// table, otherwise -1.
  int64_t subscribe_callback_index_;

  /// Commands to a GCS table can either be regular (default) or chain-replicated.
  CommandType command_type_ = CommandType::kRegular;

  int64_t num_appends_ = 0;
  int64_t num_lookups_ = 0;
};

template <typename ID, typename Data>
class TableInterface {
 public:
  using WriteCallback = typename Log<ID, Data>::WriteCallback;
  virtual Status Add(const JobID &job_id, const ID &task_id,
                     const std::shared_ptr<Data> &data, const WriteCallback &done) = 0;
  virtual ~TableInterface(){};
};

/// \class Table
///
/// A GCS table where every entry is a single data item. This class is not
/// meant to be used directly. All table classes should derive from this class
/// and override the prefix_ member with a unique prefix for that table, and
/// the pubsub_channel_ member if pubsub is required.
///
/// Example tables backed by Log:
///   TaskTable: Stores Task metadata needed for executing the task.
template <typename ID, typename Data>
class Table : private Log<ID, Data>,
              public TableInterface<ID, Data>,
              virtual public PubsubInterface<ID> {
 public:
  using Callback =
      std::function<void(RedisGcsClient *client, const ID &id, const Data &data)>;
  using WriteCallback = typename Log<ID, Data>::WriteCallback;
  /// The callback to call when a Lookup call returns an empty entry.
  using FailureCallback = std::function<void(RedisGcsClient *client, const ID &id)>;
  /// The callback to call when a Subscribe call completes and we are ready to
  /// request and receive notifications.
  using SubscriptionCallback = typename Log<ID, Data>::SubscriptionCallback;

  Table(const std::vector<std::shared_ptr<RedisContext>> &contexts,
        RedisGcsClient *client)
      : Log<ID, Data>(contexts, client) {}

  using Log<ID, Data>::RequestNotifications;
  using Log<ID, Data>::CancelNotifications;
  /// Expose this interface for use by subscription tools class SubscriptionExecutor.
  /// In this way TaskTable() can also reuse class SubscriptionExecutor.
  using Log<ID, Data>::Subscribe;

  /// Add an entry to the table. This overwrites any existing data at the key.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data that is added to the GCS.
  /// \param done Callback that is called once the data has been written to the
  /// GCS.
  /// \return Status
  Status Add(const JobID &job_id, const ID &id, const std::shared_ptr<Data> &data,
             const WriteCallback &done);

  /// Lookup an entry asynchronously.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is looked up in the GCS.
  /// \param lookup Callback that is called after lookup if there was data the
  /// key.
  /// \param failure Callback that is called after lookup if there was no data
  /// at the key.
  /// \return Status
  Status Lookup(const JobID &job_id, const ID &id, const Callback &lookup,
                const FailureCallback &failure);

  /// Subscribe to any Add operations to this table. The caller may choose to
  /// subscribe to all Adds, or to subscribe only to keys that it requests
  /// notifications for. This may only be called once per Table instance.
  ///
  /// \param job_id The ID of the job.
  /// \param node_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given node will be received. In the latter
  /// case, the node may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data at the key.
  /// \param failure Callback that is called if the key is empty at the time
  /// that notifications are requested.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status Subscribe(const JobID &job_id, const NodeID &node_id, const Callback &subscribe,
                   const FailureCallback &failure, const SubscriptionCallback &done);

  /// Subscribe to any Add operations to this table. The caller may choose to
  /// subscribe to all Adds, or to subscribe only to keys that it requests
  /// notifications for. This may only be called once per Table instance.
  ///
  /// \param job_id The ID of the job.
  /// \param node_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given node will be received. In the latter
  /// case, the node may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data at the key.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status Subscribe(const JobID &job_id, const NodeID &node_id, const Callback &subscribe,
                   const SubscriptionCallback &done);

  void Delete(const JobID &job_id, const ID &id) { Log<ID, Data>::Delete(job_id, id); }

  void Delete(const JobID &job_id, const std::vector<ID> &ids) {
    Log<ID, Data>::Delete(job_id, ids);
  }

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 protected:
  using Log<ID, Data>::shard_contexts_;
  using Log<ID, Data>::client_;
  using Log<ID, Data>::pubsub_channel_;
  using Log<ID, Data>::prefix_;
  using Log<ID, Data>::command_type_;
  using Log<ID, Data>::GetRedisContext;

  int64_t num_adds_ = 0;
  int64_t num_lookups_ = 0;
};

template <typename ID, typename Data>
class SetInterface {
 public:
  using WriteCallback = typename Log<ID, Data>::WriteCallback;
  virtual Status Add(const JobID &job_id, const ID &id, const std::shared_ptr<Data> &data,
                     const WriteCallback &done) = 0;
  virtual Status Remove(const JobID &job_id, const ID &id,
                        const std::shared_ptr<Data> &data, const WriteCallback &done) = 0;
  virtual ~SetInterface(){};
};

/// \class Set
///
/// A GCS table where every entry is an addable & removable set. This class is not
/// meant to be used directly. All set classes should derive from this class
/// and override the prefix_ member with a unique prefix for that set, and the
/// pubsub_channel_ member if pubsub is required.
///
/// Example tables backed by Set:
///   ObjectTable: Stores a set of which clients have added an object.
template <typename ID, typename Data>
class Set : private Log<ID, Data>,
            public SetInterface<ID, Data>,
            virtual public PubsubInterface<ID> {
 public:
  using Callback = typename Log<ID, Data>::Callback;
  using WriteCallback = typename Log<ID, Data>::WriteCallback;
  using SubscriptionCallback = typename Log<ID, Data>::SubscriptionCallback;

  Set(const std::vector<std::shared_ptr<RedisContext>> &contexts, RedisGcsClient *client)
      : Log<ID, Data>(contexts, client) {}

  using Log<ID, Data>::RequestNotifications;
  using Log<ID, Data>::CancelNotifications;
  using Log<ID, Data>::Lookup;
  using Log<ID, Data>::Delete;

  /// Add an entry to the set.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data to add to the set.
  /// \param done Callback that is called once the data has been written to the
  /// GCS.
  /// \return Status
  Status Add(const JobID &job_id, const ID &id, const std::shared_ptr<Data> &data,
             const WriteCallback &done);

  /// Remove an entry from the set.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is removed from the GCS.
  /// \param data Data to remove from the set.
  /// \param done Callback that is called once the data has been written to the
  /// GCS.
  /// \return Status
  Status Remove(const JobID &job_id, const ID &id, const std::shared_ptr<Data> &data,
                const WriteCallback &done);

  using NotificationCallback =
      std::function<void(RedisGcsClient *client, const ID &id,
                         const std::vector<ArrayNotification<Data>> &data)>;
  /// Subscribe to any add or remove operations to this table.
  ///
  /// \param job_id The ID of the job.
  /// \param node_id The type of update to listen to. If this is nil, then a
  /// message for each add or remove to the table will be received. Else, only
  /// messages for the given node will be received. In the latter
  /// case, the node may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status Subscribe(const JobID &job_id, const NodeID &node_id,
                   const NotificationCallback &subscribe,
                   const SubscriptionCallback &done);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 protected:
  using Log<ID, Data>::shard_contexts_;
  using Log<ID, Data>::client_;
  using Log<ID, Data>::pubsub_channel_;
  using Log<ID, Data>::prefix_;
  using Log<ID, Data>::GetRedisContext;

  int64_t num_adds_ = 0;
  int64_t num_removes_ = 0;
  using Log<ID, Data>::num_lookups_;
};

template <typename ID, typename Data>
class HashInterface {
 public:
  using DataMap = std::unordered_map<std::string, std::shared_ptr<Data>>;
  // Reuse Log's SubscriptionCallback when Subscribe is successfully called.
  using SubscriptionCallback = typename Log<ID, Data>::SubscriptionCallback;

  /// The callback function used by function Update & Lookup.
  ///
  /// \param client The client on which the RemoveEntries is called.
  /// \param id The ID of the Hash Table whose entries are removed.
  /// \param data Map data contains the change to the Hash Table.
  /// \return Void
  using HashCallback =
      std::function<void(RedisGcsClient *client, const ID &id, const DataMap &pairs)>;

  /// The callback function used by function RemoveEntries.
  ///
  /// \param client The client on which the RemoveEntries is called.
  /// \param id The ID of the Hash Table whose entries are removed.
  /// \param keys The keys that are moved from this Hash Table.
  /// \return Void
  using HashRemoveCallback = std::function<void(RedisGcsClient *client, const ID &id,
                                                const std::vector<std::string> &keys)>;

  /// The notification function used by function Subscribe.
  ///
  /// \param client The client on which the Subscribe is called.
  /// \param change_mode The mode to identify the data is removed or updated.
  /// \param data Map data contains the change to the Hash Table.
  /// \return Void
  using HashNotificationCallback =
      std::function<void(RedisGcsClient *client, const ID &id,
                         const std::vector<MapNotification<std::string, Data>> &data)>;

  /// Add entries of a hash table.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is added to the GCS.
  /// \param pairs Map data to add to the hash table.
  /// \param done HashCallback that is called once the request data has been written to
  /// the GCS.
  /// \return Status
  virtual Status Update(const JobID &job_id, const ID &id, const DataMap &pairs,
                        const HashCallback &done) = 0;

  /// Remove entries from the hash table.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is removed from the GCS.
  /// \param keys The entry keys of the hash table.
  /// \param remove_callback HashRemoveCallback that is called once the data has been
  /// written to the GCS no matter whether the key exists in the hash table.
  /// \return Status
  virtual Status RemoveEntries(const JobID &job_id, const ID &id,
                               const std::vector<std::string> &keys,
                               const HashRemoveCallback &remove_callback) = 0;

  /// Lookup the map data of a hash table.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is looked up in the GCS.
  /// \param lookup HashCallback that is called after lookup. If the callback is
  /// called with an empty hash table, then there was no data in the callback.
  /// \return Status
  virtual Status Lookup(const JobID &job_id, const ID &id,
                        const HashCallback &lookup) = 0;

  /// Subscribe to any Update or Remove operations to this hash table.
  ///
  /// \param job_id The ID of the job.
  /// \param node_id The type of update to listen to. If this is nil, then a
  /// message for each Update to the table will be received. Else, only
  /// messages for the given node will be received. In the latter
  /// case, the node may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe HashNotificationCallback that is called on each received message.
  /// \param done SubscriptionCallback that is called when subscription is complete and
  /// we are ready to receive messages.
  /// \return Status
  virtual Status Subscribe(const JobID &job_id, const NodeID &node_id,
                           const HashNotificationCallback &subscribe,
                           const SubscriptionCallback &done) = 0;

  virtual ~HashInterface(){};
};

template <typename ID, typename Data>
class Hash : private Log<ID, Data>,
             public HashInterface<ID, Data>,
             virtual public PubsubInterface<ID> {
 public:
  using DataMap = std::unordered_map<std::string, std::shared_ptr<Data>>;
  using HashCallback = typename HashInterface<ID, Data>::HashCallback;
  using HashRemoveCallback = typename HashInterface<ID, Data>::HashRemoveCallback;
  using HashNotificationCallback =
      typename HashInterface<ID, Data>::HashNotificationCallback;
  using SubscriptionCallback = typename Log<ID, Data>::SubscriptionCallback;

  Hash(const std::vector<std::shared_ptr<RedisContext>> &contexts, RedisGcsClient *client)
      : Log<ID, Data>(contexts, client) {}

  using Log<ID, Data>::RequestNotifications;
  using Log<ID, Data>::CancelNotifications;

  Status Update(const JobID &job_id, const ID &id, const DataMap &pairs,
                const HashCallback &done) override;

  Status Subscribe(const JobID &job_id, const NodeID &node_id,
                   const HashNotificationCallback &subscribe,
                   const SubscriptionCallback &done) override;

  Status Lookup(const JobID &job_id, const ID &id, const HashCallback &lookup) override;

  Status RemoveEntries(const JobID &job_id, const ID &id,
                       const std::vector<std::string> &keys,
                       const HashRemoveCallback &remove_callback) override;

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 protected:
  using Log<ID, Data>::shard_contexts_;
  using Log<ID, Data>::client_;
  using Log<ID, Data>::pubsub_channel_;
  using Log<ID, Data>::prefix_;
  using Log<ID, Data>::subscribe_callback_index_;
  using Log<ID, Data>::GetRedisContext;

  int64_t num_adds_ = 0;
  int64_t num_removes_ = 0;
  using Log<ID, Data>::num_lookups_;
};

class DynamicResourceTable : public Hash<NodeID, ResourceTableData> {
 public:
  DynamicResourceTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                       RedisGcsClient *client)
      : Hash(contexts, client) {
    pubsub_channel_ = TablePubsub::NODE_RESOURCE_PUBSUB;
    prefix_ = TablePrefix::NODE_RESOURCE;
  };

  virtual ~DynamicResourceTable(){};
};

class ObjectTable : public Set<ObjectID, ObjectTableData> {
 public:
  ObjectTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
              RedisGcsClient *client)
      : Set(contexts, client) {
    pubsub_channel_ = TablePubsub::OBJECT_PUBSUB;
    prefix_ = TablePrefix::OBJECT;
  };

  virtual ~ObjectTable(){};
};

class HeartbeatTable : public Table<NodeID, HeartbeatTableData> {
 public:
  HeartbeatTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                 RedisGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::HEARTBEAT_PUBSUB;
    prefix_ = TablePrefix::HEARTBEAT;
  }
  virtual ~HeartbeatTable() {}
};

class HeartbeatBatchTable : public Table<NodeID, HeartbeatBatchTableData> {
 public:
  HeartbeatBatchTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                      RedisGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::HEARTBEAT_BATCH_PUBSUB;
    prefix_ = TablePrefix::HEARTBEAT_BATCH;
  }
  virtual ~HeartbeatBatchTable() {}
};

class JobTable : public Log<JobID, JobTableData> {
 public:
  JobTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
           RedisGcsClient *client)
      : Log(contexts, client) {
    pubsub_channel_ = TablePubsub::JOB_PUBSUB;
    prefix_ = TablePrefix::JOB;
  };

  virtual ~JobTable() {}
};

/// Log-based Actor table starts with an ALIVE entry, which represents the first time the
/// actor is created. This may be followed by 0 or more pairs of RESTARTING, ALIVE
/// entries, which represent each time the actor fails (RESTARTING) and gets recreated
/// (ALIVE). These may be followed by a DEAD entry, which means that the actor has failed
/// and will not be reconstructed.
class LogBasedActorTable : public Log<ActorID, ActorTableData> {
 public:
  LogBasedActorTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                     RedisGcsClient *client)
      : Log(contexts, client) {
    pubsub_channel_ = TablePubsub::ACTOR_PUBSUB;
    prefix_ = TablePrefix::ACTOR;
  }

  /// Get all actor id synchronously.
  std::vector<ActorID> GetAllActorID();

  /// Get actor table data by actor id synchronously.
  Status Get(const ActorID &actor_id, ActorTableData *actor_table_data);
};

/// Actor table.
/// This table is only used for GCS-based actor management. And when completely migrate to
/// GCS service, the log-based actor table could be removed.
class ActorTable : public Table<ActorID, ActorTableData> {
 public:
  ActorTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
             RedisGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::ACTOR_PUBSUB;
    prefix_ = TablePrefix::ACTOR;
  }

  /// Get all actor id synchronously.
  std::vector<ActorID> GetAllActorID();

  /// Get actor table data by actor id synchronously.
  Status Get(const ActorID &actor_id, ActorTableData *actor_table_data);
};

class WorkerTable : public Table<WorkerID, WorkerTableData> {
 public:
  WorkerTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
              RedisGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::WORKER_FAILURE_PUBSUB;
    prefix_ = TablePrefix::WORKERS;
  }
  virtual ~WorkerTable() {}
};

class TaskReconstructionLog : public Log<TaskID, TaskReconstructionData> {
 public:
  TaskReconstructionLog(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                        RedisGcsClient *client)
      : Log(contexts, client) {
    prefix_ = TablePrefix::TASK_RECONSTRUCTION;
  }
};

class TaskLeaseTable : public Table<TaskID, TaskLeaseData> {
 public:
  /// Use boost::optional to represent subscription results, so that we can
  /// notify raylet whether the entry of task lease is empty.
  using Callback =
      std::function<void(RedisGcsClient *client, const TaskID &task_id,
                         const std::vector<boost::optional<TaskLeaseData>> &data)>;

  TaskLeaseTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                 RedisGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::TASK_LEASE_PUBSUB;
    prefix_ = TablePrefix::TASK_LEASE;
  }

  Status Add(const JobID &job_id, const TaskID &id,
             const std::shared_ptr<TaskLeaseData> &data,
             const WriteCallback &done) override {
    RAY_RETURN_NOT_OK((Table<TaskID, TaskLeaseData>::Add(job_id, id, data, done)));
    // Mark the entry for expiration in Redis. It's okay if this command fails
    // since the lease entry itself contains the expiration period. In the
    // worst case, if the command fails, then a client that looks up the lease
    // entry will overestimate the expiration time.
    // TODO(swang): Use a common helper function to format the key instead of
    // hardcoding it to match the Redis module.
    std::vector<std::string> args = {"PEXPIRE", TablePrefix_Name(prefix_) + id.Binary(),
                                     std::to_string(data->timeout())};

    return GetRedisContext(id)->RunArgvAsync(args);
  }

  /// Implement this method for the subscription tools class SubscriptionExecutor.
  /// In this way TaskLeaseTable() can also reuse class SubscriptionExecutor.
  Status Subscribe(const JobID &job_id, const NodeID &node_id, const Callback &subscribe,
                   const SubscriptionCallback &done);
};

class ActorCheckpointTable : public Table<ActorCheckpointID, ActorCheckpointData> {
 public:
  ActorCheckpointTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                       RedisGcsClient *client)
      : Table(contexts, client) {
    prefix_ = TablePrefix::ACTOR_CHECKPOINT;
  };
};

class ActorCheckpointIdTable : public Table<ActorID, ActorCheckpointIdData> {
 public:
  ActorCheckpointIdTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                         RedisGcsClient *client)
      : Table(contexts, client) {
    prefix_ = TablePrefix::ACTOR_CHECKPOINT_ID;
  };

  /// Add a checkpoint id to an actor, and remove a previous checkpoint if the
  /// total number of checkpoints in GCS exceeds the max allowed value.
  ///
  /// \param job_id The ID of the job.
  /// \param actor_id ID of the actor.
  /// \param checkpoint_id ID of the checkpoint.
  /// \return Status.
  Status AddCheckpointId(const JobID &job_id, const ActorID &actor_id,
                         const ActorCheckpointID &checkpoint_id,
                         const WriteCallback &done);
};

namespace raylet {

class TaskTable : public Table<TaskID, TaskTableData> {
 public:
  TaskTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
            RedisGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::RAYLET_TASK_PUBSUB;
    prefix_ = TablePrefix::RAYLET_TASK;
  }

  TaskTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
            RedisGcsClient *client, gcs::CommandType command_type)
      : TaskTable(contexts, client) {
    command_type_ = command_type;
  };
};

}  // namespace raylet

class ProfileTable : public Log<UniqueID, ProfileTableData> {
 public:
  ProfileTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
               RedisGcsClient *client)
      : Log(contexts, client) {
    prefix_ = TablePrefix::PROFILE;
  };

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;
};

/// \class NodeTable
///
/// The NodeTable stores information about active and inactive nodes. It is
/// structured as a single log stored at a key known to all nodes. When a
/// node connects, it appends an entry to the log indicating that it is
/// alive. When a node disconnects, or if another node detects its failure,
/// it should append an entry to the log indicating that it is dead. A node
/// that is marked as dead should never again be marked as alive; if it needs
/// to reconnect, it must connect with a different NodeID.
class NodeTable : public Log<NodeID, GcsNodeInfo> {
 public:
  NodeTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
            RedisGcsClient *client)
      : Log(contexts, client) {
    pubsub_channel_ = TablePubsub::NODE_PUBSUB;
    prefix_ = TablePrefix::NODE;
  };

  /// Connect as a NODE to the GCS. This registers us in the NODE table
  /// and begins subscription to NODE table notifications.
  ///
  /// \param local_node_info Information about the connecting NODE. This must have the
  /// same id as the one set in the NODE table.
  /// \return Status
  ray::Status Connect(const GcsNodeInfo &local_node_info);

  /// Disconnect the NODE from the GCS. The NODE ID assigned during
  /// registration should never be reused after disconnecting.
  ///
  /// \return Status
  ray::Status Disconnect();

  /// Mark a new node as connected to GCS asynchronously.
  ///
  /// \param node_info Information about the node.
  /// \param done Callback that is called once the node has been marked to connected.
  /// \return Status
  ray::Status MarkConnected(const GcsNodeInfo &node_info, const WriteCallback &done);

  /// Mark a different node as disconnected. The NODE ID should never be
  /// reused for a new node.
  ///
  /// \param dead_node_id The ID of the node to mark as dead.
  /// \param done Callback that is called once the node has been marked to
  /// disconnected.
  /// \return Status
  ray::Status MarkDisconnected(const NodeID &dead_node_id, const WriteCallback &done);

  ray::Status SubscribeToNodeChange(
      const SubscribeCallback<NodeID, GcsNodeInfo> &subscribe,
      const StatusCallback &done);

  /// Get a node's information from the cache. The cache only contains
  /// information for nodes that we've heard a notification for.
  ///
  /// \param node The node to get information about.
  /// \param node_info The node information will be copied here if
  /// we have the node in the cache.
  /// a nil node ID.
  /// \return Whether the node is in the cache.
  bool GetNode(const NodeID &node, GcsNodeInfo *node_info) const;

  /// Get the local node's ID.
  ///
  /// \return The local node's ID.
  const NodeID &GetLocalNodeId() const;

  /// Get the local node's information.
  ///
  /// \return The local node's information.
  const GcsNodeInfo &GetLocalNode() const;

  /// Check whether the given node is removed.
  ///
  /// \param node_id The ID of the node to check.
  /// \return Whether the node with specified ID is removed.
  bool IsRemoved(const NodeID &node_id) const;

  /// Get the information of all nodes.
  ///
  /// \return The node ID to node information map.
  const std::unordered_map<NodeID, GcsNodeInfo> &GetAllNodes() const;

  /// Lookup the node data in the node table.
  ///
  /// \param lookup Callback that is called after lookup. If the callback is
  /// called with an empty vector, then there was no data at the key.
  /// \return Status.
  Status Lookup(const Callback &lookup);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// The key at which the log of node information is stored. This key must
  /// be kept the same across all instances of the NodeTable, so that all
  /// nodes append and read from the same key.
  NodeID node_log_key_;

 private:
  using NodeChangeCallback =
      std::function<void(const NodeID &id, const GcsNodeInfo &node_info)>;

  /// Register a callback to call when a new node is added or a node is removed.
  ///
  /// \param callback The callback to register.
  void RegisterNodeChangeCallback(const NodeChangeCallback &callback);

  /// Handle a node table notification.
  void HandleNotification(RedisGcsClient *client, const GcsNodeInfo &node_info);

  /// Whether this node has called Disconnect().
  bool disconnected_{false};
  /// This node's ID. It will be initialized when we call method `Connect(...)`.
  NodeID local_node_id_;
  /// Information about this node.
  GcsNodeInfo local_node_info_;
  /// This ID is used in method `SubscribeToNodeChange(...)` to Subscribe and
  /// RequestNotification.
  /// The reason for not using `local_node_id_` is because it is only initialized
  /// for registered nodes.
  NodeID subscribe_id_{NodeID::FromRandom()};
  /// The callback to call when a new node is added or a node is removed.
  NodeChangeCallback node_change_callback_{nullptr};
  /// A cache for information about all nodes.
  std::unordered_map<NodeID, GcsNodeInfo> node_cache_;
  /// The set of removed nodes.
  std::unordered_set<NodeID> removed_nodes_;
};

}  // namespace gcs

}  // namespace ray
