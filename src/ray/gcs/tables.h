#ifndef RAY_GCS_TABLES_H
#define RAY_GCS_TABLES_H

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "ray/common/constants.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"

#include "ray/gcs/redis_context.h"
#include "ray/protobuf/gcs.pb.h"

struct redisAsyncContext;

namespace ray {

namespace gcs {

using rpc::ActorCheckpointData;
using rpc::ActorCheckpointIdData;
using rpc::ActorTableData;
using rpc::ClientTableData;
using rpc::ErrorTableData;
using rpc::GcsChangeMode;
using rpc::GcsEntry;
using rpc::HeartbeatBatchTableData;
using rpc::HeartbeatTableData;
using rpc::JobTableData;
using rpc::ObjectTableData;
using rpc::ProfileTableData;
using rpc::RayResource;
using rpc::TablePrefix;
using rpc::TablePubsub;
using rpc::TaskLeaseData;
using rpc::TaskReconstructionData;
using rpc::TaskTableData;

class RedisContext;

class AsyncGcsClient;

/// Specifies whether commands issued to a table should be regular or chain-replicated
/// (when available).
enum class CommandType { kRegular, kChain };

/// \class PubsubInterface
///
/// The interface for a pubsub storage system. The client of a storage system
/// that implements this interface can request and cancel notifications for
/// specific keys.
template <typename ID>
class PubsubInterface {
 public:
  virtual Status RequestNotifications(const JobID &job_id, const ID &id,
                                      const ClientID &client_id) = 0;
  virtual Status CancelNotifications(const JobID &job_id, const ID &id,
                                     const ClientID &client_id) = 0;
  virtual ~PubsubInterface(){};
};

template <typename ID, typename Data>
class LogInterface {
 public:
  using WriteCallback =
      std::function<void(AsyncGcsClient *client, const ID &id, const Data &data)>;
  virtual Status Append(const JobID &job_id, const ID &id, std::shared_ptr<Data> &data,
                        const WriteCallback &done) = 0;
  virtual Status AppendAt(const JobID &job_id, const ID &task_id,
                          std::shared_ptr<Data> &data, const WriteCallback &done,
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
///   ClientTable: Stores a log of which GCS clients have been added or deleted
///                from the system.
template <typename ID, typename Data>
class Log : public LogInterface<ID, Data>, virtual public PubsubInterface<ID> {
 public:
  using Callback = std::function<void(AsyncGcsClient *client, const ID &id,
                                      const std::vector<Data> &data)>;
  using NotificationCallback =
      std::function<void(AsyncGcsClient *client, const ID &id,
                         const GcsChangeMode change_mode, const std::vector<Data> &data)>;
  /// The callback to call when a write to a key succeeds.
  using WriteCallback = typename LogInterface<ID, Data>::WriteCallback;
  /// The callback to call when a SUBSCRIBE call completes and we are ready to
  /// request and receive notifications.
  using SubscriptionCallback = std::function<void(AsyncGcsClient *client)>;

  struct CallbackData {
    ID id;
    std::shared_ptr<Data> data;
    Callback callback;
    // An optional callback to call for subscription operations, where the
    // first message is a notification of subscription success.
    SubscriptionCallback subscription_callback;
    Log<ID, Data> *log;
    AsyncGcsClient *client;
  };

  Log(const std::vector<std::shared_ptr<RedisContext>> &contexts, AsyncGcsClient *client)
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
  Status Append(const JobID &job_id, const ID &id, std::shared_ptr<Data> &data,
                const WriteCallback &done);

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
  Status AppendAt(const JobID &job_id, const ID &id, std::shared_ptr<Data> &data,
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
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given client will be received. In the latter
  /// case, the client may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data at the key.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status Subscribe(const JobID &job_id, const ClientID &client_id,
                   const Callback &subscribe, const SubscriptionCallback &done);

  /// Request notifications about a key in this table.
  ///
  /// The notifications will be returned via the subscribe callback that was
  /// registered by `Subscribe`.  An initial notification will be returned for
  /// the current values at the key, if any, and a subsequent notification will
  /// be published for every following `Append` to the key. Before
  /// notifications can be requested, the caller must first call `Subscribe`,
  /// with the same `client_id`.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the key to request notifications for.
  /// \param client_id The client who is requesting notifications. Before
  /// notifications can be requested, a call to `Subscribe` to this
  /// table with the same `client_id` must complete successfully.
  /// \return Status
  Status RequestNotifications(const JobID &job_id, const ID &id,
                              const ClientID &client_id);

  /// Cancel notifications about a key in this table.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the key to request notifications for.
  /// \param client_id The client who originally requested notifications.
  /// \return Status
  Status CancelNotifications(const JobID &job_id, const ID &id,
                             const ClientID &client_id);

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

  /// Subscribe to any modifications to the key. The caller may choose
  /// to subscribe to all modifications, or to subscribe only to keys that it
  /// requests notifications for. This may only be called once per Log
  /// instance. This function is different from public version due to
  /// an additional parameter change_mode in NotificationCallback. Therefore this
  /// function supports notifications of remove operations.
  ///
  /// \param job_id The ID of the job.
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given client will be received. In the latter
  /// case, the client may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data at the key.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status Subscribe(const JobID &job_id, const ClientID &client_id,
                   const NotificationCallback &subscribe,
                   const SubscriptionCallback &done);

  /// The connection to the GCS.
  std::vector<std::shared_ptr<RedisContext>> shard_contexts_;
  /// The GCS client.
  AsyncGcsClient *client_;
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
  virtual Status Add(const JobID &job_id, const ID &task_id, std::shared_ptr<Data> &data,
                     const WriteCallback &done) = 0;
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
      std::function<void(AsyncGcsClient *client, const ID &id, const Data &data)>;
  using WriteCallback = typename Log<ID, Data>::WriteCallback;
  /// The callback to call when a Lookup call returns an empty entry.
  using FailureCallback = std::function<void(AsyncGcsClient *client, const ID &id)>;
  /// The callback to call when a Subscribe call completes and we are ready to
  /// request and receive notifications.
  using SubscriptionCallback = typename Log<ID, Data>::SubscriptionCallback;

  Table(const std::vector<std::shared_ptr<RedisContext>> &contexts,
        AsyncGcsClient *client)
      : Log<ID, Data>(contexts, client) {}

  using Log<ID, Data>::RequestNotifications;
  using Log<ID, Data>::CancelNotifications;

  /// Add an entry to the table. This overwrites any existing data at the key.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data that is added to the GCS.
  /// \param done Callback that is called once the data has been written to the
  /// GCS.
  /// \return Status
  Status Add(const JobID &job_id, const ID &id, std::shared_ptr<Data> &data,
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
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given client will be received. In the latter
  /// case, the client may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data at the key.
  /// \param failure Callback that is called if the key is empty at the time
  /// that notifications are requested.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status Subscribe(const JobID &job_id, const ClientID &client_id,
                   const Callback &subscribe, const FailureCallback &failure,
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
  virtual Status Add(const JobID &job_id, const ID &id, std::shared_ptr<Data> &data,
                     const WriteCallback &done) = 0;
  virtual Status Remove(const JobID &job_id, const ID &id, std::shared_ptr<Data> &data,
                        const WriteCallback &done) = 0;
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
  using NotificationCallback = typename Log<ID, Data>::NotificationCallback;
  using SubscriptionCallback = typename Log<ID, Data>::SubscriptionCallback;

  Set(const std::vector<std::shared_ptr<RedisContext>> &contexts, AsyncGcsClient *client)
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
  Status Add(const JobID &job_id, const ID &id, std::shared_ptr<Data> &data,
             const WriteCallback &done);

  /// Remove an entry from the set.
  ///
  /// \param job_id The ID of the job.
  /// \param id The ID of the data that is removed from the GCS.
  /// \param data Data to remove from the set.
  /// \param done Callback that is called once the data has been written to the
  /// GCS.
  /// \return Status
  Status Remove(const JobID &job_id, const ID &id, std::shared_ptr<Data> &data,
                const WriteCallback &done);

  Status Subscribe(const JobID &job_id, const ClientID &client_id,
                   const NotificationCallback &subscribe,
                   const SubscriptionCallback &done) {
    return Log<ID, Data>::Subscribe(job_id, client_id, subscribe, done);
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
      std::function<void(AsyncGcsClient *client, const ID &id, const DataMap &pairs)>;

  /// The callback function used by function RemoveEntries.
  ///
  /// \param client The client on which the RemoveEntries is called.
  /// \param id The ID of the Hash Table whose entries are removed.
  /// \param keys The keys that are moved from this Hash Table.
  /// \return Void
  using HashRemoveCallback = std::function<void(AsyncGcsClient *client, const ID &id,
                                                const std::vector<std::string> &keys)>;

  /// The notification function used by function Subscribe.
  ///
  /// \param client The client on which the Subscribe is called.
  /// \param change_mode The mode to identify the data is removed or updated.
  /// \param data Map data contains the change to the Hash Table.
  /// \return Void
  using HashNotificationCallback =
      std::function<void(AsyncGcsClient *client, const ID &id,
                         const GcsChangeMode change_mode, const DataMap &data)>;

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
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each Update to the table will be received. Else, only
  /// messages for the given client will be received. In the latter
  /// case, the client may request notifications on specific keys in the
  /// table via `RequestNotifications`.
  /// \param subscribe HashNotificationCallback that is called on each received message.
  /// \param done SubscriptionCallback that is called when subscription is complete and
  /// we are ready to receive messages.
  /// \return Status
  virtual Status Subscribe(const JobID &job_id, const ClientID &client_id,
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

  Hash(const std::vector<std::shared_ptr<RedisContext>> &contexts, AsyncGcsClient *client)
      : Log<ID, Data>(contexts, client) {}

  using Log<ID, Data>::RequestNotifications;
  using Log<ID, Data>::CancelNotifications;

  Status Update(const JobID &job_id, const ID &id, const DataMap &pairs,
                const HashCallback &done) override;

  Status Subscribe(const JobID &job_id, const ClientID &client_id,
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

class DynamicResourceTable : public Hash<ClientID, RayResource> {
 public:
  DynamicResourceTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                       AsyncGcsClient *client)
      : Hash(contexts, client) {
    pubsub_channel_ = TablePubsub::NODE_RESOURCE_PUBSUB;
    prefix_ = TablePrefix::NODE_RESOURCE;
  };

  virtual ~DynamicResourceTable(){};
};

class ObjectTable : public Set<ObjectID, ObjectTableData> {
 public:
  ObjectTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
              AsyncGcsClient *client)
      : Set(contexts, client) {
    pubsub_channel_ = TablePubsub::OBJECT_PUBSUB;
    prefix_ = TablePrefix::OBJECT;
  };

  virtual ~ObjectTable(){};
};

class HeartbeatTable : public Table<ClientID, HeartbeatTableData> {
 public:
  HeartbeatTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                 AsyncGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::HEARTBEAT_PUBSUB;
    prefix_ = TablePrefix::HEARTBEAT;
  }
  virtual ~HeartbeatTable() {}
};

class HeartbeatBatchTable : public Table<ClientID, HeartbeatBatchTableData> {
 public:
  HeartbeatBatchTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                      AsyncGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::HEARTBEAT_BATCH_PUBSUB;
    prefix_ = TablePrefix::HEARTBEAT_BATCH;
  }
  virtual ~HeartbeatBatchTable() {}
};

class JobTable : public Log<JobID, JobTableData> {
 public:
  JobTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
           AsyncGcsClient *client)
      : Log(contexts, client) {
    pubsub_channel_ = TablePubsub::JOB_PUBSUB;
    prefix_ = TablePrefix::JOB;
  };

  virtual ~JobTable() {}

  /// Appends job data to the job table.
  ///
  /// \param job_id The job id.
  /// \param is_dead Whether the job is dead.
  /// \param timestamp The UNIX timestamp when the driver was started/stopped.
  /// \param node_manager_address IP address of the node the driver is running on.
  /// \param driver_pid Process ID of the driver process.
  /// \return The return status.
  Status AppendJobData(const JobID &job_id, bool is_dead, int64_t timestamp,
                       const std::string &node_manager_address, int64_t driver_pid);
};

/// Actor table starts with an ALIVE entry, which represents the first time the actor
/// is created. This may be followed by 0 or more pairs of RECONSTRUCTING, ALIVE entries,
/// which represent each time the actor fails (RECONSTRUCTING) and gets recreated (ALIVE).
/// These may be followed by a DEAD entry, which means that the actor has failed and will
/// not be reconstructed.
class ActorTable : public Log<ActorID, ActorTableData> {
 public:
  ActorTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
             AsyncGcsClient *client)
      : Log(contexts, client) {
    pubsub_channel_ = TablePubsub::ACTOR_PUBSUB;
    prefix_ = TablePrefix::ACTOR;
  }
};

class TaskReconstructionLog : public Log<TaskID, TaskReconstructionData> {
 public:
  TaskReconstructionLog(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                        AsyncGcsClient *client)
      : Log(contexts, client) {
    prefix_ = TablePrefix::TASK_RECONSTRUCTION;
  }
};

class TaskLeaseTable : public Table<TaskID, TaskLeaseData> {
 public:
  TaskLeaseTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                 AsyncGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::TASK_LEASE_PUBSUB;
    prefix_ = TablePrefix::TASK_LEASE;
  }

  Status Add(const JobID &job_id, const TaskID &id, std::shared_ptr<TaskLeaseData> &data,
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
};

class ActorCheckpointTable : public Table<ActorCheckpointID, ActorCheckpointData> {
 public:
  ActorCheckpointTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                       AsyncGcsClient *client)
      : Table(contexts, client) {
    prefix_ = TablePrefix::ACTOR_CHECKPOINT;
  };
};

class ActorCheckpointIdTable : public Table<ActorID, ActorCheckpointIdData> {
 public:
  ActorCheckpointIdTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
                         AsyncGcsClient *client)
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
                         const ActorCheckpointID &checkpoint_id);
};

namespace raylet {

class TaskTable : public Table<TaskID, TaskTableData> {
 public:
  TaskTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
            AsyncGcsClient *client)
      : Table(contexts, client) {
    pubsub_channel_ = TablePubsub::RAYLET_TASK_PUBSUB;
    prefix_ = TablePrefix::RAYLET_TASK;
  }

  TaskTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
            AsyncGcsClient *client, gcs::CommandType command_type)
      : TaskTable(contexts, client) {
    command_type_ = command_type;
  };
};

}  // namespace raylet

class ErrorTable : private Log<JobID, ErrorTableData> {
 public:
  ErrorTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
             AsyncGcsClient *client)
      : Log(contexts, client) {
    pubsub_channel_ = TablePubsub::ERROR_INFO_PUBSUB;
    prefix_ = TablePrefix::ERROR_INFO;
  };

  /// Push an error message for the driver of a specific.
  ///
  /// TODO(rkn): We need to make sure that the errors are unique because
  /// duplicate messages currently cause failures (the GCS doesn't allow it). A
  /// natural way to do this is to have finer-grained time stamps.
  ///
  /// \param job_id The ID of the job that generated the error. If the error
  /// should be pushed to all drivers, then this should be nil.
  /// \param type The type of the error.
  /// \param error_message The error message to push.
  /// \param timestamp The timestamp of the error.
  /// \return Status.
  // TODO(qwang): refactor this API to implement broadcast.
  Status PushErrorToDriver(const JobID &job_id, const std::string &type,
                           const std::string &error_message, double timestamp);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;
};

class ProfileTable : private Log<UniqueID, ProfileTableData> {
 public:
  ProfileTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
               AsyncGcsClient *client)
      : Log(contexts, client) {
    prefix_ = TablePrefix::PROFILE;
  };

  /// Add a batch of profiling events to the profile table.
  ///
  /// \param profile_events The profile events to record.
  /// \return Status.
  Status AddProfileEventBatch(const ProfileTableData &profile_events);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;
};

/// \class ClientTable
///
/// The ClientTable stores information about active and inactive clients. It is
/// structured as a single log stored at a key known to all clients. When a
/// client connects, it appends an entry to the log indicating that it is
/// alive. When a client disconnects, or if another client detects its failure,
/// it should append an entry to the log indicating that it is dead. A client
/// that is marked as dead should never again be marked as alive; if it needs
/// to reconnect, it must connect with a different ClientID.
class ClientTable : public Log<ClientID, ClientTableData> {
 public:
  using ClientTableCallback = std::function<void(
      AsyncGcsClient *client, const ClientID &id, const ClientTableData &data)>;
  using DisconnectCallback = std::function<void(void)>;
  ClientTable(const std::vector<std::shared_ptr<RedisContext>> &contexts,
              AsyncGcsClient *client, const ClientID &client_id)
      : Log(contexts, client),
        // We set the client log's key equal to nil so that all instances of
        // ClientTable have the same key.
        client_log_key_(),
        disconnected_(false),
        client_id_(client_id),
        local_client_() {
    pubsub_channel_ = TablePubsub::CLIENT_PUBSUB;
    prefix_ = TablePrefix::CLIENT;

    // Set the local client's ID.
    local_client_.set_client_id(client_id.Binary());
  };

  /// Connect as a client to the GCS. This registers us in the client table
  /// and begins subscription to client table notifications.
  ///
  /// \param Information about the connecting client. This must have the
  /// same client_id as the one set in the client table.
  /// \return Status
  ray::Status Connect(const ClientTableData &local_client);

  /// Disconnect the client from the GCS. The client ID assigned during
  /// registration should never be reused after disconnecting.
  ///
  /// \return Status
  ray::Status Disconnect(const DisconnectCallback &callback = nullptr);

  /// Mark a different client as disconnected. The client ID should never be
  /// reused for a new client.
  ///
  /// \param dead_client_id The ID of the client to mark as dead.
  /// \return Status
  ray::Status MarkDisconnected(const ClientID &dead_client_id);

  /// Register a callback to call when a new client is added.
  ///
  /// \param callback The callback to register.
  void RegisterClientAddedCallback(const ClientTableCallback &callback);

  /// Register a callback to call when a client is removed.
  ///
  /// \param callback The callback to register.
  void RegisterClientRemovedCallback(const ClientTableCallback &callback);

  /// Register a callback to call when a resource is created or updated.
  ///
  /// \param callback The callback to register.
  void RegisterResourceCreateUpdatedCallback(const ClientTableCallback &callback);

  /// Register a callback to call when a resource is deleted.
  ///
  /// \param callback The callback to register.
  void RegisterResourceDeletedCallback(const ClientTableCallback &callback);

  /// Get a client's information from the cache. The cache only contains
  /// information for clients that we've heard a notification for.
  ///
  /// \param client The client to get information about.
  /// \param A reference to the client information. If we have information
  /// about the client in the cache, then the reference will be modified to
  /// contain that information. Else, the reference will be updated to contain
  /// a nil client ID.
  void GetClient(const ClientID &client, ClientTableData &client_info) const;

  /// Get the local client's ID.
  ///
  /// \return The local client's ID.
  const ClientID &GetLocalClientId() const;

  /// Get the local client's information.
  ///
  /// \return The local client's information.
  const ClientTableData &GetLocalClient() const;

  /// Check whether the given client is removed.
  ///
  /// \param client_id The ID of the client to check.
  /// \return Whether the client with ID client_id is removed.
  bool IsRemoved(const ClientID &client_id) const;

  /// Get the information of all clients.
  ///
  /// \return The client ID to client information map.
  const std::unordered_map<ClientID, ClientTableData> &GetAllClients() const;

  /// Lookup the client data in the client table.
  ///
  /// \param lookup Callback that is called after lookup. If the callback is
  /// called with an empty vector, then there was no data at the key.
  /// \return Status.
  Status Lookup(const Callback &lookup);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// The key at which the log of client information is stored. This key must
  /// be kept the same across all instances of the ClientTable, so that all
  /// clients append and read from the same key.
  ClientID client_log_key_;

 private:
  /// Handle a client table notification.
  void HandleNotification(AsyncGcsClient *client, const ClientTableData &notifications);
  /// Handle this client's successful connection to the GCS.
  void HandleConnected(AsyncGcsClient *client, const ClientTableData &client_data);
  /// Whether this client has called Disconnect().
  bool disconnected_;
  /// This client's ID.
  const ClientID client_id_;
  /// Information about this client.
  ClientTableData local_client_;
  /// The callback to call when a new client is added.
  ClientTableCallback client_added_callback_;
  /// The callback to call when a client is removed.
  ClientTableCallback client_removed_callback_;
  /// The callback to call when a resource is created or updated.
  ClientTableCallback resource_createupdated_callback_;
  /// The callback to call when a resource is deleted.
  ClientTableCallback resource_deleted_callback_;
  /// A cache for information about all clients.
  std::unordered_map<ClientID, ClientTableData> client_cache_;
  /// The set of removed clients.
  std::unordered_set<ClientID> removed_clients_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_TABLES_H
