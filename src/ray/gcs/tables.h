#ifndef RAY_GCS_TABLES_H
#define RAY_GCS_TABLES_H

#include <map>
#include <string>
#include <unordered_map>

#include "ray/constants.h"
#include "ray/id.h"
#include "ray/status.h"
#include "ray/util/logging.h"

#include "ray/gcs/format/gcs_generated.h"
#include "ray/gcs/redis_context.h"
#include "ray/raylet/format/node_manager_generated.h"

// TODO(pcm): Remove this
#include "task.h"

struct redisAsyncContext;

namespace ray {

namespace gcs {

class RedisContext;

class AsyncGcsClient;

/// \class Log
///
/// A GCS table where every entry is an append-only log.
/// Example tables backed by Log:
///   ObjectTable: Stores a log of which clients have added or evicted an
///                object.
///   ClientTable: Stores a log of which GCS clients have been added or deleted
///                from the system.
template <typename ID, typename Data>
class Log {
 public:
  using DataT = typename Data::NativeTableType;
  using Callback = std::function<void(AsyncGcsClient *client, const ID &id,
                                      const std::vector<DataT> &data)>;
  /// The callback to call when a write to a key succeeds.
  using WriteCallback = std::function<void(AsyncGcsClient *client, const ID &id,
                                           std::shared_ptr<DataT> data)>;
  /// The callback to call when a SUBSCRIBE call completes and we are ready to
  /// request and receive notifications.
  using SubscriptionCallback = std::function<void(AsyncGcsClient *client)>;

  struct CallbackData {
    ID id;
    std::shared_ptr<DataT> data;
    Callback callback;
    // An optional callback to call for subscription operations, where the
    // first message is a notification of subscription success.
    SubscriptionCallback subscription_callback;
    Log<ID, Data> *log;
    AsyncGcsClient *client;
  };

  Log(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : context_(context),
        client_(client),
        pubsub_channel_(TablePubsub_NO_PUBLISH),
        prefix_(TablePrefix_UNUSED),
        subscribe_callback_index_(-1){};

  /// Append a log entry to a key.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data to append to the log.
  /// \param done Callback that is called once the data has been written to the
  ///        GCS.
  /// \return Status
  Status Append(const JobID &job_id, const ID &id, std::shared_ptr<DataT> data,
                const WriteCallback &done);

  /// Append a log entry to a key if and only if the log has the given number
  /// of entries.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data to append to the log.
  /// \param done Callback that is called if the data was appended to the log.
  /// \param failure Callback that is called if the data was not appended to
  ///        the log because the log length did not match the given
  ///        `log_length`.
  /// \param log_length The number of entries that the log must have for the
  ///        append to succeed.
  /// \return Status
  Status AppendAt(const JobID &job_id, const ID &id, std::shared_ptr<DataT> data,
                  const WriteCallback &done, const WriteCallback &failure,
                  int log_length);

  /// Lookup the log values at a key asynchronously.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the data that is looked up in the GCS.
  /// \param lookup Callback that is called after lookup. If the callback is
  ///        called with an empty vector, then there was no data at the key.
  /// \return Status
  Status Lookup(const JobID &job_id, const ID &id, const Callback &lookup);

  /// Subscribe to any Append operations to this table. The caller may choose
  /// to subscribe to all Appends, or to subscribe only to keys that it
  /// requests notifications for. This may only be called once per Log
  /// instance.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param client_id The type of update to listen to. If this is nil, then a
  ///        message for each Add to the table will be received. Else, only
  ///        messages for the given client will be received. In the latter
  ///        case, the client may request notifications on specific keys in the
  ///        table via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  ///        callback is called with an empty vector, then there was no data at
  ///        the key.
  /// \param done Callback that is called when subscription is complete and we
  ///        are ready to receive messages.
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
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the key to request notifications for.
  /// \param client_id The client who is requesting notifications. Before
  ///        notifications can be requested, a call to `Subscribe` to this
  ///        table with the same `client_id` must complete successfully.
  /// \return Status
  Status RequestNotifications(const JobID &job_id, const ID &id,
                              const ClientID &client_id);

  /// Cancel notifications about a key in this table.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the key to request notifications for.
  /// \param client_id The client who originally requested notifications.
  /// \return Status
  Status CancelNotifications(const JobID &job_id, const ID &id,
                             const ClientID &client_id);

 protected:
  /// The connection to the GCS.
  std::shared_ptr<RedisContext> context_;
  /// The GCS client.
  AsyncGcsClient *client_;
  /// The pubsub channel to subscribe to for notifications about keys in this
  /// table. If no notifications are required, this may be set to
  /// TablePubsub_NO_PUBLISH.
  TablePubsub pubsub_channel_;
  /// The prefix to use for keys in this table.
  TablePrefix prefix_;
  /// The index in the RedisCallbackManager for the callback that is called
  /// when we receive notifications. This is >= 0 iff we have subscribed to the
  /// table, otherwise -1.
  int64_t subscribe_callback_index_;
};

template <typename ID, typename Data>
class TableInterface {
 public:
  using DataT = typename Data::NativeTableType;
  using WriteCallback = typename Log<ID, Data>::WriteCallback;
  virtual Status Add(const JobID &job_id, const ID &task_id, std::shared_ptr<DataT> data,
                     const WriteCallback &done) = 0;
  virtual ~TableInterface(){};
};

/// \class Table
///
/// A GCS table where every entry is a single data item.
/// Example tables backed by Log:
///   TaskTable: Stores Task metadata needed for executing the task.
template <typename ID, typename Data>
class Table : private Log<ID, Data>, public TableInterface<ID, Data> {
 public:
  using DataT = typename Log<ID, Data>::DataT;
  using Callback =
      std::function<void(AsyncGcsClient *client, const ID &id, const DataT &data)>;
  using WriteCallback = typename Log<ID, Data>::WriteCallback;
  /// The callback to call when a Lookup call returns an empty entry.
  using FailureCallback = std::function<void(AsyncGcsClient *client, const ID &id)>;
  /// The callback to call when a Subscribe call completes and we are ready to
  /// request and receive notifications.
  using SubscriptionCallback = typename Log<ID, Data>::SubscriptionCallback;

  struct CallbackData {
    ID id;
    std::shared_ptr<DataT> data;
    Callback callback;
    // An optional callback to call for subscription operations, where the
    // first message is a notification of subscription success.
    SubscriptionCallback subscription_callback;
    Log<ID, Data> *log;
    AsyncGcsClient *client;
  };

  Table(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : Log<ID, Data>(context, client) {}

  using Log<ID, Data>::RequestNotifications;
  using Log<ID, Data>::CancelNotifications;

  /// Add an entry to the table. This overwrites any existing data at the key.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data that is added to the GCS.
  /// \param done Callback that is called once the data has been written to the
  ///        GCS.
  /// \return Status
  Status Add(const JobID &job_id, const ID &id, std::shared_ptr<DataT> data,
             const WriteCallback &done);

  /// Lookup an entry asynchronously.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the data that is looked up in the GCS.
  /// \param lookup Callback that is called after lookup if there was data the
  ///        key.
  /// \param failure Callback that is called after lookup if there was no data
  ///        at the key.
  /// \return Status
  Status Lookup(const JobID &job_id, const ID &id, const Callback &lookup,
                const FailureCallback &failure);

  Status Subscribe(const JobID &job_id, const ClientID &client_id,
                   const Callback &subscribe, const SubscriptionCallback &done);

 protected:
  using Log<ID, Data>::context_;
  using Log<ID, Data>::client_;
  using Log<ID, Data>::pubsub_channel_;
  using Log<ID, Data>::prefix_;
};

class ObjectTable : public Log<ObjectID, ObjectTableData> {
 public:
  ObjectTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : Log(context, client) {
    pubsub_channel_ = TablePubsub_OBJECT;
    prefix_ = TablePrefix_OBJECT;
  };
  virtual ~ObjectTable(){};
};

class HeartbeatTable : public Table<ClientID, HeartbeatTableData> {
 public:
  HeartbeatTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : Table(context, client) {
    pubsub_channel_ = TablePubsub_HEARTBEAT;
    prefix_ = TablePrefix_HEARTBEAT;
  }
  virtual ~HeartbeatTable() {}
};

class FunctionTable : public Table<ObjectID, FunctionTableData> {
 public:
  FunctionTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : Table(context, client) {
    pubsub_channel_ = TablePubsub_NO_PUBLISH;
    prefix_ = TablePrefix_FUNCTION;
  };
};

using ClassTable = Table<ClassID, ClassTableData>;

// TODO(swang): Set the pubsub channel for the actor table.
using ActorTable = Table<ActorID, ActorTableData>;

class TaskReconstructionLog : public Log<TaskID, TaskReconstructionData> {
 public:
  TaskReconstructionLog(const std::shared_ptr<RedisContext> &context,
                        AsyncGcsClient *client)
      : Log(context, client) {
    prefix_ = TablePrefix_TASK_RECONSTRUCTION;
  }
};

namespace raylet {

class TaskTable : public Table<TaskID, ray::protocol::Task> {
 public:
  TaskTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : Table(context, client) {
    pubsub_channel_ = TablePubsub_RAYLET_TASK;
    prefix_ = TablePrefix_RAYLET_TASK;
  }
};

}  // namespace raylet

class TaskTable : public Table<TaskID, TaskTableData> {
 public:
  TaskTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : Table(context, client) {
    pubsub_channel_ = TablePubsub_TASK;
    prefix_ = TablePrefix_TASK;
  };
  ~TaskTable(){};

  using TestAndUpdateCallback =
      std::function<void(AsyncGcsClient *client, const TaskID &id,
                         const TaskTableDataT &task, bool updated)>;
  using SubscribeToTaskCallback =
      std::function<void(std::shared_ptr<TaskTableDataT> task)>;
  /// Update a task's scheduling information in the task table, if the current
  /// value matches the given test value. If the update succeeds, it also
  /// updates
  /// the task entry's local scheduler ID with the ID of the client who called
  /// this function. This assumes that the task spec already exists in the task
  /// table entry.
  ///
  /// \param task_id The task ID of the task entry to update.
  /// \param test_state_bitmask The bitmask to apply to the task entry's current
  ///        scheduling state.  The update happens if and only if the current
  ///        scheduling state AND-ed with the bitmask is greater than 0.
  /// \param update_state The value to update the task entry's scheduling state
  ///        with, if the current state matches test_state_bitmask.
  /// \param callback Function to be called when database returns result.
  /// \return Status
  Status TestAndUpdate(const JobID &job_id, const TaskID &id,
                       std::shared_ptr<TaskTableTestAndUpdateT> data,
                       const TestAndUpdateCallback &callback) {
    int64_t callback_index = RedisCallbackManager::instance().add(
        [this, callback, id](const std::string &data) {
          auto result = std::make_shared<TaskTableDataT>();
          auto root = flatbuffers::GetRoot<TaskTableData>(data.data());
          root->UnPackTo(result.get());
          callback(client_, id, *result, root->updated());
          return true;
        });
    flatbuffers::FlatBufferBuilder fbb;
    fbb.Finish(TaskTableTestAndUpdate::Pack(fbb, data.get()));
    RAY_RETURN_NOT_OK(context_->RunAsync("RAY.TABLE_TEST_AND_UPDATE", id,
                                         fbb.GetBufferPointer(), fbb.GetSize(), prefix_,
                                         pubsub_channel_, callback_index));
    return Status::OK();
  }

  /// This has a separate signature from Subscribe in Table
  /// Register a callback for a task event. An event is any update of a task in
  /// the task table.
  /// Events include changes to the task's scheduling state or changes to the
  /// task's local scheduler ID.
  ///
  /// \param local_scheduler_id The db_client_id of the local scheduler whose
  ///        events we want to listen to. If you want to subscribe to updates
  ///        from
  ///        all local schedulers, pass in NIL_ID.
  /// \param subscribe_callback Callback that will be called when the task table
  /// is
  ///        updated.
  /// \param state_filter Events we want to listen to. Can have values from the
  ///        enum "scheduling_state" in task.h.
  ///        TODO(pcm): Make it possible to combine these using flags like
  ///        TASK_STATUS_WAITING | TASK_STATUS_SCHEDULED.
  /// \param callback Function to be called when database returns result.
  /// \return Status
  Status SubscribeToTask(const JobID &job_id, const ClientID &local_scheduler_id,
                         int state_filter, const SubscribeToTaskCallback &callback,
                         const Callback &done);
};

Status TaskTableAdd(AsyncGcsClient *gcs_client, Task *task);

Status TaskTableTestAndUpdate(AsyncGcsClient *gcs_client, const TaskID &task_id,
                              const ClientID &local_scheduler_id, int test_state_bitmask,
                              SchedulingState update_state,
                              const TaskTable::TestAndUpdateCallback &callback);

using ErrorTable = Table<TaskID, ErrorTableData>;

using CustomSerializerTable = Table<ClassID, CustomSerializerData>;

using ConfigTable = Table<ConfigID, ConfigTableData>;

/// \class ClientTable
///
/// The ClientTable stores information about active and inactive clients. It is
/// structured as a single log stored at a key known to all clients. When a
/// client connects, it appends an entry to the log indicating that it is
/// alive. When a client disconnects, or if another client detects its failure,
/// it should append an entry to the log indicating that it is dead. A client
/// that is marked as dead should never again be marked as alive; if it needs
/// to reconnect, it must connect with a different ClientID.
class ClientTable : private Log<UniqueID, ClientTableData> {
 public:
  using ClientTableCallback = std::function<void(
      AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data)>;
  ClientTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client,
              const ClientID &client_id)
      : Log(context, client),
        // We set the client log's key equal to nil so that all instances of
        // ClientTable have the same key.
        client_log_key_(UniqueID::nil()),
        disconnected_(false),
        client_id_(client_id),
        local_client_() {
    pubsub_channel_ = TablePubsub_CLIENT;
    prefix_ = TablePrefix_CLIENT;

    // Set the local client's ID.
    local_client_.client_id = client_id.binary();

    // Add a nil client to the cache so that we can serve requests for clients
    // that we have not heard about.
    ClientTableDataT nil_client;
    nil_client.client_id = ClientID::nil().binary();
    client_cache_[ClientID::nil()] = nil_client;
  };

  /// Connect as a client to the GCS. This registers us in the client table
  /// and begins subscription to client table notifications.
  ///
  /// \param Information about the connecting client. This must have the
  ///        same client_id as the one set in the client table.
  /// \return Status
  ray::Status Connect(const ClientTableDataT &local_client);

  /// Disconnect the client from the GCS. The client ID assigned during
  /// registration should never be reused after disconnecting.
  ///
  /// \return Status
  ray::Status Disconnect();

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

  /// Get a client's information from the cache. The cache only contains
  /// information for clients that we've heard a notification for.
  ///
  /// \param client The client to get information about.
  /// \return A reference to the requested client. If the client is not in the
  ///         cache, then an entry with a nil ClientID will be returned.
  const ClientTableDataT &GetClient(const ClientID &client);

  /// Get the local client's ID.
  ///
  /// \return The local client's ID.
  const ClientID &GetLocalClientId();

  /// Get the local client's information.
  ///
  /// \return The local client's information.
  const ClientTableDataT &GetLocalClient();

 private:
  /// Handle a client table notification.
  void HandleNotification(AsyncGcsClient *client, const ClientTableDataT &notifications);
  /// Handle this client's successful connection to the GCS.
  void HandleConnected(AsyncGcsClient *client,
                       const std::shared_ptr<ClientTableDataT> client_data);

  /// The key at which the log of client information is stored. This key must
  /// be kept the same across all instances of the ClientTable, so that all
  /// clients append and read from the same key.
  UniqueID client_log_key_;
  /// Whether this client has called Disconnect().
  bool disconnected_;
  /// This client's ID.
  const ClientID client_id_;
  /// Information about this client.
  ClientTableDataT local_client_;
  /// The callback to call when a new client is added.
  ClientTableCallback client_added_callback_;
  /// The callback to call when a client is removed.
  ClientTableCallback client_removed_callback_;
  /// A cache for information about all clients.
  std::unordered_map<ClientID, ClientTableDataT, UniqueIDHasher> client_cache_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_TABLES_H
