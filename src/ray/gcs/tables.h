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

// TODO(pcm): Remove this
#include "task.h"

struct redisAsyncContext;

namespace ray {

namespace gcs {

class RedisContext;

class AsyncGcsClient;

template <typename ID, typename Data>
class Table {
 public:
  using DataT = typename Data::NativeTableType;
  using Callback =
      std::function<void(AsyncGcsClient *client, const ID &id, const DataT &data)>;
  /// The callback to call when a lookup fails because there is no entry at the
  /// key.
  using FailureCallback = std::function<void(AsyncGcsClient *client, const ID &id)>;
  /// The callback to call when a SUBSCRIBE call completes and we are ready to
  /// request and receive notifications.
  using SubscriptionCallback = std::function<void(AsyncGcsClient *client)>;

  struct CallbackData {
    ID id;
    std::shared_ptr<DataT> data;
    Callback callback;
    FailureCallback failure;
    // An optional callback to call for subscription operations, where the
    // first message is a notification of subscription success.
    SubscriptionCallback subscription_callback;
    Table<ID, Data> *table;
    AsyncGcsClient *client;
  };

  Table(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : context_(context),
        client_(client),
        pubsub_channel_(TablePubsub_NO_PUBLISH),
        prefix_(TablePrefix_UNUSED),
        subscribe_callback_index_(-1){};

  /// Add an entry to the table.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the data that is added to the GCS.
  /// \param data Data that is added to the GCS.
  /// \param done Callback that is called once the data has been written to the
  ///        GCS.
  /// \return Status
  Status Add(const JobID &job_id, const ID &id, std::shared_ptr<DataT> data,
             const Callback &done);

  /// Lookup an entry asynchronously.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param id The ID of the data that is looked up in the GCS.
  /// \param lookup Callback that is called after lookup. If the callback is
  ///        called with an empty vector, then there was no data at the key.
  /// \return Status
  Status Lookup(const JobID &job_id, const ID &id, const Callback &lookup,
                const FailureCallback &failure);

  /// Subscribe to any Add operations to this table. The caller may choose to
  /// subscribe to all Adds, or to subscribe only to keys that it requests
  /// notifications for. This may only be called once per Table instance.
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
  /// the current value(s) at the key, if any, and a subsequent notification
  /// will be published for every following `Add` to the key. Before
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

class ObjectTable : public Table<ObjectID, ObjectTableData> {
 public:
  ObjectTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : Table(context, client) {
    pubsub_channel_ = TablePubsub_OBJECT;
    prefix_ = TablePrefix_OBJECT;
  };
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

class TaskTable : public Table<TaskID, TaskTableData> {
 public:
  TaskTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client)
      : Table(context, client) {
    pubsub_channel_ = TablePubsub_TASK;
    prefix_ = TablePrefix_TASK;
  };

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
        [this, callback, id](const std::vector<std::string> &data) {
          RAY_CHECK(data.size() == 1);
          auto result = std::make_shared<TaskTableDataT>();
          auto root = flatbuffers::GetRoot<TaskTableData>(data[0].data());
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

using ErrorTable = Table<TaskID, ErrorTableData>;

using CustomSerializerTable = Table<ClassID, CustomSerializerData>;

using ConfigTable = Table<ConfigID, ConfigTableData>;

Status TaskTableAdd(AsyncGcsClient *gcs_client, Task *task);

Status TaskTableTestAndUpdate(AsyncGcsClient *gcs_client, const TaskID &task_id,
                              const ClientID &local_scheduler_id, int test_state_bitmask,
                              SchedulingState update_state,
                              const TaskTable::TestAndUpdateCallback &callback);

class ClientTable : private Table<ClientID, ClientTableData> {
 public:
  using ClientTableCallback = std::function<void(
      AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data)>;
  ClientTable(const std::shared_ptr<RedisContext> &context, AsyncGcsClient *client,
              const ClientTableDataT &local_client)
      : Table(context, client),
        disconnected_(false),
        client_id_(ClientID::from_binary(local_client.client_id)),
        local_client_(local_client) {
    pubsub_channel_ = TablePubsub_CLIENT;
    prefix_ = TablePrefix_CLIENT;

    // Add a nil client to the cache so that we can serve requests for clients
    // that we have not heard about.
    ClientTableDataT nil_client;
    nil_client.client_id = ClientID::nil().binary();
    client_cache_[ClientID::nil()] = nil_client;
  };

  /// Connect as a client to the GCS. This registers us in the client table
  /// and begins subscription to client table notifications.
  ///
  /// \return Status
  ray::Status Connect();

  /// Disconnect the client from the GCS. The client ID assigned during
  /// registration should never be reused after disconnecting.
  ///
  /// \return Status
  ray::Status Disconnect();

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
  void HandleNotification(AsyncGcsClient *client, const ClientID &channel_id,
                          const ClientTableDataT &notifications);
  /// Handle this client's successful connection to the GCS.
  void HandleConnected(AsyncGcsClient *client, const ClientID &client_id,
                       const ClientTableDataT &notifications);

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
