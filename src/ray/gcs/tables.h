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

struct redisAsyncContext;

namespace ray {

namespace gcs {

class RedisContext;

class AsyncGcsClient;

template <typename ID, typename Data>
class Table {
 public:
  using DataT = typename Data::NativeTableType;
  using Callback = std::function<
      void(AsyncGcsClient *client, const ID &id, std::shared_ptr<DataT> data)>;

  struct CallbackData {
    ID id;
    std::shared_ptr<DataT> data;
    Callback callback;
    Table<ID, Data> *table;
    AsyncGcsClient *client;
  };

  Table(const std::shared_ptr<RedisContext> &context) : context_(context){};

  /// Add an entry to the table
  Status Add(const JobID &job_id,
             const ID &id,
             std::shared_ptr<DataT> data,
             const Callback &done) {
    auto d =
        std::shared_ptr<CallbackData>(new CallbackData({id, data, done, this}));
    int64_t callback_index = RedisCallbackManager::instance().add([d](
        const std::string &data) { (d->callback)(d->client, d->id, d->data); });
    flatbuffers::FlatBufferBuilder fbb;
    fbb.Finish(Data::Pack(fbb, data.get()));
    RAY_RETURN_NOT_OK(context_->RunAsync("RAY.TABLE_ADD", id,
                                         fbb.GetBufferPointer(), fbb.GetSize(),
                                         callback_index));
    return Status::OK();
  }

  /// Lookup an entry asynchronously
  Status Lookup(const JobID &job_id,
                const ID &id,
                const Callback &lookup,
                const Callback &done) {
    auto d = std::shared_ptr<CallbackData>(
        new CallbackData({id, nullptr, done, this}));
    int64_t callback_index =
        RedisCallbackManager::instance().add([d](const std::string &data) {
          auto result = std::make_shared<DataT>();
          auto root = flatbuffers::GetRoot<Data>(data.data());
          root->UnPackTo(result.get());
          (d->callback)(d->client, d->id, result);
        });
    std::vector<uint8_t> nil;
    RAY_RETURN_NOT_OK(context_->RunAsync("RAY.TABLE_LOOKUP", id, nil.data(),
                                         nil.size(), callback_index));
    return Status::OK();
  }

  /// Subscribe to updates of this table
  Status Subscribe(const JobID &job_id,
                   const ID &id,
                   const Callback &subscribe,
                   const Callback &done);

  /// Remove and entry from the table
  Status Remove(const JobID &job_id, const ID &id, const Callback &done);

 private:
  std::unordered_map<ID, std::unique_ptr<CallbackData>, UniqueIDHasher>
      callback_data_;
  std::shared_ptr<RedisContext> context_;
};

class ObjectTable : public Table<ObjectID, ObjectTableData> {
 public:
  ObjectTable(const std::shared_ptr<RedisContext> &context) : Table(context){};

  /// Set up a client-specific channel for receiving notifications about
  /// available
  /// objects from the object table. The callback will be called once per
  /// notification received on this channel.
  ///
  /// @param subscribe_all
  /// @param object_available_callback Callback to be called when new object
  ///        becomes available.
  /// @param done_callback Callback to be called when subscription is installed.
  ///        This is only used for the tests.
  Status SubscribeToNotifications(const JobID &job_id,
                                  bool subscribe_all,
                                  const Callback &object_available,
                                  const Callback &done);

  /// Request notifications about the availability of some objects from the
  /// object
  /// table. The notifications will be published to this client's object
  /// notification channel, which was set up by the method
  /// ObjectTableSubscribeToNotifications.
  ///
  /// @param object_ids The object IDs to receive notifications about.
  Status RequestNotifications(const JobID &job_id,
                              const std::vector<ObjectID> &object_ids);
};

using FunctionTable = Table<FunctionID, FunctionTableData>;

using ClassTable = Table<ClassID, ClassTableData>;

using ActorTable = Table<ActorID, ActorTableData>;

class TaskTable : public Table<TaskID, TaskTableData> {
 public:
  TaskTable(const std::shared_ptr<RedisContext> &context) : Table(context){};

  using TestAndUpdateCallback =
      std::function<void(std::shared_ptr<TaskTableDataT> task)>;
  using SubscribeToTaskCallback =
      std::function<void(std::shared_ptr<TaskTableDataT> task)>;
  /// Update a task's scheduling information in the task table, if the current
  /// value matches the given test value. If the update succeeds, it also
  /// updates
  /// the task entry's local scheduler ID with the ID of the client who called
  /// this function. This assumes that the task spec already exists in the task
  /// table entry.
  ///
  /// @param task_id The task ID of the task entry to update.
  /// @param test_state_bitmask The bitmask to apply to the task entry's current
  ///        scheduling state.  The update happens if and only if the current
  ///        scheduling state AND-ed with the bitmask is greater than 0.
  /// @param update_state The value to update the task entry's scheduling state
  ///        with, if the current state matches test_state_bitmask.
  /// @param callback Function to be called when database returns result.
  Status TestAndUpdate(const JobID &job_id,
                       const TaskID &task_id,
                       int test_state_bitmask,
                       int updata_state,
                       const TaskTableData &data,
                       const TestAndUpdateCallback &callback);

  /// This has a separate signature from Subscribe in Table
  /// Register a callback for a task event. An event is any update of a task in
  /// the task table.
  /// Events include changes to the task's scheduling state or changes to the
  /// task's local scheduler ID.
  ///
  /// @param local_scheduler_id The db_client_id of the local scheduler whose
  ///        events we want to listen to. If you want to subscribe to updates
  ///        from
  ///        all local schedulers, pass in NIL_ID.
  /// @param subscribe_callback Callback that will be called when the task table
  /// is
  ///        updated.
  /// @param state_filter Events we want to listen to. Can have values from the
  ///        enum "scheduling_state" in task.h.
  ///        TODO(pcm): Make it possible to combine these using flags like
  ///        TASK_STATUS_WAITING | TASK_STATUS_SCHEDULED.
  /// @param callback Function to be called when database returns result.
  Status SubscribeToTask(const JobID &job_id,
                         const DBClientID &local_scheduler_id,
                         int state_filter,
                         const SubscribeToTaskCallback &callback,
                         const Callback &done);
};

using ErrorTable = Table<TaskID, ErrorTableData>;

using CustomSerializerTable = Table<ClassID, CustomSerializerData>;

using ConfigTable = Table<ConfigID, ConfigTableData>;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_TABLES_H
