#ifndef RAY_GCS_REDIS_TASK_INFO_ACCESSOR_H
#define RAY_GCS_REDIS_TASK_INFO_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"
#include "ray/gcs/task_info_accessor.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class RedisTaskInfoAccessor
/// `RedisTaskInfoAccessor` is an implementation of `TaskInfoAccessor`
/// that uses Redis as the backend storage.
class RedisTaskInfoAccessor : public TaskInfoAccessor {
 public:
  explicit RedisTaskInfoAccessor(RedisGcsClient *client_impl);

  ~RedisTaskInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<TaskTableData> &data_ptr,
                  const StatusCallback &callback);

  Status AsyncGet(const TaskID &task_id,
                  const OptionalItemCallback<TaskTableData> &callback);

  Status AsyncDelete(const std::vector<TaskID> &task_ids, const StatusCallback &callback);

  Status AsyncSubscribe(const TaskID &task_id,
                        const SubscribeCallback<TaskID, TaskTableData> &subscribe,
                        const StatusCallback &done);

  Status AsyncUnsubscribe(const TaskID &task_id, const StatusCallback &done);

 private:
  RedisGcsClient *client_impl_{nullptr};
  // Use a random ClientID for task subscription. Because:
  // If we use ClientID::Nil, GCS will still send all tasks' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local ClientID, so we use
  // random ClientID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  ClientID subscribe_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<TaskID, TaskTableData, raylet::TaskTable>
      TaskSubscriptionExecutor;
  TaskSubscriptionExecutor task_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_TASK_INFO_ACCESSOR_H
