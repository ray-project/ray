#ifndef RAY_GCS_REDIS_ACCESSOR_H
#define RAY_GCS_REDIS_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

std::shared_ptr<gcs::ActorTableData> CreateActorTableData(
    const TaskSpecification &task_spec, const rpc::Address &address,
    gcs::ActorTableData::ActorState state, uint64_t remaining_reconstructions);

/// \class RedisActorInfoAccessor
/// `RedisActorInfoAccessor` is an implementation of `ActorInfoAccessor`
/// that uses Redis as the backend storage.
class RedisActorInfoAccessor : public ActorInfoAccessor {
 public:
  explicit RedisActorInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisActorInfoAccessor() {}

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<ActorTableData> &callback) override;

  Status AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                       const StatusCallback &callback) override;

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<ActorTableData> &data_ptr,
                     const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                           const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const ActorID &actor_id, const StatusCallback &done) override;

 private:
  RedisGcsClient *client_impl_{nullptr};
  // Use a random ClientID for actor subscription. Because:
  // If we use ClientID::Nil, GCS will still send all actors' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local ClientID, so we use
  // random ClientID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  ClientID node_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<ActorID, ActorTableData, ActorTable>
      ActorSubscriptionExecutor;
  ActorSubscriptionExecutor actor_sub_executor_;
};

/// \class RedisJobInfoAccessor
/// RedisJobInfoAccessor is an implementation of `JobInfoAccessor`
/// that uses Redis as the backend storage.
class RedisJobInfoAccessor : public JobInfoAccessor {
 public:
  explicit RedisJobInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisJobInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) override;

  Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, JobTableData> &subscribe,
      const StatusCallback &done) override;

 private:
  /// Append job information to GCS asynchronously.
  ///
  /// \param data_ptr The job information that will be appended to GCS.
  /// \param callback Callback that will be called after append done.
  /// \return Status
  Status DoAsyncAppend(const std::shared_ptr<JobTableData> &data_ptr,
                       const StatusCallback &callback);

  RedisGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<JobID, JobTableData, JobTable> JobSubscriptionExecutor;
  JobSubscriptionExecutor job_sub_executor_;
};

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

/// \class RedisNodeInfoAccessor
/// RedisNodeInfoAccessor is an implementation of `NodeInfoAccessor`
/// that uses Redis as the backend storage.
class RedisNodeInfoAccessor : public NodeInfoAccessor {
 public:
  explicit RedisNodeInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisNodeInfoAccessor() {}

  Status RegisterSelf(const GcsNodeInfo &local_node_info) override;

  Status UnregisterSelf() override;

  const ClientID &GetSelfId() const override;

  const GcsNodeInfo &GetSelfInfo() const override;

  Status AsyncUnregister(const ClientID &node_id,
                         const StatusCallback &callback) override;

  Status AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback) override;

  Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
      const StatusCallback &done) override;

  boost::optional<GcsNodeInfo> Get(const ClientID &node_id) const override;

  const std::unordered_map<ClientID, GcsNodeInfo> &GetAll() const override;

  bool IsRemoved(const ClientID &node_id) const override;

 private:
  RedisGcsClient *client_impl_{nullptr};
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_ACCESSOR_H