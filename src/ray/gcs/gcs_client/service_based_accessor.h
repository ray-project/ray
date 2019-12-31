#ifndef RAY_GCS_SERVICE_BASED_ACCESSOR_H
#define RAY_GCS_SERVICE_BASED_ACCESSOR_H

#include "src/ray/common/id.h"
#include "src/ray/common/task/task_spec.h"
#include "src/ray/gcs/accessor.h"
#include "src/ray/gcs/callback.h"
#include "src/ray/gcs/subscription_executor.h"
#include "src/ray/gcs/tables.h"

namespace ray {

namespace gcs {

class ServiceBasedGcsClient;

/// \class ServiceBasedJobInfoAccessor
/// ServiceBasedJobInfoAccessor is an implementation of `JobInfoAccessor`
/// that uses GCS service as the backend storage.
class ServiceBasedJobInfoAccessor : public JobInfoAccessor {
 public:
  explicit ServiceBasedJobInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedJobInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) override;

  Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, JobTableData> &subscribe,
      const StatusCallback &done) override;

 private:
  ServiceBasedGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<JobID, JobTableData, JobTable> JobSubscriptionExecutor;
  JobSubscriptionExecutor job_sub_executor_;
};

/// \class ServiceBasedActorInfoAccessor
/// ServiceBasedActorInfoAccessor is an implementation of `ActorInfoAccessor`
/// that uses GCS service as the backend storage.
class ServiceBasedActorInfoAccessor : public ActorInfoAccessor {
 public:
  explicit ServiceBasedActorInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedActorInfoAccessor() {}

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<rpc::ActorTableData> &callback);

  Status AsyncRegister(const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                       const StatusCallback &callback);

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                     const StatusCallback &callback);

  Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done);

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                        const StatusCallback &done);

  Status AsyncUnsubscribe(const ActorID &actor_id, const StatusCallback &done);

 private:
  ServiceBasedGcsClient *client_impl_{nullptr};

  ClientID subscribe_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<ActorID, ActorTableData, ActorTable>
      ActorSubscriptionExecutor;
  ActorSubscriptionExecutor actor_sub_executor_;
};

/// \class ServiceBasedNodeInfoAccessor
/// ServiceBasedNodeInfoAccessor is an implementation of `NodeInfoAccessor`
/// that uses GCS service as the backend storage.
class ServiceBasedNodeInfoAccessor : public NodeInfoAccessor {
 public:
  explicit ServiceBasedNodeInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedNodeInfoAccessor() {}

  Status RegisterSelf(const GcsNodeInfo &local_node_info);

  Status UnregisterSelf();

  const ClientID &GetSelfId() const;

  const GcsNodeInfo &GetSelfInfo() const;

  Status Register(const GcsNodeInfo &node_info);

  Status AsyncUnregister(const ClientID &node_id, const StatusCallback &callback);

  Status AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback);

  Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
      const StatusCallback &done);

  boost::optional<GcsNodeInfo> Get(const ClientID &node_id) const;

  const std::unordered_map<ClientID, GcsNodeInfo> &GetAll() const;

  bool IsRemoved(const ClientID &node_id) const;

 private:
  ServiceBasedGcsClient *client_impl_{nullptr};

  GcsNodeInfo local_node_info;
  ClientID local_node_id;
};

/// \class ServiceBasedTaskInfoAccessor
/// ServiceBasedTaskInfoAccessor is an implementation of `TaskInfoAccessor`
/// that uses GCS service as the backend storage.
class ServiceBasedTaskInfoAccessor : public TaskInfoAccessor {
 public:
  explicit ServiceBasedTaskInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedTaskInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<rpc::TaskTableData> &data_ptr,
                  const StatusCallback &callback);

  Status AsyncGet(const TaskID &task_id,
                  const OptionalItemCallback<rpc::TaskTableData> &callback);

  Status AsyncDelete(const std::vector<TaskID> &task_ids, const StatusCallback &callback);

  Status AsyncSubscribe(const TaskID &task_id,
                        const SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe,
                        const StatusCallback &done);

  Status AsyncUnsubscribe(const TaskID &task_id, const StatusCallback &done);

 private:
  ServiceBasedGcsClient *client_impl_{nullptr};

  ClientID subscribe_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<TaskID, TaskTableData, raylet::TaskTable>
      TaskSubscriptionExecutor;
  TaskSubscriptionExecutor task_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_SERVICE_BASED_ACCESSOR_H