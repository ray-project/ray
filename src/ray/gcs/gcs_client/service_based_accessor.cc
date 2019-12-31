#include <boost/none.hpp>
#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/logging.h"

#include "ray/gcs/gcs_client/service_based_accessor.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"

namespace ray {

namespace gcs {

ServiceBasedJobInfoAccessor::ServiceBasedJobInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl),
      job_sub_executor_(client_impl->GetRedisGcsClient().job_table()) {}

Status ServiceBasedJobInfoAccessor::AsyncAdd(
    const std::shared_ptr<JobTableData> &data_ptr, const StatusCallback &callback) {
  rpc::AddJobRequest request;
  request.mutable_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().AddJob(
      request, [callback](const Status &status, const rpc::AddJobReply &reply) {
        callback(status);
      });
  return Status::OK();
}

Status ServiceBasedJobInfoAccessor::AsyncMarkFinished(const JobID &job_id,
                                                      const StatusCallback &callback) {
  rpc::MarkJobFinishedRequest request;
  request.set_job_id(job_id.Binary());
  client_impl_->GetGcsRpcClient().MarkJobFinished(
      request, [callback](const Status &status, const rpc::MarkJobFinishedReply &reply) {
        callback(status);
      });
  return Status::OK();
}

Status ServiceBasedJobInfoAccessor::AsyncSubscribeToFinishedJobs(
    const SubscribeCallback<JobID, JobTableData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const JobID &job_id, const JobTableData &job_data) {
    if (job_data.is_dead()) {
      subscribe(job_id, job_data);
    }
  };
  return job_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), on_subscribe, done);
}

ServiceBasedActorInfoAccessor::ServiceBasedActorInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl),
      actor_sub_executor_(client_impl->GetRedisGcsClient().actor_table()) {}

Status ServiceBasedActorInfoAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<rpc::ActorTableData> &callback) {
  rpc::GetActorInfoRequest request;
  request.set_actor_id(actor_id.Binary());
  client_impl_->GetGcsRpcClient().GetActorInfo(
      request, [callback](const Status &status, const rpc::GetActorInfoReply &reply) {
        rpc::ActorTableData actor_table_data;
        actor_table_data.CopyFrom(reply.actor_table_data());
        callback(status, actor_table_data);
      });
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncRegister(
    const std::shared_ptr<rpc::ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  rpc::RegisterActorInfoRequest request;
  request.mutable_actor_table_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().RegisterActorInfo(
      request,
      [callback](const Status &status, const rpc::RegisterActorInfoReply &reply) {
        callback(status);
      });
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncUpdate(
    const ActorID &actor_id, const std::shared_ptr<rpc::ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  rpc::UpdateActorInfoRequest request;
  request.set_actor_id(actor_id.Binary());
  request.mutable_actor_table_data()->CopyFrom(*data_ptr);
  client_impl_->GetGcsRpcClient().UpdateActorInfo(
      request, [callback](const Status &status, const rpc::UpdateActorInfoReply &reply) {
        callback(status);
      });
  return Status::OK();
}

Status ServiceBasedActorInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return actor_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), subscribe, done);
}

Status ServiceBasedActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id,
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return actor_sub_executor_.AsyncSubscribe(subscribe_id_, actor_id, subscribe, done);
}

Status ServiceBasedActorInfoAccessor::AsyncUnsubscribe(const ActorID &actor_id,
                                                       const StatusCallback &done) {
  return actor_sub_executor_.AsyncUnsubscribe(subscribe_id_, actor_id, done);
}

ServiceBasedTaskInfoAccessor::ServiceBasedTaskInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl),
      task_sub_executor_(client_impl->GetRedisGcsClient().raylet_task_table()) {}

Status ServiceBasedTaskInfoAccessor::AsyncAdd(
    const std::shared_ptr<rpc::TaskTableData> &data_ptr, const StatusCallback &callback) {
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncGet(
    const TaskID &task_id, const OptionalItemCallback<rpc::TaskTableData> &callback) {
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncDelete(const std::vector<TaskID> &task_ids,
                                                 const StatusCallback &callback) {
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncSubscribe(
    const TaskID &task_id, const SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe,
    const StatusCallback &done) {
  return Status::OK();
}

Status ServiceBasedTaskInfoAccessor::AsyncUnsubscribe(const TaskID &task_id,
                                                      const StatusCallback &done) {
  return Status::OK();
}

ServiceBasedNodeInfoAccessor::ServiceBasedNodeInfoAccessor(
    ServiceBasedGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status ServiceBasedNodeInfoAccessor::RegisterSelf(const GcsNodeInfo &local_node_info) {
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(local_node_info);
  this->local_node_info.CopyFrom(local_node_info);
  local_node_id = ClientID::FromBinary(local_node_info.node_id());
  std::promise<bool> promise;
  client_impl_->GetGcsRpcClient().RegisterNode(
      request, [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
        promise.set_value(true);
      });
  promise.get_future().get();
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::UnregisterSelf() {
  rpc::UnregisterNodeRequest request;
  request.set_node_id(local_node_info.node_id());
  std::promise<bool> promise;
  client_impl_->GetGcsRpcClient().UnregisterNode(
      request, [&promise](const Status &status, const rpc::UnregisterNodeReply &reply) {
        promise.set_value(true);
      });
  promise.get_future().get();
  return Status::OK();
}

const ClientID &ServiceBasedNodeInfoAccessor::GetSelfId() const { return local_node_id; }

const GcsNodeInfo &ServiceBasedNodeInfoAccessor::GetSelfInfo() const {
  return local_node_info;
}

Status ServiceBasedNodeInfoAccessor::Register(const GcsNodeInfo &node_info) {
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(node_info);
  std::promise<bool> promise;
  client_impl_->GetGcsRpcClient().RegisterNode(
      request, [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
        promise.set_value(true);
      });
  promise.get_future().get();
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncUnregister(const ClientID &node_id,
                                                     const StatusCallback &callback) {
  rpc::UnregisterNodeRequest request;
  request.set_node_id(node_id.Binary());
  client_impl_->GetGcsRpcClient().UnregisterNode(
      request, [callback](const Status &status, const rpc::UnregisterNodeReply &reply) {
        callback(status);
      });
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncGetAll(
    const MultiItemCallback<GcsNodeInfo> &callback) {
  rpc::GetAllNodeInfoRequest request;
  client_impl_->GetGcsRpcClient().GetAllNodeInfo(
      request, [callback](const Status &status, const rpc::GetAllNodeInfoReply &reply) {
        std::vector<GcsNodeInfo> result;
        for (int index = 0; index < reply.node_info_list_size(); ++index) {
          result.emplace_back(reply.node_info_list(index));
        }
        callback(status, result);
      });
  return Status::OK();
}

Status ServiceBasedNodeInfoAccessor::AsyncSubscribeToNodeChange(
    const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  ClientTable &client_table = client_impl_->GetRedisGcsClient().client_table();
  return client_table.SubscribeToNodeChange(subscribe, done);
}

boost::optional<GcsNodeInfo> ServiceBasedNodeInfoAccessor::Get(
    const ClientID &node_id) const {
  GcsNodeInfo node_info;
  ClientTable &client_table = client_impl_->GetRedisGcsClient().client_table();
  bool found = client_table.GetClient(node_id, &node_info);
  boost::optional<GcsNodeInfo> optional_node;
  if (found) {
    optional_node = std::move(node_info);
  }
  return optional_node;
}

const std::unordered_map<ClientID, GcsNodeInfo> &ServiceBasedNodeInfoAccessor::GetAll()
    const {
  ClientTable &client_table = client_impl_->GetRedisGcsClient().client_table();
  return client_table.GetAllClients();
}

bool ServiceBasedNodeInfoAccessor::IsRemoved(const ClientID &node_id) const {
  ClientTable &client_table = client_impl_->GetRedisGcsClient().client_table();
  return client_table.IsRemoved(node_id);
}

}  // namespace gcs

}  // namespace ray
