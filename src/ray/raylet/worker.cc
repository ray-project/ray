#include "worker.h"

#include <boost/bind.hpp>

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/raylet.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

namespace raylet {

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(const WorkerID &worker_id, pid_t pid, const Language &language, int port,
               std::shared_ptr<LocalClientConnection> connection,
               rpc::ClientCallManager &client_call_manager)
    : worker_id_(worker_id),
      pid_(pid),
      language_(language),
      port_(port),
      connection_(connection),
      dead_(false),
      blocked_(false),
      client_call_manager_(client_call_manager),
      is_detached_actor_(false) {
  if (port_ > 0) {
    rpc_client_ = std::unique_ptr<rpc::CoreWorkerClient>(
        new rpc::CoreWorkerClient("127.0.0.1", port_, client_call_manager_));
  }
}

void Worker::MarkDead() { dead_ = true; }

bool Worker::IsDead() const { return dead_; }

void Worker::MarkBlocked() { blocked_ = true; }

void Worker::MarkUnblocked() { blocked_ = false; }

bool Worker::IsBlocked() const { return blocked_; }

WorkerID Worker::WorkerId() const { return worker_id_; }

pid_t Worker::Pid() const { return pid_; }

Language Worker::GetLanguage() const { return language_; }

int Worker::Port() const { return port_; }

void Worker::AssignTaskId(const TaskID &task_id) { assigned_task_id_ = task_id; }

const TaskID &Worker::GetAssignedTaskId() const { return assigned_task_id_; }

bool Worker::AddBlockedTaskId(const TaskID &task_id) {
  auto inserted = blocked_task_ids_.insert(task_id);
  return inserted.second;
}

bool Worker::RemoveBlockedTaskId(const TaskID &task_id) {
  auto erased = blocked_task_ids_.erase(task_id);
  return erased == 1;
}

const std::unordered_set<TaskID> &Worker::GetBlockedTaskIds() const {
  return blocked_task_ids_;
}

void Worker::AssignJobId(const JobID &job_id) { assigned_job_id_ = job_id; }

const JobID &Worker::GetAssignedJobId() const { return assigned_job_id_; }

void Worker::AssignActorId(const ActorID &actor_id) {
  RAY_CHECK(actor_id_.IsNil())
      << "A worker that is already an actor cannot be assigned an actor ID again.";
  RAY_CHECK(!actor_id.IsNil());
  actor_id_ = actor_id;
}

const ActorID &Worker::GetActorId() const { return actor_id_; }

void Worker::MarkDetachedActor() { is_detached_actor_ = true; }

bool Worker::IsDetachedActor() const { return is_detached_actor_; }

const std::shared_ptr<LocalClientConnection> Worker::Connection() const {
  return connection_;
}

const ResourceIdSet &Worker::GetLifetimeResourceIds() const {
  return lifetime_resource_ids_;
}

void Worker::ResetLifetimeResourceIds() { lifetime_resource_ids_.Clear(); }

void Worker::SetLifetimeResourceIds(ResourceIdSet &resource_ids) {
  lifetime_resource_ids_ = resource_ids;
}

const ResourceIdSet &Worker::GetTaskResourceIds() const { return task_resource_ids_; }

void Worker::ResetTaskResourceIds() { task_resource_ids_.Clear(); }

void Worker::SetTaskResourceIds(ResourceIdSet &resource_ids) {
  task_resource_ids_ = resource_ids;
}

ResourceIdSet Worker::ReleaseTaskCpuResources() {
  auto cpu_resources = task_resource_ids_.GetCpuResources();
  // The "acquire" terminology is a bit confusing here. The resources are being
  // "acquired" from the task_resource_ids_ object, and so the worker is losing
  // some resources.
  task_resource_ids_.Acquire(cpu_resources.ToResourceSet());
  return cpu_resources;
}

void Worker::AcquireTaskCpuResources(const ResourceIdSet &cpu_resources) {
  // The "release" terminology is a bit confusing here. The resources are being
  // given back to the worker and so "released" by the caller.
  task_resource_ids_.Release(cpu_resources);
}

const std::unordered_set<ObjectID> &Worker::GetActiveObjectIds() const {
  return active_object_ids_;
}

void Worker::SetActiveObjectIds(const std::unordered_set<ObjectID> &&object_ids) {
  active_object_ids_ = object_ids;
}

Status Worker::AssignTask(const Task &task, const ResourceIdSet &resource_id_set) {
  RAY_CHECK(port_ > 0);
  rpc::AssignTaskRequest request;
  request.set_intended_worker_id(worker_id_.Binary());
  request.mutable_task()->mutable_task_spec()->CopyFrom(
      task.GetTaskSpecification().GetMessage());
  request.mutable_task()->mutable_task_execution_spec()->CopyFrom(
      task.GetTaskExecutionSpec().GetMessage());
  request.set_resource_ids(resource_id_set.Serialize());

  return rpc_client_->AssignTask(request, [](Status status,
                                             const rpc::AssignTaskReply &reply) {
    if (!status.ok()) {
      RAY_LOG(DEBUG) << "Worker failed to finish executing task: " << status.ToString();
    }
    // Worker has finished this task. There's nothing to do here
    // and assigning new task will be done when raylet receives
    // `TaskDone` message.
  });
}

void Worker::DirectActorCallArgWaitComplete(int64_t tag) {
  RAY_CHECK(port_ > 0);
  rpc::DirectActorCallArgWaitCompleteRequest request;
  request.set_tag(tag);
  request.set_intended_worker_id(worker_id_.Binary());
  auto status = rpc_client_->DirectActorCallArgWaitComplete(
      request, [](Status status, const rpc::DirectActorCallArgWaitCompleteReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to send wait complete: " << status.ToString();
        }
      });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to send wait complete: " << status.ToString();
  }
}

}  // namespace raylet

}  // end namespace ray
