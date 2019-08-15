#include "worker.h"

#include <boost/bind.hpp>

#include "ray/raylet/raylet.h"

namespace ray {

namespace raylet {

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(const WorkerID &worker_id, pid_t pid, const Language &language, int port,
               rpc::ClientCallManager &client_call_manager, bool is_worker)
    : worker_id_(worker_id),
      pid_(pid),
      port_(port),
      language_(language),
      blocked_(false),
      num_missed_heartbeats_(0),
      is_being_killed_(false),
      client_call_manager_(client_call_manager),
      is_worker_(is_worker) {
  if (port_ > 0) {
    rpc_client_ = std::unique_ptr<rpc::WorkerTaskClient>(
        new rpc::WorkerTaskClient("127.0.0.1", port_, client_call_manager_));
  }
}

void Worker::MarkAsBeingKilled() { is_being_killed_ = true; }

bool Worker::IsBeingKilled() const { return is_being_killed_; }

bool Worker::IsWorker() const { return is_worker_; }

void Worker::MarkBlocked() { blocked_ = true; }

void Worker::MarkUnblocked() { blocked_ = false; }

bool Worker::IsBlocked() const { return blocked_; }

WorkerID Worker::WorkerId() const { return worker_id_; }

pid_t Worker::Pid() const { return pid_; }

Language Worker::GetLanguage() const { return language_; }

const WorkerID &Worker::GetWorkerId() const { return worker_id_; }

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

void Worker::SetGetTaskReplyAndCallback(
    rpc::GetTaskReply *reply, const rpc::SendReplyCallback &&send_reply_callback) {
  RAY_CHECK(reply_ == nullptr && send_reply_callback_ == nullptr);
  reply_ = reply;
  send_reply_callback_ = std::move(send_reply_callback);
}

bool Worker::UsePush() const { return rpc_client_ != nullptr; }

void Worker::AssignTask(const Task &task, const ResourceIdSet &resource_id_set) {
  const TaskSpecification &spec = task.GetTaskSpecification();
  if (rpc_client_ != nullptr) {
    // Use push mode.
    RAY_CHECK(port_ > 0);
    rpc::AssignTaskRequest request;
    request.mutable_task()->mutable_task_spec()->CopyFrom(
        task.GetTaskSpecification().GetMessage());
    request.mutable_task()->mutable_task_execution_spec()->CopyFrom(
        task.GetTaskExecutionSpec().GetMessage());
    for (const auto &e : resource_id_set.ToProtobuf()) {
      auto resource = request.add_resource_ids();
      *resource = e;
    }
    auto status = rpc_client_->AssignTask(
        request, [](Status status, const rpc::AssignTaskReply &reply) {
          // Worker has finished this task. There's nothing to do here
          // and assigning new task will be done when raylet receives
          // `TaskDone` message.
        });
  } else {
    // Use pull mode. This corresponds to existing python/java workers that haven't been
    // migrated to core worker architecture.
    RAY_CHECK(reply_ != nullptr && send_reply_callback_ != nullptr);
    reply_->set_task_spec(task.GetTaskSpecification().Serialize());
    for (const auto &e : resource_id_set.ToProtobuf()) {
      auto resource = reply_->add_fractional_resource_ids();
      *resource = e;
    }
    send_reply_callback_(Status::OK(), nullptr, nullptr);
    reply_ = nullptr;
    send_reply_callback_ = nullptr;
  }
  // The status will be cleared when the worker dies.
  AssignTaskId(spec.TaskId());
  AssignJobId(spec.JobId());
}

}  // namespace raylet

}  // end namespace ray
