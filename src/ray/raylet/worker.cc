#include "worker.h"

#include <boost/bind.hpp>

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/raylet.h"

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
      client_call_manager_(client_call_manager) {
  if (port_ > 0) {
    rpc_client_ = std::unique_ptr<rpc::WorkerTaskClient>(
        new rpc::WorkerTaskClient("127.0.0.1", port_, client_call_manager_));
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

bool Worker::UsePush() const { return rpc_client_ != nullptr; }

void Worker::AssignTask(const Task &task, const ResourceIdSet &resource_id_set,
                        const std::function<void(Status)> finish_assign_callback) {
  const TaskSpecification &spec = task.GetTaskSpecification();
  if (rpc_client_ != nullptr) {
    // Use push mode.
    RAY_CHECK(port_ > 0);
    rpc::AssignTaskRequest request;
    request.mutable_task()->mutable_task_spec()->CopyFrom(
        task.GetTaskSpecification().GetMessage());
    request.mutable_task()->mutable_task_execution_spec()->CopyFrom(
        task.GetTaskExecutionSpec().GetMessage());
    request.set_resource_ids(resource_id_set.Serialize());

    auto status = rpc_client_->AssignTask(request, [](Status status,
                                                      const rpc::AssignTaskReply &reply) {
      if (!status.ok()) {
        RAY_LOG(ERROR) << "Worker failed to finish executing task: " << status.ToString();
      }
      // Worker has finished this task. There's nothing to do here
      // and assigning new task will be done when raylet receives
      // `TaskDone` message.
    });
    finish_assign_callback(status);
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to assign task " << task.GetTaskSpecification().TaskId()
                     << " to worker " << worker_id_;
    } else {
      RAY_LOG(DEBUG) << "Assigned task " << task.GetTaskSpecification().TaskId()
                     << " to worker " << worker_id_;
    }
  } else {
    // Use pull mode. This corresponds to existing python/java workers that haven't been
    // migrated to core worker architecture.
    flatbuffers::FlatBufferBuilder fbb;
    auto resource_id_set_flatbuf = resource_id_set.ToFlatbuf(fbb);

    auto message =
        protocol::CreateGetTaskReply(fbb, fbb.CreateString(spec.Serialize()),
                                     fbb.CreateVector(resource_id_set_flatbuf));
    fbb.Finish(message);
    Connection()->WriteMessageAsync(
        static_cast<int64_t>(protocol::MessageType::ExecuteTask), fbb.GetSize(),
        fbb.GetBufferPointer(), finish_assign_callback);
  }
}

}  // namespace raylet

}  // end namespace ray
