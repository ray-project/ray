// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/task_submission/actor_task_submitter.h"

#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/protobuf_utils.h"
#include "ray/core_worker/task_submission/task_submission_util.h"
#include "ray/util/time.h"

namespace ray {
namespace core {

void ActorTaskSubmitter::NotifyGCSWhenActorOutOfScope(
    const ActorID &actor_id, uint64_t num_restarts_due_to_lineage_reconstruction) {
  const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);
  auto actor_out_of_scope_callback = [this,
                                      actor_id,
                                      num_restarts_due_to_lineage_reconstruction](
                                         const ObjectID &object_id) {
    {
      absl::MutexLock lock(&mu_);
      if (auto iter = client_queues_.find(actor_id); iter != client_queues_.end()) {
        if (iter->second.state_ != rpc::ActorTableData::DEAD) {
          iter->second.pending_out_of_scope_death_ = true;
        }
      }
    }
    actor_creator_.AsyncReportActorOutOfScope(
        actor_id, num_restarts_due_to_lineage_reconstruction, [actor_id](Status status) {
          if (!status.ok()) {
            RAY_LOG(ERROR).WithField(actor_id)
                << "Failed to report actor out of scope: " << status
                << ". The actor will not be killed";
          }
        });
  };

  if (!reference_counter_->AddObjectOutOfScopeOrFreedCallback(
          actor_creation_return_id,
          [actor_out_of_scope_callback](const ObjectID &object_id) {
            actor_out_of_scope_callback(object_id);
          })) {
    RAY_LOG(DEBUG).WithField(actor_id) << "Actor already out of scope";
    actor_out_of_scope_callback(actor_creation_return_id);
  }
}

void ActorTaskSubmitter::AddActorQueueIfNotExists(const ActorID &actor_id,
                                                  int32_t max_pending_calls,
                                                  bool allow_out_of_order_execution,
                                                  bool fail_if_actor_unreachable,
                                                  bool owned) {
  bool inserted;
  {
    absl::MutexLock lock(&mu_);
    // No need to check whether the insert was successful, since it is possible
    // for this worker to have multiple references to the same actor.
    RAY_LOG(INFO).WithField(actor_id)
        << "Set actor max pending calls to " << max_pending_calls;
    inserted = client_queues_
                   .emplace(actor_id,
                            ClientQueue(allow_out_of_order_execution,
                                        max_pending_calls,
                                        fail_if_actor_unreachable,
                                        owned))
                   .second;
  }
  if (owned && inserted) {
    // Actor owner is responsible for notifying GCS when the
    // actor is out of scope so that GCS can kill the actor.
    NotifyGCSWhenActorOutOfScope(actor_id,
                                 /*num_restarts_due_to_lineage_reconstruction*/ 0);
  }
}

void ActorTaskSubmitter::SubmitActorCreationTask(TaskSpecification task_spec) {
  RAY_CHECK(task_spec.IsActorCreationTask());
  RAY_LOG(DEBUG).WithField(task_spec.ActorCreationId()).WithField(task_spec.TaskId())
      << "Submitting actor creation task";
  resolver_.ResolveDependencies(task_spec, [this, task_spec](Status status) mutable {
    // NOTE: task_spec here is capture copied (from a stack variable) and also
    // mutable. (Mutations to the variable are expected to be shared inside and
    // outside of this closure).
    const auto actor_id = task_spec.ActorCreationId();
    const auto task_id = task_spec.TaskId();
    task_manager_.MarkDependenciesResolved(task_id);
    if (!status.ok()) {
      RAY_LOG(WARNING).WithField(actor_id).WithField(task_id)
          << "Resolving actor creation task dependencies failed " << status;
      RAY_UNUSED(task_manager_.FailOrRetryPendingTask(
          task_id, rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, &status));
      return;
    }
    RAY_LOG(DEBUG).WithField(actor_id).WithField(task_id)
        << "Actor creation task dependencies resolved";
    // The actor creation task will be sent to
    // gcs server directly after the in-memory dependent objects are resolved. For
    // more details please see the protocol of actor management based on gcs.
    // https://docs.google.com/document/d/1EAWide-jy05akJp6OMtDn58XOK7bUyruWMia4E-fV28/edit?usp=sharing
    RAY_LOG(DEBUG).WithField(actor_id).WithField(task_id) << "Creating actor via GCS";
    actor_creator_.AsyncCreateActor(
        task_spec,
        [this, actor_id, task_id](Status create_actor_status,
                                  const rpc::CreateActorReply &reply) {
          if (create_actor_status.ok() || create_actor_status.IsCreationTaskError()) {
            rpc::PushTaskReply push_task_reply;
            push_task_reply.mutable_borrowed_refs()->CopyFrom(reply.borrowed_refs());
            if (create_actor_status.IsCreationTaskError()) {
              RAY_LOG(INFO).WithField(actor_id).WithField(task_id)
                  << "Actor creation failed and we will not be retrying the "
                     "creation task";
              // Update the task execution error to be CreationTaskError.
              push_task_reply.set_task_execution_error(create_actor_status.ToString());
            } else {
              RAY_LOG(DEBUG).WithField(actor_id).WithField(task_id) << "Created actor";
            }
            // NOTE: When actor creation task failed we will not retry the creation
            // task so just marking the task fails.
            task_manager_.CompletePendingTask(
                task_id,
                push_task_reply,
                reply.actor_address(),
                /*is_application_error=*/create_actor_status.IsCreationTaskError());
          } else {
            // Either fails the rpc call or actor scheduling cancelled.
            rpc::RayErrorInfo ray_error_info;
            if (create_actor_status.IsSchedulingCancelled()) {
              RAY_LOG(DEBUG).WithField(actor_id).WithField(task_id)
                  << "Actor creation cancelled";
              task_manager_.MarkTaskNoRetry(task_id);
              if (reply.has_death_cause()) {
                ray_error_info.mutable_actor_died_error()->CopyFrom(reply.death_cause());
              }
            } else {
              RAY_LOG(INFO).WithField(actor_id).WithField(task_id)
                  << "Failed to create actor with status: " << create_actor_status;
            }
            // Actor creation task retry happens in GCS
            // and transient rpc errors are retried in gcs client
            // so we don't need to retry here.
            RAY_UNUSED(task_manager_.FailPendingTask(
                task_id,
                rpc::ErrorType::ACTOR_CREATION_FAILED,
                &create_actor_status,
                ray_error_info.has_actor_died_error() ? &ray_error_info : nullptr));
          }
        });
  });
}

void ActorTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  auto task_id = task_spec.TaskId();
  auto actor_id = task_spec.ActorId();
  RAY_LOG(DEBUG).WithField(task_id) << "Submitting task";
  RAY_CHECK(task_spec.IsActorTask());

  bool task_queued = false;
  uint64_t send_pos = 0;
  {
    // We must release mu_ before resolving the task dependencies since the callback that
    // reacquires mu_ may get called in the same call stack.
    absl::MutexLock lock(&mu_);
    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (queue->second.state_ == rpc::ActorTableData::DEAD &&
        queue->second.is_restartable_ && queue->second.owned_) {
      RestartActorForLineageReconstruction(actor_id);
    }
    if (queue->second.state_ != rpc::ActorTableData::DEAD) {
      // We must fix the send order prior to resolving dependencies, which may
      // complete out of order. This ensures that we will not deadlock due to
      // backpressure. The receiving actor will execute the tasks according to
      // this sequence number.
      send_pos = task_spec.SequenceNumber();
      queue->second.actor_submit_queue_->Emplace(send_pos, task_spec);
      queue->second.cur_pending_calls_++;
      task_queued = true;
    }
  }

  if (task_queued) {
    {
      absl::MutexLock resolver_lock(&resolver_mu_);
      pending_dependency_resolution_.insert(task_id);
    }
    io_service_.post(
        [task_spec, task_id, actor_id, send_pos, this]() mutable {
          {
            absl::MutexLock resolver_lock(&resolver_mu_);
            if (pending_dependency_resolution_.erase(task_id) == 0) {
              return;
            }
            resolver_.ResolveDependencies(
                task_spec, [this, send_pos, actor_id, task_id](Status status) {
                  task_manager_.MarkDependenciesResolved(task_id);
                  bool fail_or_retry_task = false;
                  {
                    absl::MutexLock lock(&mu_);
                    auto queue = client_queues_.find(actor_id);
                    RAY_CHECK(queue != client_queues_.end());
                    auto &actor_submit_queue = queue->second.actor_submit_queue_;
                    // Only dispatch tasks if the submitted task is still queued. The task
                    // may have been dequeued if the actor has since failed.
                    if (actor_submit_queue->Contains(send_pos)) {
                      if (status.ok()) {
                        actor_submit_queue->MarkDependencyResolved(send_pos);
                        SendPendingTasks(actor_id);
                      } else {
                        fail_or_retry_task = true;
                        actor_submit_queue->MarkDependencyFailed(send_pos);
                      }
                    }
                  }

                  if (fail_or_retry_task) {
                    task_manager_.FailOrRetryPendingTask(
                        task_id, rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, &status);
                  }
                });
          }
        },
        "ActorTaskSubmitter::SubmitTask");
  } else {
    // Do not hold the lock while calling into task_manager_.
    task_manager_.MarkTaskNoRetry(task_id);
    rpc::ErrorType error_type;
    rpc::RayErrorInfo error_info;
    {
      absl::MutexLock lock(&mu_);
      const auto queue_it = client_queues_.find(task_spec.ActorId());
      const auto &death_cause = queue_it->second.death_cause_;
      error_info = gcs::GetErrorInfoFromActorDeathCause(death_cause);
      error_type = error_info.error_type();
    }
    auto status = Status::IOError("cancelling task of dead actor");
    // No need to increment the number of completed tasks since the actor is
    // dead.
    bool fail_immediately =
        error_info.has_actor_died_error() &&
        error_info.actor_died_error().has_oom_context() &&
        error_info.actor_died_error().oom_context().fail_immediately();
    task_manager_.FailOrRetryPendingTask(task_id,
                                         error_type,
                                         &status,
                                         &error_info,
                                         /*mark_task_object_failed*/ true,
                                         fail_immediately);
  }
}

void ActorTaskSubmitter::CancelDependencyResolution(const TaskID &task_id) {
  absl::MutexLock resolver_lock(&resolver_mu_);
  pending_dependency_resolution_.erase(task_id);
  RAY_UNUSED(resolver_.CancelDependencyResolution(task_id));
}

void ActorTaskSubmitter::DisconnectRpcClient(ClientQueue &queue) {
  queue.client_address_ = std::nullopt;
  // If the actor on the worker is dead, the worker is also dead.
  core_worker_client_pool_.Disconnect(WorkerID::FromBinary(queue.worker_id_));
  queue.worker_id_.clear();
}

void ActorTaskSubmitter::FailInflightTasksOnRestart(
    const absl::flat_hash_map<TaskAttempt, rpc::ClientCallback<rpc::PushTaskReply>>
        &inflight_task_callbacks) {
  // NOTE(kfstorm): We invoke the callbacks with a bad status to act like there's a
  // network issue. We don't call `task_manager_.FailOrRetryPendingTask` directly
  // because there's much more work to do in the callback.
  auto status = Status::IOError("The actor was restarted");
  for (const auto &[_, callback] : inflight_task_callbacks) {
    callback(status, rpc::PushTaskReply());
  }
}

void ActorTaskSubmitter::ConnectActor(const ActorID &actor_id,
                                      const rpc::Address &address,
                                      int64_t num_restarts) {
  RAY_LOG(DEBUG).WithField(actor_id).WithField(WorkerID::FromBinary(address.worker_id()))
      << "Connecting to actor";

  absl::flat_hash_map<TaskAttempt, rpc::ClientCallback<rpc::PushTaskReply>>
      inflight_task_callbacks;

  {
    absl::MutexLock lock(&mu_);

    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (num_restarts < queue->second.num_restarts_) {
      // This message is about an old version of the actor and the actor has
      // already restarted since then. Skip the connection.
      RAY_LOG(INFO).WithField(actor_id)
          << "Skip actor connection that has already been restarted";
      return;
    }

    if (queue->second.client_address_.has_value() &&
        queue->second.client_address_->ip_address() == address.ip_address() &&
        queue->second.client_address_->port() == address.port()) {
      RAY_LOG(DEBUG).WithField(actor_id) << "Skip actor that has already been connected";
      return;
    }

    if (queue->second.state_ == rpc::ActorTableData::DEAD) {
      // This message is about an old version of the actor and the actor has
      // already died since then. Skip the connection.
      return;
    }

    queue->second.num_restarts_ = num_restarts;
    if (queue->second.client_address_.has_value()) {
      // Clear the client to the old version of the actor.
      DisconnectRpcClient(queue->second);
      inflight_task_callbacks = std::move(queue->second.inflight_task_callbacks_);
      queue->second.inflight_task_callbacks_.clear();
    }

    queue->second.state_ = rpc::ActorTableData::ALIVE;
    // So new RPCs go out with the right intended worker id to the right address.
    queue->second.worker_id_ = address.worker_id();
    queue->second.client_address_ = address;

    SendPendingTasks(actor_id);
  }

  // NOTE(kfstorm): We need to make sure the lock is released before invoking callbacks.
  FailInflightTasksOnRestart(inflight_task_callbacks);
}

void ActorTaskSubmitter::RestartActorForLineageReconstruction(const ActorID &actor_id) {
  RAY_LOG(INFO).WithField(actor_id) << "Reconstructing actor";
  auto queue = client_queues_.find(actor_id);
  RAY_CHECK(queue != client_queues_.end());
  RAY_CHECK(queue->second.owned_) << "Only owner can restart the dead actor";
  RAY_CHECK(queue->second.is_restartable_) << "This actor is no longer restartable";
  queue->second.state_ = rpc::ActorTableData::RESTARTING;
  queue->second.num_restarts_due_to_lineage_reconstructions_ += 1;
  actor_creator_.AsyncRestartActorForLineageReconstruction(
      actor_id,
      queue->second.num_restarts_due_to_lineage_reconstructions_,
      [this,
       actor_id,
       num_restarts_due_to_lineage_reconstructions =
           queue->second.num_restarts_due_to_lineage_reconstructions_](Status status) {
        if (!status.ok()) {
          RAY_LOG(ERROR).WithField(actor_id)
              << "Failed to reconstruct actor. Error message: " << status.ToString();
        } else {
          // Notify GCS when the actor is out of scope again.
          NotifyGCSWhenActorOutOfScope(actor_id,
                                       num_restarts_due_to_lineage_reconstructions);
        }
      });
}

void ActorTaskSubmitter::DisconnectActor(const ActorID &actor_id,
                                         int64_t num_restarts,
                                         bool dead,
                                         const rpc::ActorDeathCause &death_cause,
                                         bool is_restartable) {
  RAY_LOG(DEBUG).WithField(actor_id) << "Disconnecting from actor, death context type="
                                     << gcs::GetActorDeathCauseString(death_cause);

  absl::flat_hash_map<TaskAttempt, rpc::ClientCallback<rpc::PushTaskReply>>
      inflight_task_callbacks;
  std::deque<std::shared_ptr<PendingTaskWaitingForDeathInfo>> wait_for_death_info_tasks;
  std::vector<TaskID> task_ids_to_fail;
  {
    absl::MutexLock lock(&mu_);
    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (!dead) {
      RAY_CHECK_GT(num_restarts, 0);
    }
    if (num_restarts <= queue->second.num_restarts_ && !dead) {
      // This message is about an old version of the actor that has already been
      // restarted successfully. Skip the message handling.
      RAY_LOG(INFO).WithField(actor_id)
          << "Skip actor disconnection that has already been restarted";
      return;
    }

    // The actor failed, so erase the client for now. Either the actor is
    // permanently dead or the new client will be inserted once the actor is
    // restarted.
    DisconnectRpcClient(queue->second);
    inflight_task_callbacks = std::move(queue->second.inflight_task_callbacks_);
    queue->second.inflight_task_callbacks_.clear();

    if (dead) {
      queue->second.state_ = rpc::ActorTableData::DEAD;
      queue->second.death_cause_ = death_cause;
      queue->second.pending_out_of_scope_death_ = false;
      queue->second.is_restartable_ = is_restartable;

      if (queue->second.is_restartable_ && queue->second.owned_) {
        // Actor is out of scope so there should be no inflight actor tasks.
        RAY_CHECK(queue->second.wait_for_death_info_tasks_.empty());
        RAY_CHECK(inflight_task_callbacks.empty());
        if (!queue->second.actor_submit_queue_->Empty()) {
          // There are pending lineage reconstruction tasks.
          RestartActorForLineageReconstruction(actor_id);
        }
      } else {
        // If there are pending requests, treat the pending tasks as failed.
        RAY_LOG(INFO).WithField(actor_id)
            << "Failing pending tasks for actor because the actor is already dead.";

        task_ids_to_fail = queue->second.actor_submit_queue_->ClearAllTasks();
        // We need to execute this outside of the lock to prevent deadlock.
        wait_for_death_info_tasks = std::move(queue->second.wait_for_death_info_tasks_);
        // Reset the queue
        queue->second.wait_for_death_info_tasks_ =
            std::deque<std::shared_ptr<PendingTaskWaitingForDeathInfo>>();
      }
    } else if (queue->second.state_ != rpc::ActorTableData::DEAD) {
      // Only update the actor's state if it is not permanently dead. The actor
      // will eventually get restarted or marked as permanently dead.
      queue->second.state_ = rpc::ActorTableData::RESTARTING;
      queue->second.num_restarts_ = num_restarts;
    }
  }

  if (task_ids_to_fail.size() + wait_for_death_info_tasks.size() != 0) {
    // Failing tasks has to be done without mu_ hold because the callback
    // might require holding mu_ which will lead to a deadlock.
    auto status = Status::IOError("cancelling all pending tasks of dead actor");
    const auto error_info = gcs::GetErrorInfoFromActorDeathCause(death_cause);
    const auto error_type = error_info.error_type();

    for (auto &task_id : task_ids_to_fail) {
      // No need to increment the number of completed tasks since the actor is
      // dead.
      task_manager_.MarkTaskNoRetry(task_id);
      // This task may have been waiting for dependency resolution, so cancel
      // this first.
      CancelDependencyResolution(task_id);
      bool fail_immediatedly =
          error_info.has_actor_died_error() &&
          error_info.actor_died_error().has_oom_context() &&
          error_info.actor_died_error().oom_context().fail_immediately();
      task_manager_.FailOrRetryPendingTask(task_id,
                                           error_type,
                                           &status,
                                           &error_info,
                                           /*mark_task_object_failed*/ true,
                                           fail_immediatedly);
    }
    if (!wait_for_death_info_tasks.empty()) {
      RAY_LOG(DEBUG).WithField(actor_id) << "Failing tasks waiting for death info, size="
                                         << wait_for_death_info_tasks.size();
      for (auto &task : wait_for_death_info_tasks) {
        task_manager_.FailPendingTask(
            task->task_spec_.TaskId(), error_type, &task->status_, &error_info);
      }
    }
  }
  // NOTE(kfstorm): We need to make sure the lock is released before invoking callbacks.
  FailInflightTasksOnRestart(inflight_task_callbacks);
}

void ActorTaskSubmitter::FailTaskWithError(const PendingTaskWaitingForDeathInfo &task) {
  rpc::RayErrorInfo error_info;
  if (!task.actor_preempted_) {
    error_info = task.timeout_error_info_;
  } else {
    // Special error for preempted actor. The task "timed out" because the actor may
    // not have sent a notification to the gcs; regardless we already know it's
    // preempted and it's dead.
    auto actor_death_cause = error_info.mutable_actor_died_error();
    auto actor_died_error_context = actor_death_cause->mutable_actor_died_error_context();
    actor_died_error_context->set_reason(rpc::ActorDiedErrorContext::NODE_DIED);
    actor_died_error_context->set_actor_id(task.task_spec_.ActorId().Binary());
    auto node_death_info = actor_died_error_context->mutable_node_death_info();
    node_death_info->set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
    node_death_info->set_reason_message(
        "the node was inferred to be dead due to draining.");
    error_info.set_error_type(rpc::ErrorType::ACTOR_DIED);
    error_info.set_error_message("Actor died by preemption.");
  }
  task_manager_.FailPendingTask(
      task.task_spec_.TaskId(), error_info.error_type(), &task.status_, &error_info);
}

void ActorTaskSubmitter::CheckTimeoutTasks() {
  // For each task in `wait_for_death_info_tasks`, if it times out, fail it with
  // timeout_error_info. But operating on the queue requires the mu_ lock; while calling
  // FailPendingTask requires the opposite. So we copy the tasks out from the queue
  // within the lock. This requires putting the data into shared_ptr.
  std::vector<std::shared_ptr<PendingTaskWaitingForDeathInfo>> timeout_tasks;
  int64_t now = current_time_ms();
  {
    absl::MutexLock lock(&mu_);
    for (auto &[actor_id, client_queue] : client_queues_) {
      auto &deque = client_queue.wait_for_death_info_tasks_;
      auto deque_itr = deque.begin();
      while (deque_itr != deque.end() && (*deque_itr)->deadline_ms_ < now) {
        // Populate the info of whether the actor is preempted. If so we hard fail the
        // task.
        (*deque_itr)->actor_preempted_ = client_queue.preempted_;
        timeout_tasks.push_back(*deque_itr);
        deque_itr = deque.erase(deque_itr);
      }
    }
  }
  // Note: mu_ released.
  for (auto &task : timeout_tasks) {
    FailTaskWithError(*task);
  }
}

void ActorTaskSubmitter::SendPendingTasks(const ActorID &actor_id) {
  auto it = client_queues_.find(actor_id);
  RAY_CHECK(it != client_queues_.end());
  auto &client_queue = it->second;
  auto &actor_submit_queue = client_queue.actor_submit_queue_;
  if (client_queue.pending_out_of_scope_death_) {
    // Wait until the actor is dead and then decide
    // whether we should fail pending tasks or restart the actor.
    // If the actor is restarted, ConnectActor will be called
    // and pending tasks will be sent at that time.
    return;
  }
  if (!client_queue.client_address_.has_value()) {
    if (client_queue.state_ == rpc::ActorTableData::RESTARTING &&
        client_queue.fail_if_actor_unreachable_) {
      // When `fail_if_actor_unreachable` is true, tasks submitted while the actor is in
      // `RESTARTING` state fail immediately.
      while (true) {
        auto task = actor_submit_queue->PopNextTaskToSend();
        if (!task.has_value()) {
          break;
        }

        io_service_.post(
            [this, task_spec = std::move(task.value().first)] {
              rpc::PushTaskReply reply;
              rpc::Address addr;
              HandlePushTaskReply(
                  Status::IOError("The actor is restarting."), reply, addr, task_spec);
            },
            "ActorTaskSubmitter::SendPendingTasks_ForceFail");
      }
    }
    return;
  }

  // Submit all pending actor_submit_queue->
  while (true) {
    auto task = actor_submit_queue->PopNextTaskToSend();
    if (!task.has_value()) {
      break;
    }
    RAY_CHECK(!client_queue.worker_id_.empty());
    PushActorTask(client_queue, /*task_spec=*/task->first, /*skip_queue=*/task->second);
  }
}

void ActorTaskSubmitter::PushActorTask(ClientQueue &queue,
                                       const TaskSpecification &task_spec,
                                       bool skip_queue) {
  const auto task_id = task_spec.TaskId();

  auto request = std::make_unique<rpc::PushTaskRequest>();
  // NOTE(swang): CopyFrom is needed because if we use Swap here and the task
  // fails, then the task data will be gone when the TaskManager attempts to
  // access the task.
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());

  request->set_intended_worker_id(queue.worker_id_);
  request->set_sequence_number(task_spec.SequenceNumber());

  const auto actor_id = task_spec.ActorId();

  const auto num_queued = queue.inflight_task_callbacks_.size();
  RAY_LOG(DEBUG).WithField(task_id).WithField(actor_id)
      << "Pushing task to actor, actor id " << actor_id << " seq no "
      << request->sequence_number() << " num queued " << num_queued;
  if (num_queued >= next_queueing_warn_threshold_) {
    on_excess_queueing_(
        actor_id, task_spec.FunctionDescriptor()->ClassName(), num_queued);
    next_queueing_warn_threshold_ *= 2;
  }

  auto &addr = queue.client_address_.value();
  rpc::ClientCallback<rpc::PushTaskReply> reply_callback =
      [this, addr, task_spec](const Status &status, const rpc::PushTaskReply &reply) {
        HandlePushTaskReply(status, reply, addr, task_spec);
      };

  const TaskAttempt task_attempt = std::make_pair(task_id, task_spec.AttemptNumber());
  queue.inflight_task_callbacks_.emplace(task_attempt, std::move(reply_callback));
  rpc::ClientCallback<rpc::PushTaskReply> wrapped_callback =
      [this, task_attempt, actor_id](const Status &status, rpc::PushTaskReply &&reply) {
        rpc::ClientCallback<rpc::PushTaskReply> push_task_reply_callback;
        {
          absl::MutexLock lock(&mu_);
          auto it = client_queues_.find(actor_id);
          RAY_CHECK(it != client_queues_.end());
          auto &client_queue = it->second;
          auto callback_it = client_queue.inflight_task_callbacks_.find(task_attempt);
          if (callback_it == client_queue.inflight_task_callbacks_.end()) {
            RAY_LOG(DEBUG).WithField(task_attempt.first)
                << "The task has already been marked as failed. Ignore the reply.";
            return;
          }
          push_task_reply_callback = std::move(callback_it->second);
          client_queue.inflight_task_callbacks_.erase(callback_it);
        }
        push_task_reply_callback(status, std::move(reply));
      };

  task_manager_.MarkTaskWaitingForExecution(task_id,
                                            NodeID::FromBinary(addr.node_id()),
                                            WorkerID::FromBinary(addr.worker_id()));
  core_worker_client_pool_.GetOrConnect(addr)->PushActorTask(
      std::move(request), skip_queue, std::move(wrapped_callback));
}

void ActorTaskSubmitter::HandlePushTaskReply(const Status &status,
                                             const rpc::PushTaskReply &reply,
                                             const rpc::Address &addr,
                                             const TaskSpecification &task_spec) {
  const auto task_id = task_spec.TaskId();
  const auto actor_id = task_spec.ActorId();

  bool resubmit_generator = false;
  {
    absl::MutexLock lock(&mu_);
    // If the generator was queued up for resubmission for object recovery,
    // resubmit as long as we get a valid reply.
    resubmit_generator = generators_to_resubmit_.erase(task_id) > 0 && status.ok();
    if (resubmit_generator) {
      auto queue_pair = client_queues_.find(actor_id);
      RAY_CHECK(queue_pair != client_queues_.end());
      auto &queue = queue_pair->second;
      queue.cur_pending_calls_--;
    }
  }
  if (resubmit_generator) {
    task_manager_.MarkGeneratorFailedAndResubmit(task_id);
    return;
  }

  const bool is_retryable_exception = status.ok() && reply.is_retryable_error();
  /// Whether or not we will retry this actor task.
  auto will_retry = false;

  if ((status.ok() && reply.was_cancelled_before_running()) ||
      status.IsSchedulingCancelled()) {
    HandleTaskCancelledBeforeExecution(status, reply, task_spec);
  } else if (status.ok() && !is_retryable_exception) {
    // status.ok() means the worker completed the reply, either succeeded or with a
    // retryable failure (e.g. user exceptions). We complete only on non-retryable case.

    // Handle tasks marked as canceled but completed without application error (sync
    // actors). For async actors, cancellation raises an asyncio.CancelledError exception
    // during task execution, which is treated as an application error
    // (with is_application_error=true) and will be handled by CompletePendingTask.
    // For sync actors, no exception is raised during cancellation, so
    // is_application_error=false and we must explicitly fail the task here with
    // TASK_CANCELLED.
    if (task_manager_.IsTaskCanceled(task_id) && !reply.is_application_error()) {
      RAY_LOG(INFO) << "Task " << task_id << " completed but was cancelled, failing it";
      rpc::RayErrorInfo error_info;
      std::ostringstream error_message;
      error_message << "Task: " << task_id.Hex() << " was cancelled.";
      error_info.set_error_message(error_message.str());
      error_info.set_error_type(rpc::ErrorType::TASK_CANCELLED);
      task_manager_.FailPendingTask(
          task_id, rpc::ErrorType::TASK_CANCELLED, nullptr, &error_info);
    } else {
      task_manager_.CompletePendingTask(
          task_id, reply, addr, reply.is_application_error());
    }
  } else {
    bool is_actor_dead = false;
    bool fail_immediately = false;
    rpc::RayErrorInfo error_info;
    if (status.ok()) {
      // retryable user exception.
      RAY_CHECK(is_retryable_exception);
      error_info = gcs::GetRayErrorInfo(rpc::ErrorType::TASK_EXECUTION_EXCEPTION,
                                        reply.task_execution_error());
    } else {
      // push task failed due to network error. For example, actor is dead
      // and no process response for the push task.
      absl::MutexLock lock(&mu_);
      auto queue_pair = client_queues_.find(actor_id);
      RAY_CHECK(queue_pair != client_queues_.end());
      auto &queue = queue_pair->second;

      // If the actor is already dead, immediately mark the task object as failed.
      // Otherwise, start the grace period, waiting for the actor death reason. Before
      // the deadline:
      // - If we got the death reason: mark the object as failed with that reason.
      // - If we did not get the death reason: raise ACTOR_UNAVAILABLE with the status.
      // - If we did not get the death reason, but *the actor is preempted*: raise
      // ACTOR_DIED. See `CheckTimeoutTasks`.
      is_actor_dead = queue.state_ == rpc::ActorTableData::DEAD;
      if (is_actor_dead) {
        const auto &death_cause = queue.death_cause_;
        error_info = gcs::GetErrorInfoFromActorDeathCause(death_cause);
        fail_immediately = error_info.has_actor_died_error() &&
                           error_info.actor_died_error().has_oom_context() &&
                           error_info.actor_died_error().oom_context().fail_immediately();
      } else {
        // The actor may or may not be dead, but the request failed. Consider the
        // failure temporary. May recognize retry, so fail_immediately = false.
        error_info.set_error_message("The actor is temporarily unavailable: " +
                                     status.ToString());
        error_info.set_error_type(rpc::ErrorType::ACTOR_UNAVAILABLE);
        error_info.mutable_actor_unavailable_error()->set_actor_id(actor_id.Binary());
      }
    }

    // This task may have been waiting for dependency resolution, so cancel
    // this first.
    CancelDependencyResolution(task_id);

    will_retry =
        task_manager_.FailOrRetryPendingTask(task_id,
                                             error_info.error_type(),
                                             &status,
                                             &error_info,
                                             /*mark_task_object_failed*/ is_actor_dead,
                                             fail_immediately);
    if (!is_actor_dead && !will_retry) {
      // Ran out of retries, last failure = either user exception or actor death.
      if (status.ok()) {
        // last failure = user exception, just complete it with failure.
        RAY_CHECK(reply.is_retryable_error());

        task_manager_.CompletePendingTask(
            task_id, reply, addr, reply.is_application_error());

      } else if (RayConfig::instance().timeout_ms_task_wait_for_death_info() != 0) {
        // last failure = Actor death, but we still see the actor "alive" so we
        // optionally wait for a grace period for the death info.

        int64_t death_info_grace_period_ms =
            current_time_ms() +
            RayConfig::instance().timeout_ms_task_wait_for_death_info();
        absl::MutexLock lock(&mu_);
        auto queue_pair = client_queues_.find(actor_id);
        RAY_CHECK(queue_pair != client_queues_.end());
        auto &queue = queue_pair->second;
        queue.wait_for_death_info_tasks_.push_back(
            std::make_shared<PendingTaskWaitingForDeathInfo>(
                death_info_grace_period_ms, task_spec, status, error_info));
        RAY_LOG(INFO).WithField(task_spec.TaskId())
            << "PushActorTask failed because of network error, this task "
               "will be stashed away and waiting for Death info from GCS"
               ", wait_queue_size="
            << queue.wait_for_death_info_tasks_.size();
      } else {
        // TODO(vitsai): if we don't need death info, just fail the request.
        {
          absl::MutexLock lock(&mu_);
          auto queue_pair = client_queues_.find(actor_id);
          RAY_CHECK(queue_pair != client_queues_.end());
        }
        task_manager_.FailPendingTask(
            task_spec.TaskId(), error_info.error_type(), &status, &error_info);
      }
    }
  }
  {
    absl::MutexLock lock(&mu_);
    auto queue_pair = client_queues_.find(actor_id);
    RAY_CHECK(queue_pair != client_queues_.end());
    auto &queue = queue_pair->second;
    queue.cur_pending_calls_--;
  }
}

void ActorTaskSubmitter::HandleTaskCancelledBeforeExecution(
    const Status &status,
    const rpc::PushTaskReply &reply,
    const TaskSpecification &task_spec) {
  const auto task_id = task_spec.TaskId();
  const auto actor_id = task_spec.ActorId();

  if (reply.worker_exiting()) {
    // Task cancelled due to actor shutdown - use ACTOR_DIED error.
    // If we have the death cause, use it immediately. Otherwise,
    // wait for it from GCS to provide an accurate error message.
    bool is_actor_dead = false;
    rpc::RayErrorInfo error_info;
    {
      absl::MutexLock lock(&mu_);
      auto queue_pair = client_queues_.find(actor_id);
      if (queue_pair != client_queues_.end()) {
        is_actor_dead = queue_pair->second.state_ == rpc::ActorTableData::DEAD;
        if (is_actor_dead) {
          const auto &death_cause = queue_pair->second.death_cause_;
          error_info = gcs::GetErrorInfoFromActorDeathCause(death_cause);
        }
      }
    }

    if (is_actor_dead) {
      CancelDependencyResolution(task_id);
      RAY_LOG(DEBUG) << "Task " << task_id << " cancelled due to actor " << actor_id
                     << " death";
      task_manager_.FailPendingTask(task_spec.TaskId(),
                                    error_info.error_type(),
                                    /*status*/ nullptr,
                                    &error_info);
    } else if (RayConfig::instance().timeout_ms_task_wait_for_death_info() != 0) {
      CancelDependencyResolution(task_id);

      int64_t death_info_grace_period_ms =
          current_time_ms() + RayConfig::instance().timeout_ms_task_wait_for_death_info();

      error_info.set_error_type(rpc::ErrorType::ACTOR_DIED);
      error_info.set_error_message(
          "The actor is dead because its worker process has died.");

      {
        absl::MutexLock lock(&mu_);
        auto queue_pair = client_queues_.find(actor_id);
        RAY_CHECK(queue_pair != client_queues_.end());
        auto &queue = queue_pair->second;
        queue.wait_for_death_info_tasks_.push_back(
            std::make_shared<PendingTaskWaitingForDeathInfo>(
                death_info_grace_period_ms, task_spec, status, error_info));
        RAY_LOG(INFO).WithField(task_spec.TaskId())
            << "Task cancelled during actor shutdown, waiting for death info from GCS"
            << ", wait_queue_size=" << queue.wait_for_death_info_tasks_.size();
      }
    } else {
      CancelDependencyResolution(task_id);
      error_info.set_error_type(rpc::ErrorType::ACTOR_DIED);
      error_info.set_error_message(
          "The actor is dead because its worker process has died.");
      task_manager_.FailPendingTask(task_spec.TaskId(),
                                    rpc::ErrorType::ACTOR_DIED,
                                    /*status*/ nullptr,
                                    &error_info);
    }
  } else {
    // Explicit user cancellation - use TASK_CANCELLED error.
    std::ostringstream stream;
    stream << "The task " << task_id << " is canceled from an actor " << actor_id
           << " before it executes.";
    const auto &msg = stream.str();
    RAY_LOG(DEBUG) << msg;
    rpc::RayErrorInfo error_info;
    error_info.set_error_message(msg);
    error_info.set_error_type(rpc::ErrorType::TASK_CANCELLED);
    task_manager_.FailPendingTask(task_spec.TaskId(),
                                  rpc::ErrorType::TASK_CANCELLED,
                                  /*status*/ nullptr,
                                  &error_info);
  }
}

std::optional<rpc::ActorTableData::ActorState> ActorTaskSubmitter::GetLocalActorState(
    const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);

  auto iter = client_queues_.find(actor_id);
  if (iter == client_queues_.end()) {
    return std::nullopt;
  } else {
    return iter->second.state_;
  }
}

bool ActorTaskSubmitter::IsActorAlive(const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);

  auto iter = client_queues_.find(actor_id);
  return (iter != client_queues_.end() && iter->second.client_address_.has_value());
}

std::optional<rpc::Address> ActorTaskSubmitter::GetActorAddress(
    const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);
  auto iter = client_queues_.find(actor_id);
  if (iter == client_queues_.end()) {
    return std::nullopt;
  }
  return iter->second.client_address_;
}

bool ActorTaskSubmitter::PendingTasksFull(const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);
  auto it = client_queues_.find(actor_id);
  RAY_CHECK(it != client_queues_.end());
  return it->second.max_pending_calls_ > 0 &&
         it->second.cur_pending_calls_ >= it->second.max_pending_calls_;
}

size_t ActorTaskSubmitter::NumPendingTasks(const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);
  auto it = client_queues_.find(actor_id);
  RAY_CHECK(it != client_queues_.end());
  return it->second.cur_pending_calls_;
}

bool ActorTaskSubmitter::CheckActorExists(const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);
  return client_queues_.find(actor_id) != client_queues_.end();
}

std::string ActorTaskSubmitter::DebugString(const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);
  auto it = client_queues_.find(actor_id);
  RAY_CHECK(it != client_queues_.end());
  std::ostringstream stream;
  stream << "Submitter debug string for actor " << actor_id << " "
         << it->second.DebugString();
  return stream.str();
}

void ActorTaskSubmitter::RetryCancelTask(TaskSpecification task_spec, bool recursive) {
  auto delay_ms = RayConfig::instance().cancellation_retry_ms();
  RAY_LOG(DEBUG).WithField(task_spec.TaskId())
      << "Task cancelation will be retried in " << delay_ms << " ms";
  execute_after(
      io_service_,
      [this, task_spec = std::move(task_spec), recursive] {
        CancelTask(task_spec, recursive);
      },
      std::chrono::milliseconds(delay_ms));
}

void ActorTaskSubmitter::CancelTask(TaskSpecification task_spec, bool recursive) {
  // We don't support force_kill = true for actor tasks.
  bool force_kill = false;
  RAY_LOG(INFO).WithField(task_spec.TaskId()).WithField(task_spec.ActorId())
      << "Cancelling an actor task: force_kill: " << force_kill
      << " recursive: " << recursive;

  // Tasks are in one of the following states.
  // - dependencies not resolved
  // - queued
  // - sent
  // - finished.

  const auto actor_id = task_spec.ActorId();
  const auto &task_id = task_spec.TaskId();
  auto send_pos = task_spec.SequenceNumber();

  // Shouldn't hold a lock while accessing task_manager_.
  // Task is already canceled or finished.
  task_manager_.MarkTaskCanceled(task_id);
  if (!task_manager_.IsTaskPending(task_id)) {
    RAY_LOG(DEBUG).WithField(task_id) << "Task is already finished or canceled";
    return;
  }

  auto task_queued = false;
  {
    absl::MutexLock lock(&mu_);

    generators_to_resubmit_.erase(task_id);

    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (queue->second.state_ == rpc::ActorTableData::DEAD) {
      // No need to decrement cur_pending_calls because it doesn't matter.
      RAY_LOG(DEBUG).WithField(task_id)
          << "Task's actor is already dead. Ignoring the cancel request.";
      return;
    }

    task_queued = queue->second.actor_submit_queue_->Contains(send_pos);
    if (task_queued) {
      RAY_LOG(DEBUG).WithField(task_id)
          << "Task was queued. Mark a task is canceled from a queue.";
      queue->second.actor_submit_queue_->MarkTaskCanceled(send_pos);
    }
  }

  // Fail a request immediately if it is still queued.
  // The task won't be sent to an actor in this case.
  // We cannot hold a lock when calling `FailOrRetryPendingTask`.
  if (task_queued) {
    // Could be in dependency resolution or ResolveDependencies call may be queued up
    CancelDependencyResolution(task_id);
    rpc::RayErrorInfo error_info;
    std::ostringstream stream;
    stream << "The task " << task_id << " is canceled from an actor " << actor_id
           << " before it executes.";
    error_info.set_error_message(stream.str());
    error_info.set_error_type(rpc::ErrorType::TASK_CANCELLED);
    task_manager_.FailOrRetryPendingTask(
        task_id, rpc::ErrorType::TASK_CANCELLED, /*status*/ nullptr, &error_info);
    return;
  }

  // At this point, the task is in "sent" state and not finished yet.
  // We cannot guarantee a cancel request is received "after" a task
  // is submitted because gRPC is not ordered. To get around it,
  // we keep retrying cancel RPCs until task is finished or
  // an executor tells us to stop retrying.

  // If there's no client, it means actor is not created yet.
  // Retry after the configured delay.
  NodeID node_id;
  std::string executor_worker_id;
  {
    absl::MutexLock lock(&mu_);
    RAY_LOG(DEBUG).WithField(task_id) << "Task was sent to an actor. Send a cancel RPC.";
    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (!queue->second.client_address_.has_value()) {
      RetryCancelTask(task_spec, recursive);
      return;
    }
    node_id = NodeID::FromBinary(queue->second.client_address_.value().node_id());
    executor_worker_id = queue->second.client_address_.value().worker_id();
  }

  auto do_cancel_local_task =
      [this, task_spec = std::move(task_spec), force_kill, recursive, executor_worker_id](
          const rpc::Address &raylet_address) mutable {
        rpc::CancelLocalTaskRequest request;
        request.set_intended_task_id(task_spec.TaskIdBinary());
        request.set_force_kill(force_kill);
        request.set_recursive(recursive);
        request.set_caller_worker_id(task_spec.CallerWorkerIdBinary());
        request.set_executor_worker_id(executor_worker_id);

        auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(raylet_address);
        raylet_client->CancelLocalTask(
            request,
            [this, task_spec = std::move(task_spec), recursive](
                const Status &status, const rpc::CancelLocalTaskReply &reply) mutable {
              if (!status.ok()) {
                RAY_LOG(INFO) << "CancelLocalTask RPC failed for task "
                              << task_spec.TaskId() << ": " << status.ToString()
                              << " due to node death";
                return;
              } else {
                RAY_LOG(INFO) << "CancelLocalTask RPC response received for "
                              << task_spec.TaskId()
                              << " with attempt_succeeded: " << reply.attempt_succeeded()
                              << " requested_task_running: "
                              << reply.requested_task_running();
              }
              // Keep retrying until a task is officially finished.
              if (!reply.attempt_succeeded()) {
                RetryCancelTask(std::move(task_spec), recursive);
              }
            });
      };
  SendCancelLocalTask(gcs_client_, node_id, std::move(do_cancel_local_task), []() {});
}

bool ActorTaskSubmitter::QueueGeneratorForResubmit(const TaskSpecification &spec) {
  // TODO(dayshah): Needs to integrate with the cancellation logic - what if task was
  // cancelled before this?
  absl::MutexLock lock(&mu_);
  generators_to_resubmit_.insert(spec.TaskId());
  return true;
}

}  // namespace core
}  // namespace ray
