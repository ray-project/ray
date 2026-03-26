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

#include "ray/core_worker/task_execution/task_receiver.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/core_worker/common.h"
#include "ray/core_worker/task_execution/common.h"
#include "ray/core_worker/task_execution/ordered_actor_task_execution_queue.h"
#include "ray/core_worker/task_execution/unordered_actor_task_execution_queue.h"

namespace ray {
namespace core {

void TaskReceiver::HandleTaskExecutionResult(
    Status status,
    const TaskSpecification &task_spec,
    const TaskExecutionResult &result,
    const rpc::SendReplyCallback &send_reply_callback,
    rpc::PushTaskReply *reply) {
  reply->set_is_retryable_error(result.is_retryable_error);
  reply->set_is_application_error(!result.application_error.empty());
  std::string task_execution_error;

  if (!result.application_error.empty()) {
    task_execution_error = "User exception:\n" + result.application_error;
  }
  if (!status.ok()) {
    if (!task_execution_error.empty()) {
      task_execution_error += "\n\n";
    }
    task_execution_error += "System error:\n" + status.ToString();
  }

  if (!task_execution_error.empty()) {
    reply->set_task_execution_error(task_execution_error);
  }

  for (const auto &it : result.streaming_generator_returns) {
    const auto &object_id = it.first;
    bool is_plasma_object = it.second;
    auto return_id_proto = reply->add_streaming_generator_return_ids();
    return_id_proto->set_object_id(object_id.Binary());
    return_id_proto->set_is_plasma_object(is_plasma_object);
  }

  bool objects_valid = result.return_objects.size() == task_spec.NumReturns();
  size_t empty_object_idx = 0;
  for (size_t i = 0; i < result.return_objects.size(); i++) {
    if (result.return_objects[i].second == nullptr) {
      objects_valid = false;
      empty_object_idx = i;
    }
  }

  if (objects_valid) {
    if (task_spec.ReturnsDynamic()) {
      size_t num_dynamic_returns_expected = task_spec.DynamicReturnIds().size();
      if (num_dynamic_returns_expected > 0) {
        RAY_CHECK(result.dynamic_return_objects.size() == num_dynamic_returns_expected)
            << "Expected " << num_dynamic_returns_expected
            << " dynamic returns, but task generated "
            << result.dynamic_return_objects.size();
      }
    } else {
      RAY_CHECK(result.dynamic_return_objects.size() == 0)
          << "Task with static num_returns returned "
          << result.dynamic_return_objects.size() << " objects dynamically";
    }
    for (const auto &dynamic_return : result.dynamic_return_objects) {
      auto return_object_proto = reply->add_dynamic_return_objects();
      SerializeReturnObject(
          dynamic_return.first, dynamic_return.second, return_object_proto);
    }
    for (size_t i = 0; i < result.return_objects.size(); i++) {
      const auto &return_object = result.return_objects[i];
      auto return_object_proto = reply->add_return_objects();
      SerializeReturnObject(
          return_object.first, return_object.second, return_object_proto);
    }

    if (task_spec.IsActorCreationTask()) {
      concurrency_groups_ = task_spec.ConcurrencyGroups();
      if (is_asyncio_) {
        fiber_state_manager_ = std::make_shared<ConcurrencyGroupManager<FiberState>>(
            concurrency_groups_, fiber_max_concurrency_, initialize_thread_callback_);
      } else {
        const int default_max_concurrency = task_spec.MaxActorConcurrency();
        pool_manager_ = std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
            concurrency_groups_, default_max_concurrency, initialize_thread_callback_);
      }

      if (status.IsCreationTaskError()) {
        RAY_LOG(WARNING) << "Actor creation task finished with errors, task_id: "
                         << task_spec.TaskId()
                         << ", actor_id: " << task_spec.ActorCreationId()
                         << ", status: " << status;
      } else {
        if (!result.actor_repr_name.empty()) {
          reply->set_actor_repr_name(result.actor_repr_name);
        }
        RAY_LOG(INFO) << "Actor creation task finished, task_id: " << task_spec.TaskId()
                      << ", actor_id: " << task_spec.ActorCreationId()
                      << ", actor_repr_name: " << result.actor_repr_name;
      }
    }
  }
  RAY_CHECK(!status.IsTimedOut())
      << "Timeout unexpected! We assume calls to the raylet don't timeout!";
  if (status.IsIntentionalSystemExit() || status.IsUnexpectedSystemExit() ||
      status.IsCreationTaskError() || status.IsInterrupted() || status.IsIOError() ||
      status.IsDisconnected()) {
    reply->set_worker_exiting(true);
    if (objects_valid) {
      send_reply_callback(Status::OK(), nullptr, nullptr);
    } else {
      send_reply_callback(status, nullptr, nullptr);
    }
  } else {
    RAY_CHECK_OK(status);
    RAY_CHECK(objects_valid) << task_spec.NumReturns() << " return objects expected, "
                             << result.return_objects.size()
                             << " returned. Object at idx " << empty_object_idx
                             << " was not stored.";
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
}

void TaskReceiver::QueueTaskForExecution(rpc::PushTaskRequest request,
                                         rpc::PushTaskReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  TaskSpecification task_spec =
      TaskSpecification(std::move(*request.mutable_task_spec()));
  if (stopping_) {
    reply->set_was_cancelled_before_running(true);
    if (task_spec.IsActorTask()) {
      reply->set_worker_exiting(true);
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  // Only assign resources for non-actor tasks. Actor tasks inherit the resources
  // assigned at initial actor creation time.
  std::optional<ResourceMappingType> resource_ids;
  if (!task_spec.IsActorTask()) {
    resource_ids.emplace();
    for (auto &mapping : *request.mutable_resource_mapping()) {
      std::vector<std::pair<int64_t, double>> rids;
      rids.reserve(mapping.resource_ids().size());
      for (const auto &ids : mapping.resource_ids()) {
        rids.emplace_back(ids.index(), ids.quantity());
      }
      resource_ids->emplace(std::move(*mapping.mutable_name()), std::move(rids));
    }
  }

  auto execute_callback =
      [this, reply, send_reply_callback, resource_ids = std::move(resource_ids)](
          const TaskSpecification &t) mutable {
        TaskExecutionResult result;
        auto status = task_handler_(t,
                                    std::move(resource_ids),
                                    &result.return_objects,
                                    &result.dynamic_return_objects,
                                    &result.streaming_generator_returns,
                                    reply->mutable_borrowed_refs(),
                                    &result.is_retryable_error,
                                    &result.actor_repr_name,
                                    &result.application_error);

        HandleTaskExecutionResult(status, t, result, send_reply_callback, reply);
      };

  auto cancel_callback = [this, reply, send_reply_callback](const TaskSpecification &t,
                                                            const Status &status) {
    if (t.IsActorTask()) {
      // If task cancelation is due to worker shutdown, propagate that information
      // to the submitter.
      if (stopping_) {
        reply->set_worker_exiting(true);
        reply->set_was_cancelled_before_running(true);
        send_reply_callback(Status::OK(), nullptr, nullptr);
      } else {
        send_reply_callback(status, nullptr, nullptr);
      }
    } else {
      reply->set_was_cancelled_before_running(true);
      send_reply_callback(status, nullptr, nullptr);
    }
  };

  if (task_spec.IsActorCreationTask()) {
    SetupActor(task_spec.IsAsyncioActor(),
               task_spec.MaxActorConcurrency(),
               task_spec.AllowOutOfOrderExecution());
    normal_task_execution_queue_->EnqueueTask(
        TaskToExecute(execute_callback, cancel_callback, std::move(task_spec)));
  } else if (task_spec.IsActorTask()) {
    auto it = actor_task_execution_queues_.find(task_spec.CallerWorkerId());
    if (it == actor_task_execution_queues_.end()) {
      it = actor_task_execution_queues_
               .emplace(
                   task_spec.CallerWorkerId(),
                   allow_out_of_order_execution_
                       ? std::unique_ptr<ActorTaskExecutionQueueInterface>(
                             std::make_unique<UnorderedActorTaskExecutionQueue>(
                                 task_execution_service_,
                                 waiter_,
                                 task_event_buffer_,
                                 pool_manager_,
                                 fiber_state_manager_,
                                 is_asyncio_,
                                 fiber_max_concurrency_,
                                 concurrency_groups_))
                       : std::unique_ptr<ActorTaskExecutionQueueInterface>(
                             std::make_unique<OrderedActorTaskExecutionQueue>(
                                 task_execution_service_,
                                 waiter_,
                                 task_event_buffer_,
                                 pool_manager_,
                                 RayConfig::instance()
                                     .actor_scheduling_queue_max_reorder_wait_seconds())))
               .first;
    }
    it->second->EnqueueTask(
        request.sequence_number(),
        request.client_processed_up_to(),
        TaskToExecute(execute_callback, cancel_callback, std::move(task_spec)));
  } else {
    normal_task_execution_queue_->EnqueueTask(
        TaskToExecute(execute_callback, cancel_callback, std::move(task_spec)));
  }
}

void TaskReceiver::ExecuteQueuedNormalTasks() {
  normal_task_execution_queue_->ExecuteQueuedTasks();
}

bool TaskReceiver::CancelQueuedActorTask(const WorkerID &caller_worker_id,
                                         const TaskID &task_id) {
  bool task_found = false;
  auto it = actor_task_execution_queues_.find(caller_worker_id);
  if (it != actor_task_execution_queues_.end()) {
    task_found = it->second->CancelTaskIfFound(task_id);
  }

  // Return false if either:
  //   (1) there is no scheduling queue for the caller
  //   (2) the specified task_id was not found in the scheduling queue
  return task_found;
}

bool TaskReceiver::CancelQueuedNormalTask(TaskID task_id) {
  // Look up the task to be canceled in the queue of normal tasks. If it is found and
  // removed successfully, return true.
  return normal_task_execution_queue_->CancelTaskIfFound(task_id);
}

void TaskReceiver::SetupActor(bool is_asyncio,
                              int fiber_max_concurrency,
                              bool allow_out_of_order_execution) {
  RAY_CHECK(fiber_max_concurrency_ == 0)
      << "SetupActor should only be called at most once.";
  // Note: It's possible to have allow_out_of_order_execution as false but max_concurrency
  // > 1, from the C++ / Java API's.
  RAY_CHECK(is_asyncio ? allow_out_of_order_execution : true)
      << "allow_out_of_order_execution must be true if is_asyncio is true";
  is_asyncio_ = is_asyncio;
  fiber_max_concurrency_ = fiber_max_concurrency;
  allow_out_of_order_execution_ = allow_out_of_order_execution;
}

void TaskReceiver::Stop() {
  if (stopping_.exchange(true)) {
    return;
  }
  for (const auto &[_, scheduling_queue] : actor_task_execution_queues_) {
    scheduling_queue->Stop();
  }
  if (normal_task_execution_queue_) {
    normal_task_execution_queue_->Stop();
  }
}

}  // namespace core
}  // namespace ray
