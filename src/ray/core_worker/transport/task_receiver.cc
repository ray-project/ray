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

#include "ray/core_worker/transport/task_receiver.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ray {
namespace core {

void TaskReceiver::HandleTask(rpc::PushTaskRequest request,
                              rpc::PushTaskReply *reply,
                              rpc::SendReplyCallback send_reply_callback) {
  TaskSpecification task_spec(std::move(*request.mutable_task_spec()));

  if (task_spec.IsActorCreationTask()) {
    SetupActor(task_spec.IsAsyncioActor(),
               task_spec.MaxActorConcurrency(),
               task_spec.ExecuteOutOfOrder());
  }

  // Only assign resources for non-actor tasks. Actor tasks inherit the resources
  // assigned at initial actor creation time.
  std::optional<ResourceMappingType> resource_ids;
  if (!task_spec.IsActorTask()) {
    resource_ids = ResourceMappingType{};
    for (const auto &mapping : request.resource_mapping()) {
      std::vector<std::pair<int64_t, double>> rids;
      rids.reserve(mapping.resource_ids().size());
      for (const auto &ids : mapping.resource_ids()) {
        rids.emplace_back(ids.index(), ids.quantity());
      }
      (*resource_ids)[mapping.name()] = std::move(rids);
    }
  }

  auto accept_callback = [this, reply, resource_ids = std::move(resource_ids)](
                             const TaskSpecification &task_spec,
                             const rpc::SendReplyCallback &send_reply_callback) mutable {
    auto num_returns = task_spec.NumReturns();
    RAY_CHECK(num_returns >= 0);

    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> return_objects;
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> dynamic_return_objects;
    std::vector<std::pair<ObjectID, bool>> streaming_generator_returns;
    bool is_retryable_error = false;
    std::string application_error;
    auto status = task_handler_(task_spec,
                                std::move(resource_ids),
                                &return_objects,
                                &dynamic_return_objects,
                                &streaming_generator_returns,
                                reply->mutable_borrowed_refs(),
                                &is_retryable_error,
                                &application_error);
    reply->set_is_retryable_error(is_retryable_error);
    reply->set_is_application_error(!application_error.empty());
    std::string task_execution_error;

    if (!application_error.empty()) {
      task_execution_error = "User exception:\n" + application_error;
    }
    // System errors occurred while executing the task.
    if (!status.ok()) {
      if (!task_execution_error.empty()) {
        task_execution_error += "\n\n";
      }
      task_execution_error += "System error:\n" + status.ToString();
    }

    if (!task_execution_error.empty()) {
      // Application errors occurred while executing the task.
      // We could get the errors from return_objects, but it would require deserializing
      // the serialized error message. So we just record the error message directly while
      // executing the task.
      reply->set_task_execution_error(task_execution_error);
    }

    for (const auto &it : streaming_generator_returns) {
      const auto &object_id = it.first;
      bool is_plasma_object = it.second;
      auto return_id_proto = reply->add_streaming_generator_return_ids();
      return_id_proto->set_object_id(object_id.Binary());
      return_id_proto->set_is_plasma_object(is_plasma_object);
    }

    bool objects_valid = return_objects.size() == num_returns;
    for (const auto &return_object : return_objects) {
      if (return_object.second == nullptr) {
        objects_valid = false;
      }
    }

    if (objects_valid) {
      if (task_spec.ReturnsDynamic()) {
        size_t num_dynamic_returns_expected = task_spec.DynamicReturnIds().size();
        if (num_dynamic_returns_expected > 0) {
          RAY_CHECK(dynamic_return_objects.size() == num_dynamic_returns_expected)
              << "Expected " << num_dynamic_returns_expected
              << " dynamic returns, but task generated " << dynamic_return_objects.size();
        }
      } else {
        RAY_CHECK(dynamic_return_objects.size() == 0)
            << "Task with static num_returns returned " << dynamic_return_objects.size()
            << " objects dynamically";
      }
      for (const auto &dynamic_return : dynamic_return_objects) {
        auto return_object_proto = reply->add_dynamic_return_objects();
        SerializeReturnObject(
            dynamic_return.first, dynamic_return.second, return_object_proto);
      }
      for (size_t i = 0; i < return_objects.size(); i++) {
        const auto &return_object = return_objects[i];
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
          // If the actor is an asyncio actor, then this concurrency group manager
          // for BoundedExecutor will never be used, so we don't need to initialize it.
          const int default_max_concurrency = task_spec.MaxActorConcurrency();
          pool_manager_ = std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
              concurrency_groups_, default_max_concurrency, initialize_thread_callback_);
        }

        // Tell raylet that an actor creation task has finished execution, so that
        // raylet can publish actor creation event to GCS, and mark this worker as
        // actor, thus if this worker dies later raylet will restart the actor.
        RAY_CHECK_OK(actor_creation_task_done_());
        if (status.IsCreationTaskError()) {
          RAY_LOG(WARNING) << "Actor creation task finished with errors, task_id: "
                           << task_spec.TaskId()
                           << ", actor_id: " << task_spec.ActorCreationId()
                           << ", status: " << status;
        } else {
          // Set the actor repr name if it's customized by the actor.
          if (!actor_repr_name_.empty()) {
            reply->set_actor_repr_name(actor_repr_name_);
          }
          RAY_LOG(INFO) << "Actor creation task finished, task_id: " << task_spec.TaskId()
                        << ", actor_id: " << task_spec.ActorCreationId()
                        << ", actor_repr_name: " << actor_repr_name_;
        }
      }
    }
    if (status.ShouldExitWorker()) {
      // Don't allow the worker to be reused, even though the reply status is OK.
      // The worker will be shutting down shortly.
      reply->set_worker_exiting(true);
      if (objects_valid) {
        // This happens when max_calls is hit. We still need to return the objects.
        send_reply_callback(Status::OK(), nullptr, nullptr);
      } else {
        send_reply_callback(status, nullptr, nullptr);
      }
    } else {
      RAY_CHECK(objects_valid);
      send_reply_callback(status, nullptr, nullptr);
    }
  };

  auto cancel_callback = [reply](const TaskSpecification &task_spec,
                                 const Status &status,
                                 const rpc::SendReplyCallback &send_reply_callback) {
    if (task_spec.IsActorTask()) {
      // We consider cancellation of actor tasks to be a push task RPC failure.
      send_reply_callback(status, nullptr, nullptr);
    } else {
      // We consider cancellation of normal tasks to be an in-band cancellation of a
      // successful RPC.
      reply->set_was_cancelled_before_running(true);
      send_reply_callback(status, nullptr, nullptr);
    }
  };

  if (task_spec.IsActorTask()) {
    auto it = actor_scheduling_queues_.find(task_spec.CallerWorkerId());
    if (it == actor_scheduling_queues_.end()) {
      it = actor_scheduling_queues_
               .emplace(
                   task_spec.CallerWorkerId(),
                   execute_out_of_order_
                       ? std::unique_ptr<SchedulingQueue>(
                             std::make_unique<OutOfOrderActorSchedulingQueue>(
                                 task_execution_service_,
                                 waiter_,
                                 task_event_buffer_,
                                 pool_manager_,
                                 fiber_state_manager_,
                                 is_asyncio_,
                                 fiber_max_concurrency_,
                                 concurrency_groups_))
                       : std::unique_ptr<SchedulingQueue>(
                             std::make_unique<ActorSchedulingQueue>(
                                 task_execution_service_,
                                 waiter_,
                                 task_event_buffer_,
                                 pool_manager_,
                                 RayConfig::instance()
                                     .actor_scheduling_queue_max_reorder_wait_seconds())))
               .first;
    }

    it->second->Add(request.sequence_number(),
                    request.client_processed_up_to(),
                    std::move(accept_callback),
                    std::move(cancel_callback),
                    std::move(send_reply_callback),
                    std::move(task_spec));
  } else {
    // Add the normal task's callbacks to the non-actor scheduling queue.
    RAY_LOG(DEBUG) << "Adding task " << task_spec.TaskId()
                   << " to normal scheduling task queue.";
    normal_scheduling_queue_->Add(request.sequence_number(),
                                  request.client_processed_up_to(),
                                  std::move(accept_callback),
                                  std::move(cancel_callback),
                                  std::move(send_reply_callback),
                                  std::move(task_spec));
  }
}

void TaskReceiver::RunNormalTasksFromQueue() {
  // If the scheduling queue is empty, return.
  if (normal_scheduling_queue_->TaskQueueEmpty()) {
    return;
  }

  // Execute as many tasks as there are in the queue, in sequential order.
  normal_scheduling_queue_->ScheduleRequests();
}

bool TaskReceiver::CancelQueuedActorTask(const WorkerID &caller_worker_id,
                                         const TaskID &task_id) {
  bool task_found = false;
  auto it = actor_scheduling_queues_.find(caller_worker_id);
  if (it != actor_scheduling_queues_.end()) {
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
  return normal_scheduling_queue_->CancelTaskIfFound(task_id);
}

void TaskReceiver::SetupActor(bool is_asyncio,
                              int fiber_max_concurrency,
                              bool execute_out_of_order) {
  RAY_CHECK(fiber_max_concurrency_ == 0)
      << "SetupActor should only be called at most once.";
  // Note: It's possible to have execute_out_of_order as false but max_concurrency > 1,
  // from the C++ / Java API's.
  RAY_CHECK(is_asyncio ? execute_out_of_order : true)
      << "execute_out_of_order must be true if is_asyncio is true";
  is_asyncio_ = is_asyncio;
  fiber_max_concurrency_ = fiber_max_concurrency;
  execute_out_of_order_ = execute_out_of_order;
}

void TaskReceiver::Stop() {
  for (const auto &[_, scheduling_queue] : actor_scheduling_queues_) {
    scheduling_queue->Stop();
  }
}

void TaskReceiver::SetActorReprName(const std::string &repr_name) {
  actor_repr_name_ = repr_name;
}

}  // namespace core
}  // namespace ray
