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

#include "ray/core_worker/transport/direct_actor_transport.h"

#include <thread>

#include "ray/common/task/task.h"
#include "ray/gcs/pb_util.h"

using ray::rpc::ActorTableData;
using namespace ray::gcs;

namespace ray {
namespace core {

void SerializeReturnObject(const ObjectID &object_id,
                           const std::shared_ptr<RayObject> &return_object,
                           rpc::ReturnObject *return_object_proto) {
  return_object_proto->set_object_id(object_id.Binary());

  if (!return_object) {
    // This should only happen if the local raylet died. Caller should
    // retry the task.
    RAY_LOG(WARNING) << "Failed to create task return object " << object_id
                     << " in the object store, exiting.";
    QuickExit();
  }
  return_object_proto->set_size(return_object->GetSize());
  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    return_object_proto->set_in_plasma(true);
  } else {
    if (return_object->GetData() != nullptr) {
      return_object_proto->set_data(return_object->GetData()->Data(),
                                    return_object->GetData()->Size());
    }
    if (return_object->GetMetadata() != nullptr) {
      return_object_proto->set_metadata(return_object->GetMetadata()->Data(),
                                        return_object->GetMetadata()->Size());
    }
  }
  for (const auto &nested_ref : return_object->GetNestedRefs()) {
    return_object_proto->add_nested_inlined_refs()->CopyFrom(nested_ref);
  }
}

void CoreWorkerDirectTaskReceiver::Init(
    std::shared_ptr<rpc::CoreWorkerClientPool> client_pool,
    rpc::Address rpc_address,
    std::shared_ptr<DependencyWaiter> dependency_waiter) {
  waiter_ = std::move(dependency_waiter);
  rpc_address_ = rpc_address;
  client_pool_ = client_pool;
}

void CoreWorkerDirectTaskReceiver::HandleTask(
    rpc::PushTaskRequest request,
    rpc::PushTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(waiter_ != nullptr) << "Must call init() prior to use";
  // Use `mutable_task_spec()` here as `task_spec()` returns a const reference
  // which doesn't work with std::move.
  TaskSpecification task_spec(
      std::move(*(const_cast<rpc::PushTaskRequest &>(request).mutable_task_spec())));

  // If GCS server is restarted after sending an actor creation task to this core worker,
  // the restarted GCS server will send the same actor creation task to the core worker
  // again. We just need to ignore it and reply ok.
  if (task_spec.IsActorCreationTask() &&
      worker_context_.GetCurrentActorID() == task_spec.ActorCreationId()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    RAY_LOG(INFO) << "Ignoring duplicate actor creation task for actor "
                  << task_spec.ActorCreationId()
                  << ". This is likely due to a GCS server restart.";
    return;
  }

  if (task_spec.IsActorCreationTask()) {
    worker_context_.SetCurrentActorId(task_spec.ActorCreationId());
    SetupActor(task_spec.IsAsyncioActor(),
               task_spec.MaxActorConcurrency(),
               task_spec.ExecuteOutOfOrder());
  }

  // Only assign resources for non-actor tasks. Actor tasks inherit the resources
  // assigned at initial actor creation time.
  std::shared_ptr<ResourceMappingType> resource_ids;
  if (!task_spec.IsActorTask()) {
    resource_ids.reset(new ResourceMappingType());
    for (const auto &mapping : request.resource_mapping()) {
      std::vector<std::pair<int64_t, double>> rids;
      for (const auto &ids : mapping.resource_ids()) {
        rids.push_back(std::make_pair(ids.index(), ids.quantity()));
      }
      (*resource_ids)[mapping.name()] = rids;
    }
  }

  auto accept_callback = [this, reply, task_spec, resource_ids](
                             rpc::SendReplyCallback send_reply_callback) {
    if (task_spec.GetMessage().skip_execution()) {
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }

    auto num_returns = task_spec.NumReturns();
    RAY_CHECK(num_returns >= 0);

    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> return_objects;
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> dynamic_return_objects;
    bool is_retryable_error = false;
    bool is_application_error = false;
    auto status = task_handler_(task_spec,
                                resource_ids,
                                &return_objects,
                                &dynamic_return_objects,
                                reply->mutable_borrowed_refs(),
                                &is_retryable_error,
                                &is_application_error);
    reply->set_is_retryable_error(is_retryable_error);
    reply->set_is_application_error(is_application_error);

    bool objects_valid = return_objects.size() == num_returns;
    for (const auto &return_object : return_objects) {
      if (return_object.second == NULL) {
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
        /// The default max concurrency for creating PoolManager should
        /// be 0 if this is an asyncio actor.
        const int default_max_concurrency =
            task_spec.IsAsyncioActor() ? 0 : task_spec.MaxActorConcurrency();
        pool_manager_ = std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
            task_spec.ConcurrencyGroups(), default_max_concurrency);
        concurrency_groups_cache_[task_spec.TaskId().ActorId()] =
            task_spec.ConcurrencyGroups();
        RAY_LOG(INFO) << "Actor creation task finished, task_id: " << task_spec.TaskId()
                      << ", actor_id: " << task_spec.ActorCreationId();
        // Tell raylet that an actor creation task has finished execution, so that
        // raylet can publish actor creation event to GCS, and mark this worker as
        // actor, thus if this worker dies later raylet will restart the actor.
        RAY_CHECK_OK(task_done_());
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

  auto cancel_callback = [reply, task_spec](rpc::SendReplyCallback send_reply_callback) {
    if (task_spec.IsActorTask()) {
      // We consider cancellation of actor tasks to be a push task RPC failure.
      send_reply_callback(
          Status::Invalid("client cancelled stale rpc"), nullptr, nullptr);
    } else {
      // We consider cancellation of normal tasks to be an in-band cancellation of a
      // successful RPC.
      reply->set_was_cancelled_before_running(true);
      send_reply_callback(Status::OK(), nullptr, nullptr);
    }
  };

  auto dependencies = task_spec.GetDependencies();

  if (task_spec.IsActorTask()) {
    auto it = actor_scheduling_queues_.find(task_spec.CallerWorkerId());
    if (it == actor_scheduling_queues_.end()) {
      auto cg_it = concurrency_groups_cache_.find(task_spec.ActorId());
      RAY_CHECK(cg_it != concurrency_groups_cache_.end());
      if (execute_out_of_order_) {
        it = actor_scheduling_queues_
                 .emplace(task_spec.CallerWorkerId(),
                          std::unique_ptr<SchedulingQueue>(
                              new OutOfOrderActorSchedulingQueue(task_main_io_service_,
                                                                 *waiter_,
                                                                 pool_manager_,
                                                                 is_asyncio_,
                                                                 fiber_max_concurrency_,
                                                                 cg_it->second)))
                 .first;
      } else {
        it = actor_scheduling_queues_
                 .emplace(task_spec.CallerWorkerId(),
                          std::unique_ptr<SchedulingQueue>(
                              new ActorSchedulingQueue(task_main_io_service_,
                                                       *waiter_,
                                                       pool_manager_,
                                                       is_asyncio_,
                                                       fiber_max_concurrency_,
                                                       cg_it->second)))
                 .first;
      }
    }

    it->second->Add(request.sequence_number(),
                    request.client_processed_up_to(),
                    std::move(accept_callback),
                    std::move(cancel_callback),
                    std::move(send_reply_callback),
                    task_spec.ConcurrencyGroupName(),
                    task_spec.FunctionDescriptor(),
                    task_spec.TaskId(),
                    dependencies);
  } else {
    // Add the normal task's callbacks to the non-actor scheduling queue.
    RAY_LOG(DEBUG) << "Adding task " << task_spec.TaskId()
                   << " to normal scheduling task queue.";
    normal_scheduling_queue_->Add(request.sequence_number(),
                                  request.client_processed_up_to(),
                                  std::move(accept_callback),
                                  std::move(cancel_callback),
                                  std::move(send_reply_callback),
                                  "",
                                  task_spec.FunctionDescriptor(),
                                  task_spec.TaskId(),
                                  dependencies);
  }
}

void CoreWorkerDirectTaskReceiver::RunNormalTasksFromQueue() {
  // If the scheduling queue is empty, return.
  if (normal_scheduling_queue_->TaskQueueEmpty()) {
    return;
  }

  // Execute as many tasks as there are in the queue, in sequential order.
  normal_scheduling_queue_->ScheduleRequests();
}

bool CoreWorkerDirectTaskReceiver::CancelQueuedNormalTask(TaskID task_id) {
  // Look up the task to be canceled in the queue of normal tasks. If it is found and
  // removed successfully, return true.
  return normal_scheduling_queue_->CancelTaskIfFound(task_id);
}

/// Note that this method is only used for asyncio actor.
void CoreWorkerDirectTaskReceiver::SetupActor(bool is_asyncio,
                                              int fiber_max_concurrency,
                                              bool execute_out_of_order) {
  RAY_CHECK(fiber_max_concurrency_ == 0)
      << "SetupActor should only be called at most once.";
  is_asyncio_ = is_asyncio;
  fiber_max_concurrency_ = fiber_max_concurrency;
  execute_out_of_order_ = execute_out_of_order;
}

void CoreWorkerDirectTaskReceiver::Stop() {
  for (const auto &[_, scheduling_queue] : actor_scheduling_queues_) {
    scheduling_queue->Stop();
  }
}

}  // namespace core
}  // namespace ray
