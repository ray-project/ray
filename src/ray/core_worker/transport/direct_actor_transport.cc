#include <thread>

#include "ray/common/task/task.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

using ray::rpc::ActorTableData;

namespace ray {

int64_t GetRequestNumber(const std::unique_ptr<rpc::PushTaskRequest> &request) {
  return request->task_spec().actor_task_spec().actor_counter();
}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  RAY_LOG(DEBUG) << "Submitting task " << task_spec.TaskId();
  RAY_CHECK(task_spec.IsActorTask());

  resolver_.ResolveDependencies(task_spec, [this, task_spec]() mutable {
    const auto &actor_id = task_spec.ActorId();
    const auto task_id = task_spec.TaskId();

    auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
    request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());

    std::unique_lock<std::mutex> guard(mutex_);

    auto iter = actor_states_.find(actor_id);
    if (iter == actor_states_.end() ||
        iter->second.state_ == ActorTableData::RECONSTRUCTING) {
      // Actor is not yet created, or is being reconstructed, cache the request
      // and submit after actor is alive.
      // TODO(zhijunfu): it might be possible for a user to specify an invalid
      // actor handle (e.g. from unpickling), in that case it might be desirable
      // to have a timeout to mark it as invalid if it doesn't show up in the
      // specified time.
      auto inserted = pending_requests_[actor_id].emplace(GetRequestNumber(request),
                                                          std::move(request));
      RAY_CHECK(inserted.second);
      RAY_LOG(DEBUG) << "Actor " << actor_id << " is not yet created.";
    } else if (iter->second.state_ == ActorTableData::ALIVE) {
      auto inserted = pending_requests_[actor_id].emplace(GetRequestNumber(request),
                                                          std::move(request));
      RAY_CHECK(inserted.second);
      SendPendingTasks(actor_id);
    } else {
      // Actor is dead, treat the task as failure.
      RAY_CHECK(iter->second.state_ == ActorTableData::DEAD);
      task_finisher_->FailPendingTask(task_id, rpc::ErrorType::ACTOR_DIED);
    }
  });

  // If the task submission subsequently fails, then the client will receive
  // the error in a callback.
  return Status::OK();
}

void CoreWorkerDirectActorTaskSubmitter::HandleActorUpdate(
    const ActorID &actor_id, const ActorTableData &actor_data) {
  std::unique_lock<std::mutex> guard(mutex_);
  actor_states_.erase(actor_id);
  actor_states_.emplace(
      actor_id, ActorStateData(actor_data.state(), actor_data.address().ip_address(),
                               actor_data.address().port()));

  if (actor_data.state() == ActorTableData::ALIVE) {
    // Create a new connection to the actor.
    if (rpc_clients_.count(actor_id) == 0) {
      rpc::WorkerAddress addr = {actor_data.address().ip_address(),
                                 actor_data.address().port()};
      rpc_clients_[actor_id] =
          std::shared_ptr<rpc::CoreWorkerClientInterface>(client_factory_(addr));
    }
    if (pending_requests_.count(actor_id) > 0) {
      SendPendingTasks(actor_id);
    }
  } else {
    // Remove rpc client if it's dead or being reconstructed.
    rpc_clients_.erase(actor_id);

    // If there are pending requests, treat the pending tasks as failed.
    auto pending_it = pending_requests_.find(actor_id);
    if (pending_it != pending_requests_.end()) {
      auto head = pending_it->second.begin();
      while (head != pending_it->second.end()) {
        auto request = std::move(head->second);
        head = pending_it->second.erase(head);
        auto task_id = TaskID::FromBinary(request->task_spec().task_id());
        task_finisher_->FailPendingTask(task_id, rpc::ErrorType::ACTOR_DIED);
      }
      pending_requests_.erase(pending_it);
    }

    next_sequence_number_.erase(actor_id);

    // No need to clean up tasks that have been sent and are waiting for
    // replies. They will be treated as failed once the connection dies.
  }
}

void CoreWorkerDirectActorTaskSubmitter::SendPendingTasks(const ActorID &actor_id) {
  auto &client = rpc_clients_[actor_id];
  RAY_CHECK(client);
  // Submit all pending requests.
  auto &requests = pending_requests_[actor_id];
  auto head = requests.begin();
  while (head != requests.end() && head->first == next_sequence_number_[actor_id]) {
    auto request = std::move(head->second);
    head = requests.erase(head);

    auto num_returns = request->task_spec().num_returns();
    auto task_id = TaskID::FromBinary(request->task_spec().task_id());
    PushActorTask(*client, std::move(request), actor_id, task_id, num_returns);
  }
}

void CoreWorkerDirectActorTaskSubmitter::PushActorTask(
    rpc::CoreWorkerClientInterface &client, std::unique_ptr<rpc::PushTaskRequest> request,
    const ActorID &actor_id, const TaskID &task_id, int num_returns) {
  RAY_LOG(DEBUG) << "Pushing task " << task_id << " to actor " << actor_id;

  auto task_number = GetRequestNumber(request);
  RAY_CHECK(next_sequence_number_[actor_id] == task_number)
      << "Counter was " << task_number << " expected " << next_sequence_number_[actor_id];
  next_sequence_number_[actor_id]++;

  auto status = client.PushActorTask(
      std::move(request),
      [this, task_id](Status status, const rpc::PushTaskReply &reply) {
        if (!status.ok()) {
          // Note that this might be the __ray_terminate__ task, so we don't log
          // loudly with ERROR here.
          RAY_LOG(INFO) << "Task failed with error: " << status;
          task_finisher_->FailPendingTask(task_id, rpc::ErrorType::ACTOR_DIED);
        } else {
          task_finisher_->CompletePendingTask(task_id, reply);
        }
      });
  if (!status.ok()) {
    task_finisher_->FailPendingTask(task_id, rpc::ErrorType::ACTOR_DIED);
  }
}

bool CoreWorkerDirectActorTaskSubmitter::IsActorAlive(const ActorID &actor_id) const {
  std::unique_lock<std::mutex> guard(mutex_);

  auto iter = actor_states_.find(actor_id);
  return (iter != actor_states_.end() && iter->second.state_ == ActorTableData::ALIVE);
}

CoreWorkerDirectTaskReceiver::CoreWorkerDirectTaskReceiver(
    WorkerContext &worker_context, boost::asio::io_service &main_io_service,
    const TaskHandler &task_handler, const std::function<void()> &exit_handler)
    : worker_context_(worker_context),
      task_handler_(task_handler),
      exit_handler_(exit_handler),
      task_main_io_service_(main_io_service) {}

void CoreWorkerDirectTaskReceiver::Init(RayletClient &raylet_client) {
  waiter_.reset(new DependencyWaiterImpl(raylet_client));
}

void CoreWorkerDirectTaskReceiver::SetMaxActorConcurrency(int max_concurrency) {
  if (max_concurrency != max_concurrency_) {
    RAY_LOG(INFO) << "Creating new thread pool of size " << max_concurrency;
    RAY_CHECK(pool_ == nullptr) << "Cannot change max concurrency at runtime.";
    pool_.reset(new BoundedExecutor(max_concurrency));
    max_concurrency_ = max_concurrency;
  }
}

void CoreWorkerDirectTaskReceiver::SetActorAsAsync() {
  if (!is_asyncio_) {
    RAY_LOG(DEBUG) << "Setting direct actor as async, creating new fiber thread.";

    // The main thread will be used the creating new fibers.
    // The fiber_runner_thread_ will run all fibers.
    // boost::fibers::algo::shared_work allows two threads to transparently
    // share all the fibers.
    boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();

    fiber_runner_thread_ = std::thread([&]() {
      boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();

      // The event here is used to make sure fiber_runner_thread_ never terminates.
      // Because fiber_shutdown_event_ is never notified, fiber_runner_thread_ will
      // immediately start working on any ready fibers.
      fiber_shutdown_event_.Wait();
    });
    fiber_rate_limiter_.reset(new FiberRateLimiter(max_concurrency_));
    is_asyncio_ = true;
  }
};

void CoreWorkerDirectTaskReceiver::HandlePushTask(
    const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(waiter_ != nullptr) << "Must call init() prior to use";
  const TaskSpecification task_spec(request.task_spec());
  RAY_LOG(DEBUG) << "Received task " << task_spec.TaskId();
  if (task_spec.IsActorTask() && !worker_context_.CurrentTaskIsDirectCall()) {
    send_reply_callback(Status::Invalid("This actor doesn't accept direct calls."),
                        nullptr, nullptr);
    return;
  }
  SetMaxActorConcurrency(worker_context_.CurrentActorMaxConcurrency());
  if (worker_context_.CurrentActorIsAsync()) {
    SetActorAsAsync();
  }

  // TODO(ekl) resolving object dependencies is expensive and requires an IPC to
  // the raylet, which is a central bottleneck. In the future, we should inline
  // dependencies that are small and already known to be local to the client.
  std::vector<ObjectID> dependencies;
  for (size_t i = 0; i < task_spec.NumArgs(); ++i) {
    int count = task_spec.ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      dependencies.push_back(task_spec.ArgId(i, j));
    }
  }

  auto it = scheduling_queue_.find(task_spec.CallerId());
  if (it == scheduling_queue_.end()) {
    auto result = scheduling_queue_.emplace(
        task_spec.CallerId(),
        std::unique_ptr<SchedulingQueue>(new SchedulingQueue(
            task_main_io_service_, *waiter_, pool_, is_asyncio_, fiber_rate_limiter_)));
    it = result.first;
  }

  auto accept_callback = [this, reply, send_reply_callback, task_spec]() {
    // We have posted an exit task onto the main event loop,
    // so shouldn't bother executing any further work.
    if (exiting_) return;

    auto num_returns = task_spec.NumReturns();
    RAY_CHECK(num_returns > 0);
    if (task_spec.IsActorCreationTask() || task_spec.IsActorTask()) {
      // Decrease to account for the dummy object id.
      num_returns--;
    }

    // TODO(edoakes): resource IDs are currently kept track of in the
    // raylet, need to come up with a solution for this.
    ResourceMappingType resource_ids;
    std::vector<std::shared_ptr<RayObject>> return_objects;
    auto status = task_handler_(task_spec, resource_ids, &return_objects);
    bool objects_valid = return_objects.size() == num_returns;
    if (objects_valid) {
      for (size_t i = 0; i < return_objects.size(); i++) {
        auto return_object = reply->add_return_objects();
        ObjectID id = ObjectID::ForTaskReturn(
            task_spec.TaskId(), /*index=*/i + 1,
            /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT));
        return_object->set_object_id(id.Binary());

        // The object is nullptr if it already existed in the object store.
        const auto &result = return_objects[i];
        if (result == nullptr || result->GetData()->IsPlasmaBuffer()) {
          return_object->set_in_plasma(true);
        } else {
          if (result->GetData() != nullptr) {
            return_object->set_data(result->GetData()->Data(), result->GetData()->Size());
          }
          if (result->GetMetadata() != nullptr) {
            return_object->set_metadata(result->GetMetadata()->Data(),
                                        result->GetMetadata()->Size());
          }
        }
      }
    }
    if (status.IsSystemExit()) {
      // In Python, SystemExit can only be raised on the main thread. To
      // work around this when we are executing tasks on worker threads,
      // we re-post the exit event explicitly on the main thread.
      exiting_ = true;
      if (objects_valid) {
        send_reply_callback(Status::OK(), nullptr, nullptr);
      } else {
        send_reply_callback(Status::SystemExit(), nullptr, nullptr);
      }
      task_main_io_service_.post([this]() { exit_handler_(); });
    } else {
      RAY_CHECK(objects_valid) << return_objects.size() << "  " << num_returns;
      send_reply_callback(status, nullptr, nullptr);
    }
  };

  auto reject_callback = [send_reply_callback]() {
    send_reply_callback(Status::Invalid("client cancelled stale rpc"), nullptr, nullptr);
  };

  it->second->Add(request.sequence_number(), request.client_processed_up_to(),
                  accept_callback, reject_callback, dependencies);
}

void CoreWorkerDirectTaskReceiver::HandleDirectActorCallArgWaitComplete(
    const rpc::DirectActorCallArgWaitCompleteRequest &request,
    rpc::DirectActorCallArgWaitCompleteReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Arg wait complete for tag " << request.tag();
  waiter_->OnWaitComplete(request.tag());
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace ray
