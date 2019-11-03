#include "ray/core_worker/transport/direct_task_transport.h"

namespace ray {

Status CoreWorkerDirectTaskSubmitter::SubmitTask(const TaskSpecification &task_spec) {
  // TODO(ekl) should have a queue per distinct resource type required
  RequestNewWorkerIfNeeded(task_spec);
  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  auto msg = task_spec.GetMutableMessage();
  request->mutable_task_spec()->Swap(&msg);
  {
    absl::MutexLock lock(&mu_);
    queued_tasks_.push_back(std::move(request));
  }
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::HandleWorkerLeaseGranted(const std::string &address,
                                                             int port) {
  WorkerAddress addr = std::make_pair(address, port);

  // Setup client state for this worker.
  {
    absl::MutexLock lock(&mu_);
    worker_request_pending_ = false;

    auto it = client_cache_.find(addr);
    if (it == client_cache_.end()) {
      client_cache_[addr] =
          std::unique_ptr<rpc::DirectActorClient>(new rpc::DirectActorClient(
              address, port, direct_actor_submitter_.CallManager()));
      RAY_LOG(INFO) << "Connected to " << address << ":" << port;
    }
  }

  // Try to assign it work.
  WorkerIdle(addr);
}

void CoreWorkerDirectTaskSubmitter::WorkerIdle(const WorkerAddress &addr) {
  absl::MutexLock lock(&mu_);
  if (queued_tasks_.empty()) {
    RAY_CHECK_OK(raylet_client_.ReturnWorker(addr.second));
  } else {
    auto &client = *client_cache_[addr];
    PushTask(addr, client, std::move(queued_tasks_.front()));
    queued_tasks_.pop_front();
  }
}

void CoreWorkerDirectTaskSubmitter::RequestNewWorkerIfNeeded(
    const TaskSpecification &resource_spec) {
  absl::MutexLock lock(&mu_);
  if (worker_request_pending_) {
    return;
  }
  RAY_CHECK_OK(raylet_client_.RequestWorkerLease(resource_spec));
  worker_request_pending_ = true;
}

void CoreWorkerDirectTaskSubmitter::PushTask(
    const WorkerAddress &addr, rpc::DirectActorClient &client,
    std::unique_ptr<rpc::PushTaskRequest> request) {
  auto status = client.PushTaskImmediate(
      std::move(request), [this, addr](Status status, const rpc::PushTaskReply &reply) {
        if (!status.ok()) {
          RAY_LOG(FATAL) << "Task failed with error: " << status;
        }
        for (int i = 0; i < reply.return_objects_size(); i++) {
          const auto &return_object = reply.return_objects(i);
          ObjectID object_id = ObjectID::FromBinary(return_object.object_id());
          std::shared_ptr<LocalMemoryBuffer> data_buffer;
          if (return_object.data().size() > 0) {
            data_buffer = std::make_shared<LocalMemoryBuffer>(
                const_cast<uint8_t *>(
                    reinterpret_cast<const uint8_t *>(return_object.data().data())),
                return_object.data().size());
          }
          std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
          if (return_object.metadata().size() > 0) {
            metadata_buffer = std::make_shared<LocalMemoryBuffer>(
                const_cast<uint8_t *>(
                    reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
                return_object.metadata().size());
          }
          RAY_CHECK_OK(
              store_provider_->Put(RayObject(data_buffer, metadata_buffer), object_id));
          WorkerIdle(addr);
        }
      });
  RAY_CHECK_OK(status);
}
};  // namespace ray
