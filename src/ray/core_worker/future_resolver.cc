#include "ray/core_worker/future_resolver.h"

namespace ray {

void FutureResolver::ResolveFutureAsync(const ObjectID &object_id, const TaskID &owner_id,
                                        const rpc::Address &owner_address) {
  RAY_CHECK(object_id.IsDirectCallType());
  absl::MutexLock lock(&mu_);
  auto it = owner_clients_.find(owner_id);
  if (it == owner_clients_.end()) {
    auto client = std::shared_ptr<rpc::CoreWorkerClientInterface>(
        client_factory_(owner_address.ip_address(), owner_address.port()));
    it = owner_clients_.emplace(owner_id, std::move(client)).first;
  }

  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_id(owner_id.Binary());
  RAY_CHECK_OK(it->second->GetObjectStatus(
      request,
      [this, object_id](const Status &status, const rpc::GetObjectStatusReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Error retrieving the value of object ID " << object_id
                         << " that was deserialized: " << status.ToString();
        }
        // Either the owner is gone or the owner replied that the object has
        // been created. In both cases, we can now try to fetch the object via
        // plasma.
        RAY_CHECK_OK(in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                           object_id));
      }));
}

}  // namespace ray
