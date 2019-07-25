#include "ray/gcs/subscribe_executor.h"

namespace ray {

namespace gcs {

template <typename ID, typename Data, typename Table>
Status SubscribeExecutor<ID, Data, Table>::AsyncSubscribe(
    const ClientID &client_id, const SubscribeCallback<ID, Data> &subscribe,
    const StatusCallback &done) {
  // TODO(micafan) Optimize the lock when necessary.
  std::lock_guard<std::mutex> lock(mutex_);

  if (registered_) {
    return Status::OK();
  }

  auto on_subscribe = [this, subscribe](RedisGcsClient *client, const ID &id,
                                        const std::vector<Data> &result) {
    if (result.empty()) {
      return;
    }

    SubscribeCallback<ID, Data> callback = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = id_to_request_map_.find(id);
      if (it != id_to_request_map_.end()) {
        callback = it->second.subscribe_;
      }
    }
    if (callback != nullptr) {
      callback(id, result.back());
    } else if (subscribe != nullptr) {
      subscribe(id, result.back());
    }
  };

  auto on_done = [this, done](RedisGcsClient *client) {
    if (done != nullptr) {
      done(Status::OK());
    }
  };

  Status status = table_.Subscribe(JobID::Nil(), client_id, on_subscribe, on_done);
  if (status.ok()) {
    registered_ = true;
  }

  return status;
}

template <typename ID, typename Data, typename Table>
Status SubscribeExecutor<ID, Data, Table>::AsyncSubscribe(
    const ClientID &client_id, const ID &id, const SubscribeCallback<ID, Data> &subscribe,
    const StatusCallback &done) {
  Status status = AsyncSubscribe(client_id, nullptr, nullptr);
  if (!status.ok()) {
    return status;
  }

  status = table_.RequestNotifications(JobID::Nil(), id, client_id, done);
  if (status.ok()) {
    SubscribeCallbacks callbacks(subscribe, done);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      id_to_request_map_[id] = std::move(callbacks);
    }
  }
  return status;
}

template <typename ID, typename Data, typename Table>
Status SubscribeExecutor<ID, Data, Table>::AsyncUnsubscribe(const ClientID &client_id,
                                                            const ID &id,
                                                            const StatusCallback &done) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = id_to_request_map_.find(id);
    RAY_CHECK(it != id_to_request_map_.end());
  }

  auto on_done = [this, id, done](Status status) {
    if (status.ok()) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = id_to_request_map_.find(id);
        if (it != id_to_request_map_.end()) {
          it->second.subscribe_ = nullptr;
          it->second.done_ = nullptr;
        }
      }
    }
    if (done != nullptr) {
      done(status);
    }
  };

  return table_.CancelNotifications(JobID::Nil(), id, client_id, on_done);
}

template class SubscribeExecutor<ActorID, ActorTableData, ActorTable>;

}  // namespace gcs

}  // namespace ray
