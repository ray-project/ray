#include "ray/gcs/subscribe_executor.h"

namespace ray {

namespace gcs {

template <typename ID, typename Data, typename Table>
Status SubscribeExecutor<ID, Data, Table>::AsyncSubscribe(
    const ClientID &client_id, const SubscribeCallback<ID, Data> &subscribe,
    const StatusCallback &done) {
  // TODO(micafan) Optimize the lock when necessary.
  // Maybe avoid locking in the raylet process.
  std::lock_guard<std::mutex> lock(mutex_);

  if (subscribe != nullptr && subscribe_all_callback_ != nullptr) {
    RAY_LOG(INFO) << "Duplicate subscription!";
    return Status::Invalid("Duplicate subscription!");
  }

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
      const auto it = id_to_callback_map_.find(id);
      if (it != id_to_callback_map_.end()) {
        callback = it->second;
      }
    }
    if (callback != nullptr) {
      callback(id, result.back());
    }
    if (subscribe != nullptr) {
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
    subscribe_all_callback_ = subscribe;
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

  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = id_to_callback_map_.find(id);
    if (it != id_to_callback_map_.end()) {
      RAY_LOG(INFO) << "Duplicate subscription to id " << id << " client_id "
                    << client_id;
      return Status::Invalid("Duplicate subscription!");
    }
    status = table_.RequestNotifications(JobID::Nil(), id, client_id, done);
    if (status.ok()) {
      id_to_callback_map_[id] = subscribe;
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
    const auto it = id_to_callback_map_.find(id);
    if (it == id_to_callback_map_.end()) {
      RAY_LOG(INFO) << "Invalid Unsubscribe! id " << id << " client_id " << client_id;
      return Status::Invalid("Invalid Unsubscribe, not found subscription.");
    }
  }

  auto on_done = [this, id, done](Status status) {
    if (status.ok()) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = id_to_callback_map_.find(id);
        if (it != id_to_callback_map_.end()) {
          id_to_callback_map_.erase(it);
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
