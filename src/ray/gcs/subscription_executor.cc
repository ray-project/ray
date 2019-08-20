#include "ray/gcs/subscription_executor.h"

namespace ray {

namespace gcs {

template <typename ID, typename Data, typename Table>
Status SubscriptionExecutor<ID, Data, Table>::AsyncSubscribe(
    const ClientID &client_id, const SubscribeCallback<ID, Data> &subscribe,
    const StatusCallback &done) {
  // TODO(micafan) Optimize the lock when necessary.
  // Consider avoiding locking in single-threaded processes.
  std::lock_guard<std::mutex> lock(mutex_);

  if (subscribe_all_callback_ != nullptr) {
    RAY_LOG(DEBUG) << "Duplicate subscription! Already subscribed to all elements.";
    return Status::Invalid("Duplicate subscription!");
  }

  if (registered_) {
    if (subscribe != nullptr) {
      RAY_LOG(DEBUG) << "Duplicate subscription! Already subscribed to specific elements"
                        ", can't subscribe to all elements.";
      return Status::Invalid("Duplicate subscription!");
    }
    return Status::OK();
  }

  auto on_subscribe = [this](RedisGcsClient *client, const ID &id,
                             const std::vector<Data> &result) {
    if (result.empty()) {
      return;
    }

    RAY_LOG(DEBUG) << "Subscribe received update of id " << id;

    SubscribeCallback<ID, Data> sub_one_callback = nullptr;
    SubscribeCallback<ID, Data> sub_all_callback = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = id_to_callback_map_.find(id);
      if (it != id_to_callback_map_.end()) {
        sub_one_callback = it->second;
      }
      sub_all_callback = subscribe_all_callback_;
    }
    if (sub_one_callback != nullptr) {
      sub_one_callback(id, result.back());
    }
    if (sub_all_callback != nullptr) {
      RAY_CHECK(sub_one_callback == nullptr);
      sub_all_callback(id, result.back());
    }
  };

  auto on_done = [done](RedisGcsClient *client) {
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
Status SubscriptionExecutor<ID, Data, Table>::AsyncSubscribe(
    const ClientID &client_id, const ID &id, const SubscribeCallback<ID, Data> &subscribe,
    const StatusCallback &done) {
  Status status = AsyncSubscribe(client_id, nullptr, nullptr);
  if (!status.ok()) {
    return status;
  }

  auto on_done = [this, done, id](Status status) {
    if (!status.ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      id_to_callback_map_.erase(id);
    }
    if (done != nullptr) {
      done(status);
    }
  };

  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = id_to_callback_map_.find(id);
    if (it != id_to_callback_map_.end()) {
      RAY_LOG(DEBUG) << "Duplicate subscription to id " << id << " client_id "
                     << client_id;
      return Status::Invalid("Duplicate subscription to element!");
    }
    status = table_.RequestNotifications(JobID::Nil(), id, client_id, on_done);
    if (status.ok()) {
      id_to_callback_map_[id] = subscribe;
    }
  }

  return status;
}

template <typename ID, typename Data, typename Table>
Status SubscriptionExecutor<ID, Data, Table>::AsyncUnsubscribe(
    const ClientID &client_id, const ID &id, const StatusCallback &done) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = id_to_callback_map_.find(id);
    if (it == id_to_callback_map_.end()) {
      RAY_LOG(DEBUG) << "Invalid Unsubscribe! id " << id << " client_id " << client_id;
      return Status::Invalid("Invalid Unsubscribe, no existing subscription found.");
    }
  }

  auto on_done = [this, id, done](Status status) {
    if (status.ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = id_to_callback_map_.find(id);
      if (it != id_to_callback_map_.end()) {
        id_to_callback_map_.erase(it);
      }
    }
    if (done != nullptr) {
      done(status);
    }
  };

  return table_.CancelNotifications(JobID::Nil(), id, client_id, on_done);
}

template class SubscriptionExecutor<ActorID, ActorTableData, ActorTable>;

}  // namespace gcs

}  // namespace ray
