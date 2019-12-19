#include "ray/gcs/subscription_executor.h"

namespace ray {

namespace gcs {

template <typename ID, typename Data, typename Table>
Status SubscriptionExecutor<ID, Data, Table>::AsyncSubscribeAll(
    const ClientID &client_id, const SubscribeCallback<ID, Data> &subscribe,
    const StatusCallback &done) {
  // TODO(micafan) Optimize the lock when necessary.
  // Consider avoiding locking in single-threaded processes.
  std::unique_lock<std::mutex> lock(mutex_);

  if (subscribe_all_callback_ != nullptr) {
    RAY_LOG(DEBUG) << "Duplicate subscription! Already subscribed to all elements.";
    return Status::Invalid("Duplicate subscription!");
  }

  if (registration_status_ != RegistrationStatus::kNotRegistered) {
    if (subscribe != nullptr) {
      RAY_LOG(DEBUG) << "Duplicate subscription! Already subscribed to specific elements"
                        ", can't subscribe to all elements.";
      return Status::Invalid("Duplicate subscription!");
    }
  }

  if (registration_status_ == RegistrationStatus::kRegistered) {
    // Already registered to GCS, just invoke the `done` callback.
    lock.unlock();
    if (done != nullptr) {
      done(Status::OK());
    }
    return Status::OK();
  }

  // Registration to GCS is not finished yet, add the `done` callback to the pending list
  // to be invoked when registration is done.
  if (done != nullptr) {
    pending_subscriptions_.emplace_back(done);
  }

  // If there's another registration request that's already on-going, then wait for it
  // to finish.
  if (registration_status_ == RegistrationStatus::kRegistering) {
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
      std::unique_lock<std::mutex> lock(mutex_);
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

  auto on_done = [this](RedisGcsClient *client) {
    std::list<StatusCallback> pending_callbacks;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      registration_status_ = RegistrationStatus::kRegistered;
      pending_callbacks.swap(pending_subscriptions_);
      RAY_CHECK(pending_subscriptions_.empty());
    }

    for (const auto &callback : pending_callbacks) {
      callback(Status::OK());
    }
  };

  Status status = table_.Subscribe(JobID::Nil(), client_id, on_subscribe, on_done);
  if (status.ok()) {
    registration_status_ = RegistrationStatus::kRegistering;
    subscribe_all_callback_ = subscribe;
  }

  return status;
}

template <typename ID, typename Data, typename Table>
Status SubscriptionExecutor<ID, Data, Table>::AsyncSubscribe(
    const ClientID &client_id, const ID &id, const SubscribeCallback<ID, Data> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(client_id != ClientID::Nil());

  // NOTE(zhijunfu): `Subscribe` and other operations use different redis contexts,
  // thus we need to call `RequestNotifications` in the Subscribe callback to ensure
  // it's processed after the `Subscribe` request. Otherwise if `RequestNotifications`
  // is processed first we will miss the initial notification.
  auto on_subscribe_done = [this, client_id, id, subscribe, done](Status status) {
    auto on_request_notification_done = [this, done, id](Status status) {
      if (!status.ok()) {
        std::unique_lock<std::mutex> lock(mutex_);
        id_to_callback_map_.erase(id);
      }
      if (done != nullptr) {
        done(status);
      }
    };

    {
      std::unique_lock<std::mutex> lock(mutex_);
      status = table_.RequestNotifications(JobID::Nil(), id, client_id,
                                           on_request_notification_done);
      if (!status.ok()) {
        id_to_callback_map_.erase(id);
      }
    }
  };

  {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto it = id_to_callback_map_.find(id);
    if (it != id_to_callback_map_.end()) {
      RAY_LOG(DEBUG) << "Duplicate subscription to id " << id << " client_id "
                     << client_id;
      return Status::Invalid("Duplicate subscription to element!");
    }
    id_to_callback_map_[id] = subscribe;
  }

  auto status = AsyncSubscribeAll(client_id, nullptr, on_subscribe_done);
  if (!status.ok()) {
    std::unique_lock<std::mutex> lock(mutex_);
    id_to_callback_map_.erase(id);
  }
  return status;
}

template <typename ID, typename Data, typename Table>
Status SubscriptionExecutor<ID, Data, Table>::AsyncUnsubscribe(
    const ClientID &client_id, const ID &id, const StatusCallback &done) {
  SubscribeCallback<ID, Data> subscribe = nullptr;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto it = id_to_callback_map_.find(id);
    if (it == id_to_callback_map_.end()) {
      RAY_LOG(DEBUG) << "Invalid Unsubscribe! id " << id << " client_id " << client_id;
      return Status::Invalid("Invalid Unsubscribe, no existing subscription found.");
    }
    subscribe = std::move(it->second);
    id_to_callback_map_.erase(it);
  }

  RAY_CHECK(subscribe != nullptr);
  auto on_done = [this, id, subscribe, done](Status status) {
    if (!status.ok()) {
      std::unique_lock<std::mutex> lock(mutex_);
      const auto it = id_to_callback_map_.find(id);
      if (it != id_to_callback_map_.end()) {
        // The initial AsyncUnsubscribe deleted the callback, but the client
        // has subscribed again in the meantime. This new callback will be
        // called if we receive more notifications.
        RAY_LOG(WARNING)
            << "Client called AsyncSubscribe on " << id
            << " while AsyncUnsubscribe was pending, but the unsubscribe failed.";
      } else {
        // The Unsubscribe failed, so restore the initial callback.
        id_to_callback_map_[id] = subscribe;
      }
    }
    if (done != nullptr) {
      done(status);
    }
  };

  return table_.CancelNotifications(JobID::Nil(), id, client_id, on_done);
}

template class SubscriptionExecutor<ActorID, ActorTableData, ActorTable>;
template class SubscriptionExecutor<ActorID, ActorTableData, DirectActorTable>;
template class SubscriptionExecutor<JobID, JobTableData, JobTable>;

}  // namespace gcs

}  // namespace ray
