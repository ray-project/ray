#include "ray/gcs/tables.h"

#include "common_protocol.h"
#include "ray/gcs/client.h"

namespace ray {

namespace gcs {

template <typename ID, typename Data>
Status Table<ID, Data>::Add(const JobID &job_id, const ID &id,
                            std::shared_ptr<DataT> data, const Callback &done) {
  auto d = std::shared_ptr<CallbackData>(
      new CallbackData({id, data, done, nullptr, nullptr, this, client_}));
  int64_t callback_index =
      RedisCallbackManager::instance().add([d](const std::vector<std::string> &data) {
        if (d->callback != nullptr) {
          (d->callback)(d->client, d->id, *d->data);
        }
        return true;
      });
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, data.get()));
  return context_->RunAsync("RAY.TABLE_ADD", id, fbb.GetBufferPointer(), fbb.GetSize(),
                            prefix_, pubsub_channel_, callback_index);
}

template <typename ID, typename Data>
Status Table<ID, Data>::Lookup(const JobID &job_id, const ID &id, const Callback &lookup,
                               const FailureCallback &failure) {
  auto d = std::shared_ptr<CallbackData>(
      new CallbackData({id, nullptr, lookup, failure, nullptr, this, client_}));
  int64_t callback_index =
      RedisCallbackManager::instance().add([d](const std::vector<std::string> &data) {
        if (data.empty()) {
          if (d->failure != nullptr) {
            (d->failure)(d->client, d->id);
          }
        } else {
          RAY_CHECK(data.size() == 1);
          if (d->callback != nullptr) {
            DataT result;
            auto root = flatbuffers::GetRoot<Data>(data[0].data());
            root->UnPackTo(&result);
            (d->callback)(d->client, d->id, result);
          }
        }
        return true;
      });
  std::vector<uint8_t> nil;
  return context_->RunAsync("RAY.TABLE_LOOKUP", id, nil.data(), nil.size(), prefix_,
                            pubsub_channel_, callback_index);
}

template <typename ID, typename Data>
Status Table<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                                  const Callback &subscribe,
                                  const SubscriptionCallback &done) {
  RAY_CHECK(subscribe_callback_index_ == -1)
      << "Client called Subscribe twice on the same table";
  auto d = std::shared_ptr<CallbackData>(
      new CallbackData({client_id, nullptr, subscribe, nullptr, done, this, client_}));
  int64_t callback_index = RedisCallbackManager::instance().add(
      [this, d](const std::vector<std::string> &data) {
        if (data.size() == 1 && data[0] == "") {
          // No notification data is provided. This is the callback for the
          // initial subscription request.
          if (d->subscription_callback != nullptr) {
            (d->subscription_callback)(d->client);
          }
        } else {
          // Data is provided. This is the callback for a message.
          RAY_CHECK(data.size() == 1);
          if (d->callback != nullptr) {
            // Parse the notification.
            auto notification = flatbuffers::GetRoot<GcsNotification>(data[0].data());
            ID id = UniqueID::nil();
            if (notification->id()->size() > 0) {
              id = from_flatbuf(*notification->id());
            }
            DataT result;
            auto root = flatbuffers::GetRoot<Data>(notification->data()->data());
            root->UnPackTo(&result);
            (d->callback)(d->client, id, result);
          }
        }
        // We do not delete the callback after calling it since there may be
        // more subscription messages.
        return false;
      });
  subscribe_callback_index_ = callback_index;
  return context_->SubscribeAsync(client_id, pubsub_channel_, callback_index);
}

template <typename ID, typename Data>
Status Table<ID, Data>::RequestNotifications(const JobID &job_id, const ID &id,
                                             const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client requested notifications on a key before Subscribe completed";
  return context_->RunAsync("RAY.TABLE_REQUEST_NOTIFICATIONS", id, client_id.data(),
                            client_id.size(), prefix_, pubsub_channel_,
                            subscribe_callback_index_);
}

template <typename ID, typename Data>
Status Table<ID, Data>::CancelNotifications(const JobID &job_id, const ID &id,
                                            const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client canceled notifications on a key before Subscribe completed";
  return context_->RunAsync("RAY.TABLE_CANCEL_NOTIFICATIONS", id, client_id.data(),
                            client_id.size(), prefix_, pubsub_channel_,
                            /*callback_index=*/-1);
}

void ClientTable::RegisterClientAddedCallback(const ClientTableCallback &callback) {
  client_added_callback_ = callback;
  // Call the callback for any added clients that are cached.
  for (const auto &entry : client_cache_) {
    if (!entry.first.is_nil() && entry.second.is_insertion) {
      client_added_callback_(client_, ClientID::nil(), entry.second);
    }
  }
}

void ClientTable::RegisterClientRemovedCallback(const ClientTableCallback &callback) {
  client_removed_callback_ = callback;
  // Call the callback for any removed clients that are cached.
  for (const auto &entry : client_cache_) {
    if (!entry.first.is_nil() && !entry.second.is_insertion) {
      client_removed_callback_(client_, ClientID::nil(), entry.second);
    }
  }
}

void ClientTable::HandleNotification(AsyncGcsClient *client, const ClientID &channel_id,
                                     const ClientTableDataT &data) {
  ClientID client_id = ClientID::from_binary(data.client_id);
  // It's possible to get duplicate notifications from the client table, so
  // check whether this notification is new.
  auto entry = client_cache_.find(client_id);
  bool is_new;
  if (entry == client_cache_.end()) {
    // If the entry is not in the cache, then the notification is new.
    is_new = true;
  } else {
    // If the entry is in the cache, then the notification is new if the client
    // was alive and is now dead.
    bool was_inserted = entry->second.is_insertion;
    bool is_deleted = !data.is_insertion;
    is_new = (was_inserted && is_deleted);
    // Once a client with a given ID has been removed, it should never be added
    // again. If the entry was in the cache and the client was deleted, check
    // that this new notification is not an insertion.
    if (!entry->second.is_insertion) {
      RAY_CHECK(!data.is_insertion)
          << "Notification for addition of a client that was already removed:"
          << client_id.hex();
    }
  }

  // Add the notification to our cache. Notifications are idempotent.
  client_cache_[client_id] = data;

  // If the notification is new, call any registered callbacks.
  if (is_new) {
    if (data.is_insertion) {
      if (client_added_callback_ != nullptr) {
        client_added_callback_(client, client_id, data);
      }
    } else {
      if (client_removed_callback_ != nullptr) {
        client_removed_callback_(client, client_id, data);
      }
    }
  }
}

void ClientTable::HandleConnected(AsyncGcsClient *client, const ClientID &client_id,
                                  const ClientTableDataT &data) {
  RAY_CHECK(client_id == client_id_) << client_id.hex() << " " << client_id_.hex();
}

const ClientID &ClientTable::GetLocalClientId() { return client_id_; }

const ClientTableDataT &ClientTable::GetLocalClient() { return local_client_; }

Status ClientTable::Connect() {
  RAY_CHECK(!disconnected_) << "Tried to reconnect a disconnected client.";

  auto data = std::make_shared<ClientTableDataT>(local_client_);
  data->is_insertion = true;
  // Callback for a notification from the client table.
  auto notification_callback = [this](AsyncGcsClient *client, const ClientID &channel_id,
                                      const ClientTableDataT &data) {
    return HandleNotification(client, channel_id, data);
  };
  // Callback to handle our own successful connection once we've added
  // ourselves.
  auto add_callback = [this](AsyncGcsClient *client, const ClientID &id,
                             const ClientTableDataT &data) {
    HandleConnected(client, id, data);
  };
  // Callback to add ourselves once we've successfully subscribed.
  auto subscription_callback = [this, data, add_callback](AsyncGcsClient *c) {
    // Mark ourselves as deleted if we called Disconnect() since the last
    // Connect() call.
    if (disconnected_) {
      data->is_insertion = false;
    }
    return Add(JobID::nil(), client_id_, data, add_callback);
  };
  return Subscribe(JobID::nil(), ClientID::nil(), notification_callback,
                   subscription_callback);
}

Status ClientTable::Disconnect() {
  auto data = std::make_shared<ClientTableDataT>(local_client_);
  data->is_insertion = true;
  auto add_callback = [this](AsyncGcsClient *client, const ClientID &id,
                             const ClientTableDataT &data) {
    HandleConnected(client, id, data);
  };
  RAY_RETURN_NOT_OK(Add(JobID::nil(), client_id_, data, add_callback));
  // We successfully added the deletion entry. Mark ourselves as disconnected.
  disconnected_ = true;
  return Status::OK();
}

const ClientTableDataT &ClientTable::GetClient(const ClientID &client_id) {
  RAY_CHECK(!client_id.is_nil());
  auto entry = client_cache_.find(client_id);
  if (entry != client_cache_.end()) {
    return entry->second;
  } else {
    // If the requested client was not found, return a reference to the nil
    // client entry.
    return client_cache_[ClientID::nil()];
  }
}

template class Table<TaskID, ray::protocol::Task>;
template class Table<TaskID, TaskTableData>;
template class Table<ObjectID, ObjectTableData>;

}  // namespace gcs

}  // namespace ray
