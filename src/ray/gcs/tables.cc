#include "ray/gcs/tables.h"

#include "common_protocol.h"
#include "ray/gcs/client.h"

namespace ray {

namespace gcs {

template <typename ID, typename Data>
Status Log<ID, Data>::Append(const JobID &job_id, const ID &id,
                             std::shared_ptr<DataT> data, const WriteCallback &done) {
  auto d = std::shared_ptr<CallbackData>(
      new CallbackData({id, data, nullptr, nullptr, this, client_}));
  int64_t callback_index =
      RedisCallbackManager::instance().add([d, done](const std::string &data) {
        RAY_CHECK(data.empty());
        if (done != nullptr) {
          (done)(d->client, d->id, d->data);
        }
        return true;
      });
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, data.get()));
  return context_->RunAsync("RAY.TABLE_APPEND", id, fbb.GetBufferPointer(), fbb.GetSize(),
                            prefix_, pubsub_channel_, callback_index);
}

template <typename ID, typename Data>
Status Log<ID, Data>::AppendAt(const JobID &job_id, const ID &id,
                               std::shared_ptr<DataT> data, const WriteCallback &done,
                               const WriteCallback &failure, int log_length) {
  auto d = std::shared_ptr<CallbackData>(
      new CallbackData({id, data, nullptr, nullptr, this, client_}));
  int64_t callback_index =
      RedisCallbackManager::instance().add([d, done, failure](const std::string &data) {
        if (data.empty()) {
          if (done != nullptr) {
            (done)(d->client, d->id, d->data);
          }
        } else {
          if (failure != nullptr) {
            (failure)(d->client, d->id, d->data);
          }
        }
        return true;
      });
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, data.get()));
  return context_->RunAsync("RAY.TABLE_APPEND", id, fbb.GetBufferPointer(), fbb.GetSize(),
                            prefix_, pubsub_channel_, callback_index, log_length);
}

template <typename ID, typename Data>
Status Log<ID, Data>::Lookup(const JobID &job_id, const ID &id, const Callback &lookup) {
  auto d = std::shared_ptr<CallbackData>(
      new CallbackData({id, nullptr, lookup, nullptr, this, client_}));
  int64_t callback_index =
      RedisCallbackManager::instance().add([d](const std::string &data) {
        if (d->callback != nullptr) {
          std::vector<DataT> results;
          if (!data.empty()) {
            auto root = flatbuffers::GetRoot<GcsTableEntry>(data.data());
            RAY_CHECK(from_flatbuf(*root->id()) == d->id);
            for (size_t i = 0; i < root->entries()->size(); i++) {
              DataT result;
              auto data_root =
                  flatbuffers::GetRoot<Data>(root->entries()->Get(i)->data());
              data_root->UnPackTo(&result);
              results.emplace_back(std::move(result));
            }
          }
          (d->callback)(d->client, d->id, results);
        }
        return true;
      });
  std::vector<uint8_t> nil;
  return context_->RunAsync("RAY.TABLE_LOOKUP", id, nil.data(), nil.size(), prefix_,
                            pubsub_channel_, callback_index);
}

template <typename ID, typename Data>
Status Log<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                                const Callback &subscribe,
                                const SubscriptionCallback &done) {
  RAY_CHECK(subscribe_callback_index_ == -1)
      << "Client called Subscribe twice on the same table";
  auto d = std::shared_ptr<CallbackData>(
      new CallbackData({client_id, nullptr, subscribe, done, this, client_}));
  int64_t callback_index =
      RedisCallbackManager::instance().add([this, d](const std::string &data) {
        if (data.empty()) {
          // No notification data is provided. This is the callback for the
          // initial subscription request.
          if (d->subscription_callback != nullptr) {
            (d->subscription_callback)(d->client);
          }
        } else {
          // Data is provided. This is the callback for a message.
          if (d->callback != nullptr) {
            // Parse the notification.
            auto root = flatbuffers::GetRoot<GcsTableEntry>(data.data());
            ID id = UniqueID::nil();
            if (root->id()->size() > 0) {
              id = from_flatbuf(*root->id());
            }
            std::vector<DataT> results;
            for (size_t i = 0; i < root->entries()->size(); i++) {
              DataT result;
              auto data_root =
                  flatbuffers::GetRoot<Data>(root->entries()->Get(i)->data());
              data_root->UnPackTo(&result);
              results.emplace_back(std::move(result));
            }
            (d->callback)(d->client, id, results);
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
Status Log<ID, Data>::RequestNotifications(const JobID &job_id, const ID &id,
                                           const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client requested notifications on a key before Subscribe completed";
  return context_->RunAsync("RAY.TABLE_REQUEST_NOTIFICATIONS", id, client_id.data(),
                            client_id.size(), prefix_, pubsub_channel_,
                            /*callback_index=*/-1);
}

template <typename ID, typename Data>
Status Log<ID, Data>::CancelNotifications(const JobID &job_id, const ID &id,
                                          const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client canceled notifications on a key before Subscribe completed";
  return context_->RunAsync("RAY.TABLE_CANCEL_NOTIFICATIONS", id, client_id.data(),
                            client_id.size(), prefix_, pubsub_channel_,
                            /*callback_index=*/-1);
}

template <typename ID, typename Data>
Status Table<ID, Data>::Add(const JobID &job_id, const ID &id,
                            std::shared_ptr<DataT> data, const WriteCallback &done) {
  auto d = std::shared_ptr<CallbackData>(
      new CallbackData({id, data, nullptr, nullptr, this, client_}));
  int64_t callback_index =
      RedisCallbackManager::instance().add([d, done](const std::string &data) {
        if (done != nullptr) {
          (done)(d->client, d->id, d->data);
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
  return Log<ID, Data>::Lookup(job_id, id,
                               [lookup, failure](AsyncGcsClient *client, const ID &id,
                                                 const std::vector<DataT> &data) {
                                 if (data.empty()) {
                                   if (failure != nullptr) {
                                     (failure)(client, id);
                                   }
                                 } else {
                                   RAY_CHECK(data.size() == 1);
                                   if (lookup != nullptr) {
                                     (lookup)(client, id, data[0]);
                                   }
                                 }
                               });
}

template <typename ID, typename Data>
Status Table<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                                  const Callback &subscribe,
                                  const SubscriptionCallback &done) {
  return Log<ID, Data>::Subscribe(
      job_id, client_id,
      [subscribe](AsyncGcsClient *client, const ID &id, const std::vector<DataT> &data) {
        RAY_CHECK(data.size() == 1);
        subscribe(client, id, data[0]);
      },
      done);
}

void ClientTable::RegisterClientAddedCallback(const ClientTableCallback &callback) {
  client_added_callback_ = callback;
  // Call the callback for any added clients that are cached.
  for (const auto &entry : client_cache_) {
    if (!entry.first.is_nil() && entry.second.is_insertion) {
      client_added_callback_(client_, entry.first, entry.second);
    }
  }
}

void ClientTable::RegisterClientRemovedCallback(const ClientTableCallback &callback) {
  client_removed_callback_ = callback;
  // Call the callback for any removed clients that are cached.
  for (const auto &entry : client_cache_) {
    if (!entry.first.is_nil() && !entry.second.is_insertion) {
      client_removed_callback_(client_, entry.first, entry.second);
    }
  }
}

void ClientTable::HandleNotification(AsyncGcsClient *client,
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

void ClientTable::HandleConnected(AsyncGcsClient *client,
                                  const std::shared_ptr<ClientTableDataT> data) {
  auto connected_client_id = ClientID::from_binary(data->client_id);
  RAY_CHECK(client_id_ == connected_client_id) << connected_client_id.hex() << " "
                                               << client_id_.hex();
}

const ClientID &ClientTable::GetLocalClientId() { return client_id_; }

const ClientTableDataT &ClientTable::GetLocalClient() { return local_client_; }

Status ClientTable::Connect(const ClientTableDataT &local_client) {
  RAY_CHECK(!disconnected_) << "Tried to reconnect a disconnected client.";

  RAY_CHECK(local_client.client_id == local_client_.client_id);
  local_client_ = local_client;

  auto data = std::make_shared<ClientTableDataT>(local_client_);
  data->is_insertion = true;
  // Callback for a notification from the client table.
  auto notification_callback = [this](
      AsyncGcsClient *client, const UniqueID &log_key,
      const std::vector<ClientTableDataT> &notifications) {
    RAY_CHECK(log_key == client_log_key_);
    for (auto &notification : notifications) {
      HandleNotification(client, notification);
    }
  };
  // Callback to handle our own successful connection once we've added
  // ourselves.
  auto add_callback = [this](AsyncGcsClient *client, const UniqueID &log_key,
                             std::shared_ptr<ClientTableDataT> data) {
    RAY_CHECK(log_key == client_log_key_);
    HandleConnected(client, data);
  };
  // Callback to add ourselves once we've successfully subscribed.
  auto subscription_callback = [this, data, add_callback](AsyncGcsClient *c) {
    // Mark ourselves as deleted if we called Disconnect() since the last
    // Connect() call.
    if (disconnected_) {
      data->is_insertion = false;
    }
    RAY_CHECK_OK(RequestNotifications(JobID::nil(), client_log_key_, client_id_));
    RAY_CHECK_OK(Append(JobID::nil(), client_log_key_, data, add_callback));
  };
  return Subscribe(JobID::nil(), client_id_, notification_callback,
                   subscription_callback);
}

Status ClientTable::Disconnect() {
  auto data = std::make_shared<ClientTableDataT>(local_client_);
  data->is_insertion = true;
  auto add_callback = [this](AsyncGcsClient *client, const ClientID &id,
                             std::shared_ptr<ClientTableDataT> data) {
    HandleConnected(client, data);
    RAY_CHECK_OK(CancelNotifications(JobID::nil(), client_log_key_, id));
  };
  RAY_RETURN_NOT_OK(Append(JobID::nil(), client_log_key_, data, add_callback));
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

template class Log<ObjectID, ObjectTableData>;
template class Log<TaskID, ray::protocol::Task>;
template class Table<TaskID, ray::protocol::Task>;
template class Table<TaskID, TaskTableData>;
template class Log<TaskID, TaskReconstructionData>;
template class Table<ClientID, HeartbeatTableData>;

}  // namespace gcs

}  // namespace ray
