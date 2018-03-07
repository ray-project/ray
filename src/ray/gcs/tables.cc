#include "ray/gcs/tables.h"

#include "ray/gcs/client.h"

namespace ray {

namespace gcs {

void ClientTable::RegisterClientAddedCallback(const Callback &callback) {
  client_added_callback_ = callback;
  // Call the callback for any added clients that are cached.
  for (const auto &entry : client_cache_) {
    if (!entry.first.is_nil() && entry.second.is_insertion) {
      auto data = std::make_shared<ClientTableDataT>(entry.second);
      client_added_callback_(client_, entry.first, data);
    }
  }
}

void ClientTable::RegisterClientRemovedCallback(const Callback &callback) {
  client_removed_callback_ = callback;
  // Call the callback for any removed clients that are cached.
  for (const auto &entry : client_cache_) {
    if (!entry.first.is_nil() && !entry.second.is_insertion) {
      auto data = std::make_shared<ClientTableDataT>(entry.second);
      client_removed_callback_(client_, entry.first, data);
    }
  }
}

void ClientTable::HandleNotification(AsyncGcsClient *client,
                                     const ClientID &channel_id,
                                     std::shared_ptr<ClientTableDataT> data) {
  ClientID client_id = ClientID::from_binary(data->client_id);
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
    bool is_deleted = !data->is_insertion;
    is_new = (was_inserted && is_deleted);
    // Once a client with a given ID has been removed, it should never be added
    // again. If the entry was in the cache and the client was deleted, check
    // that this new notification is not an insertion.
    RAY_CHECK(!entry->second.is_insertion && data->is_insertion)
        << "Notification for addition of a client that was already removed";
  }

  // Add the notification to our cache. Notifications are idempotent.
  client_cache_[client_id] = *data;

  // If the notification is new, call any registered callbacks.
  if (is_new) {
    if (data->is_insertion) {
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
                                  const ClientID &client_id,
                                  std::shared_ptr<ClientTableDataT> data) {
  RAY_CHECK(client_id == client_id_) << client_id.hex() << " "
                                     << client_id_.hex();
}

const ClientID &ClientTable::GetLocalClientId() {
  return client_id_;
}

const ClientTableDataT &ClientTable::GetLocalClient() {
  return local_client_;
}

Status ClientTable::Connect() {
  RAY_CHECK(local_client_.is_insertion)
      << "Tried to reconnect a disconnected client.";

  auto data = std::make_shared<ClientTableDataT>(local_client_);
  // Callback for a notification from the client table.
  auto notification_callback = [this](AsyncGcsClient *client,
                                      const ClientID &channel_id,
                                      std::shared_ptr<ClientTableDataT> data) {
    return HandleNotification(client, channel_id, data);
  };
  // Callback to subscribe to the client table once we've successfully added
  // ourselves.
  auto add_callback = [this](AsyncGcsClient *client, const ClientID &id,
                             std::shared_ptr<ClientTableDataT> data) {
    HandleConnected(client, id, data);
  };
  // Callback for subscription success.
  auto subscription_callback = [this, data, add_callback](
      AsyncGcsClient *c, const ClientID &id,
      std::shared_ptr<ClientTableDataT> d) {
    return Add(JobID::nil(), client_id_, data, add_callback);
  };
  return Subscribe(JobID::nil(), ClientID::nil(), notification_callback,
                   subscription_callback);
}

Status ClientTable::Disconnect() {
  local_client_.is_insertion = false;
  auto data = std::make_shared<ClientTableDataT>(local_client_);
  auto add_callback = [this](AsyncGcsClient *client, const ClientID &id,
                             std::shared_ptr<ClientTableDataT> data) {
    HandleConnected(client, id, data);
  };
  return Add(JobID::nil(), client_id_, data, add_callback);
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

}  // namespace gcs

}  // namespace ray
