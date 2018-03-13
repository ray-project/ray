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

void ClientTable::HandleNotification(AsyncGcsClient *client, const ClientID &channel_id,
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
    if (!entry->second.is_insertion) {
      RAY_CHECK(!data->is_insertion)
          << "Notification for addition of a client that was already removed:"
          << client_id.hex();
    }
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

void ClientTable::HandleConnected(AsyncGcsClient *client, const ClientID &client_id,
                                  std::shared_ptr<ClientTableDataT> data) {
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
                                      std::shared_ptr<ClientTableDataT> data) {
    return HandleNotification(client, channel_id, data);
  };
  // Callback to handle our own successful connection once we've added
  // ourselves.
  auto add_callback = [this](AsyncGcsClient *client, const ClientID &id,
                             std::shared_ptr<ClientTableDataT> data) {
    HandleConnected(client, id, data);
  };
  // Callback to add ourselves once we've successfully subscribed.
  auto subscription_callback = [this, data, add_callback](
      AsyncGcsClient *c, const ClientID &id, std::shared_ptr<ClientTableDataT> d) {
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
                             std::shared_ptr<ClientTableDataT> data) {
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

}  // namespace gcs

}  // namespace ray
