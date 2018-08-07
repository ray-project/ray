#include "ray/gcs/tables.h"

#include "common_protocol.h"
#include "ray/gcs/client.h"

namespace {

static const std::string kTableAppendCommand = "RAY.TABLE_APPEND";
static const std::string kChainTableAppendCommand = "RAY.CHAIN.TABLE_APPEND";

static const std::string kTableAddCommand = "RAY.TABLE_ADD";
static const std::string kChainTableAddCommand = "RAY.CHAIN.TABLE_ADD";

std::string GetLogAppendCommand(const ray::gcs::CommandType command_type) {
  if (command_type == ray::gcs::CommandType::kRegular) {
    return kTableAppendCommand;
  } else {
    RAY_CHECK(command_type == ray::gcs::CommandType::kChain);
    return kChainTableAppendCommand;
  }
}

std::string GetTableAddCommand(const ray::gcs::CommandType command_type) {
  if (command_type == ray::gcs::CommandType::kRegular) {
    return kTableAddCommand;
  } else {
    RAY_CHECK(command_type == ray::gcs::CommandType::kChain);
    return kChainTableAddCommand;
  }
}

}  // namespace

namespace ray {

namespace gcs {

template <typename ID, typename Data>
Status Log<ID, Data>::Append(const JobID &job_id, const ID &id,
                             std::shared_ptr<DataT> &dataT, const WriteCallback &done) {
  auto callback = [this, id, dataT, done](const std::string &data) {
    if (done != nullptr) {
      (done)(client_, id, *dataT);
    }
    return true;
  };
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, dataT.get()));
  return context_->RunAsync(GetLogAppendCommand(command_type_), id,
                            fbb.GetBufferPointer(), fbb.GetSize(), prefix_,
                            pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
Status Log<ID, Data>::AppendAt(const JobID &job_id, const ID &id,
                               std::shared_ptr<DataT> &dataT, const WriteCallback &done,
                               const WriteCallback &failure, int log_length) {
  auto callback = [this, id, dataT, done, failure](const std::string &data) {
    if (data.empty()) {
      if (done != nullptr) {
        (done)(client_, id, *dataT);
      }
    } else {
      if (failure != nullptr) {
        (failure)(client_, id, *dataT);
      }
    }
    return true;
  };
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, dataT.get()));
  return context_->RunAsync(GetLogAppendCommand(command_type_), id,
                            fbb.GetBufferPointer(), fbb.GetSize(), prefix_,
                            pubsub_channel_, std::move(callback), log_length);
}

template <typename ID, typename Data>
Status Log<ID, Data>::Lookup(const JobID &job_id, const ID &id, const Callback &lookup) {
  auto callback = [this, id, lookup](const std::string &data) {
    if (lookup != nullptr) {
      std::vector<DataT> results;
      if (!data.empty()) {
        auto root = flatbuffers::GetRoot<GcsTableEntry>(data.data());
        RAY_CHECK(from_flatbuf(*root->id()) == id);
        for (size_t i = 0; i < root->entries()->size(); i++) {
          DataT result;
          auto data_root = flatbuffers::GetRoot<Data>(root->entries()->Get(i)->data());
          data_root->UnPackTo(&result);
          results.emplace_back(std::move(result));
        }
      }
      lookup(client_, id, results);
    }
    return true;
  };
  std::vector<uint8_t> nil;
  return context_->RunAsync("RAY.TABLE_LOOKUP", id, nil.data(), nil.size(), prefix_,
                            pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
Status Log<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                                const Callback &subscribe,
                                const SubscriptionCallback &done) {
  RAY_CHECK(subscribe_callback_index_ == -1)
      << "Client called Subscribe twice on the same table";
  auto callback = [this, subscribe, done](const std::string &data) {
    if (data.empty()) {
      // No notification data is provided. This is the callback for the
      // initial subscription request.
      if (done != nullptr) {
        done(client_);
      }
    } else {
      // Data is provided. This is the callback for a message.
      if (subscribe != nullptr) {
        // Parse the notification.
        auto root = flatbuffers::GetRoot<GcsTableEntry>(data.data());
        ID id = UniqueID::nil();
        if (root->id()->size() > 0) {
          id = from_flatbuf(*root->id());
        }
        std::vector<DataT> results;
        for (size_t i = 0; i < root->entries()->size(); i++) {
          DataT result;
          auto data_root = flatbuffers::GetRoot<Data>(root->entries()->Get(i)->data());
          data_root->UnPackTo(&result);
          results.emplace_back(std::move(result));
        }
        subscribe(client_, id, results);
      }
    }
    // We do not delete the callback after calling it since there may be
    // more subscription messages.
    return false;
  };
  return context_->SubscribeAsync(client_id, pubsub_channel_, std::move(callback),
                                  &subscribe_callback_index_);
}

template <typename ID, typename Data>
Status Log<ID, Data>::RequestNotifications(const JobID &job_id, const ID &id,
                                           const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client requested notifications on a key before Subscribe completed";
  return context_->RunAsync("RAY.TABLE_REQUEST_NOTIFICATIONS", id, client_id.data(),
                            client_id.size(), prefix_, pubsub_channel_, nullptr);
}

template <typename ID, typename Data>
Status Log<ID, Data>::CancelNotifications(const JobID &job_id, const ID &id,
                                          const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client canceled notifications on a key before Subscribe completed";
  return context_->RunAsync("RAY.TABLE_CANCEL_NOTIFICATIONS", id, client_id.data(),
                            client_id.size(), prefix_, pubsub_channel_, nullptr);
}

template <typename ID, typename Data>
Status Table<ID, Data>::Add(const JobID &job_id, const ID &id,
                            std::shared_ptr<DataT> &dataT, const WriteCallback &done) {
  auto callback = [this, id, dataT, done](const std::string &data) {
    if (done != nullptr) {
      (done)(client_, id, *dataT);
    }
    return true;
  };
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, dataT.get()));
  return context_->RunAsync(GetTableAddCommand(command_type_), id, fbb.GetBufferPointer(),
                            fbb.GetSize(), prefix_, pubsub_channel_, std::move(callback));
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
                                  const FailureCallback &failure,
                                  const SubscriptionCallback &done) {
  return Log<ID, Data>::Subscribe(
      job_id, client_id,
      [subscribe, failure](AsyncGcsClient *client, const ID &id,
                           const std::vector<DataT> &data) {
        RAY_CHECK(data.empty() || data.size() == 1);
        if (data.size() == 1) {
          subscribe(client, id, data[0]);
        } else {
          if (failure != nullptr) {
            failure(client, id);
          }
        }
      },
      done);
}

Status ErrorTable::PushErrorToDriver(const JobID &job_id, const std::string &type,
                                     const std::string &error_message, double timestamp) {
  auto data = std::make_shared<ErrorTableDataT>();
  data->job_id = job_id.binary();
  data->type = type;
  data->error_message = error_message;
  data->timestamp = timestamp;
  return Append(job_id, job_id, data, [](ray::gcs::AsyncGcsClient *client,
                                         const JobID &id, const ErrorTableDataT &data) {
    RAY_LOG(DEBUG) << "Error message pushed callback";
  });
}

Status ProfileTable::AddProfileEvent(const std::string &event_type,
                                     const std::string &component_type,
                                     const UniqueID &component_id,
                                     const std::string &node_ip_address,
                                     double start_time, double end_time,
                                     const std::string &extra_data) {
  auto data = std::make_shared<ProfileTableDataT>();

  ProfileEventT profile_event;
  profile_event.event_type = event_type;
  profile_event.start_time = start_time;
  profile_event.end_time = end_time;
  profile_event.extra_data = extra_data;

  data->component_type = component_type;
  data->component_id = component_id.binary();
  data->node_ip_address = node_ip_address;
  data->profile_events.emplace_back(new ProfileEventT(profile_event));

  return Append(JobID::nil(), component_id, data,
                [](ray::gcs::AsyncGcsClient *client, const JobID &id,
                   const ProfileTableDataT &data) {
                  RAY_LOG(DEBUG) << "Profile message pushed callback";
                });
}

Status ProfileTable::AddProfileEventBatch(const ProfileTableData &profile_events) {
  auto data = std::make_shared<ProfileTableDataT>();
  // There is some room for optimization here because the Append function will just
  // call "Pack" and undo the "UnPack".
  profile_events.UnPackTo(data.get());

  return Append(JobID::nil(), from_flatbuf(*profile_events.component_id()), data,
                [](ray::gcs::AsyncGcsClient *client, const JobID &id,
                   const ProfileTableDataT &data) {
                  RAY_LOG(DEBUG) << "Profile message pushed callback";
                });
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
          << client_id;
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
      RAY_CHECK(removed_clients_.find(client_id) == removed_clients_.end());
    } else {
      if (client_removed_callback_ != nullptr) {
        client_removed_callback_(client, client_id, data);
      }
      removed_clients_.insert(client_id);
    }
  }
}

void ClientTable::HandleConnected(AsyncGcsClient *client, const ClientTableDataT &data) {
  auto connected_client_id = ClientID::from_binary(data.client_id);
  RAY_CHECK(client_id_ == connected_client_id) << connected_client_id << " "
                                               << client_id_;
}

const ClientID &ClientTable::GetLocalClientId() const { return client_id_; }

const ClientTableDataT &ClientTable::GetLocalClient() const { return local_client_; }

bool ClientTable::IsRemoved(const ClientID &client_id) const {
  return removed_clients_.count(client_id) == 1;
}

Status ClientTable::Connect(const ClientTableDataT &local_client) {
  RAY_CHECK(!disconnected_) << "Tried to reconnect a disconnected client.";

  RAY_CHECK(local_client.client_id == local_client_.client_id);
  local_client_ = local_client;

  // Construct the data to add to the client table.
  auto data = std::make_shared<ClientTableDataT>(local_client_);
  data->is_insertion = true;
  // Callback to handle our own successful connection once we've added
  // ourselves.
  auto add_callback = [this](AsyncGcsClient *client, const UniqueID &log_key,
                             const ClientTableDataT &data) {
    RAY_CHECK(log_key == client_log_key_);
    HandleConnected(client, data);

    // Callback for a notification from the client table.
    auto notification_callback = [this](
        AsyncGcsClient *client, const UniqueID &log_key,
        const std::vector<ClientTableDataT> &notifications) {
      RAY_CHECK(log_key == client_log_key_);
      for (auto &notification : notifications) {
        HandleNotification(client, notification);
      }
    };
    // Callback to request notifications from the client table once we've
    // successfully subscribed.
    auto subscription_callback = [this](AsyncGcsClient *c) {
      RAY_CHECK_OK(RequestNotifications(JobID::nil(), client_log_key_, client_id_));
    };
    // Subscribe to the client table.
    RAY_CHECK_OK(Subscribe(JobID::nil(), client_id_, notification_callback,
                           subscription_callback));
  };
  return Append(JobID::nil(), client_log_key_, data, add_callback);
}

Status ClientTable::Disconnect() {
  auto data = std::make_shared<ClientTableDataT>(local_client_);
  data->is_insertion = false;
  auto add_callback = [this](AsyncGcsClient *client, const ClientID &id,
                             const ClientTableDataT &data) {
    HandleConnected(client, data);
    RAY_CHECK_OK(CancelNotifications(JobID::nil(), client_log_key_, id));
  };
  RAY_RETURN_NOT_OK(Append(JobID::nil(), client_log_key_, data, add_callback));
  // We successfully added the deletion entry. Mark ourselves as disconnected.
  disconnected_ = true;
  return Status::OK();
}

ray::Status ClientTable::MarkDisconnected(const ClientID &dead_client_id) {
  auto data = std::make_shared<ClientTableDataT>();
  data->client_id = dead_client_id.binary();
  data->is_insertion = false;
  return Append(JobID::nil(), client_log_key_, data, nullptr);
}

const ClientTableDataT &ClientTable::GetClient(const ClientID &client_id) const {
  RAY_CHECK(!client_id.is_nil());
  auto entry = client_cache_.find(client_id);
  if (entry != client_cache_.end()) {
    return entry->second;
  } else {
    // If the requested client was not found, return a reference to the nil
    // client entry.
    return client_cache_.at(ClientID::nil());
  }
}

template class Log<ObjectID, ObjectTableData>;
template class Log<TaskID, ray::protocol::Task>;
template class Table<TaskID, ray::protocol::Task>;
template class Table<TaskID, TaskTableData>;
template class Log<ActorID, ActorTableData>;
template class Log<TaskID, TaskReconstructionData>;
template class Table<TaskID, TaskLeaseData>;
template class Table<ClientID, HeartbeatTableData>;
template class Log<JobID, ErrorTableData>;
template class Log<UniqueID, ClientTableData>;
template class Log<UniqueID, ProfileTableData>;

}  // namespace gcs

}  // namespace ray
