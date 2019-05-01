#include "ray/gcs/tables.h"

#include "ray/common/common_protocol.h"
#include "ray/gcs/client.h"
#include "ray/ray_config.h"
#include "ray/util/util.h"

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
Status Log<ID, Data>::Append(const DriverID &driver_id, const ID &id,
                             std::shared_ptr<DataT> &dataT, const WriteCallback &done) {
  num_appends_++;
  auto callback = [this, id, dataT, done](const std::string &data) {
    // If data is not empty, then Redis failed to append the entry.
    RAY_CHECK(data.empty()) << "TABLE_APPEND command failed: " << data;

    if (done != nullptr) {
      (done)(client_, id, *dataT);
    }
  };
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, dataT.get()));
  return GetRedisContext(id)->RunAsync(GetLogAppendCommand(command_type_), id,
                                       fbb.GetBufferPointer(), fbb.GetSize(), prefix_,
                                       pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
Status Log<ID, Data>::AppendAt(const DriverID &driver_id, const ID &id,
                               std::shared_ptr<DataT> &dataT, const WriteCallback &done,
                               const WriteCallback &failure, int log_length) {
  num_appends_++;
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
  };
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, dataT.get()));
  return GetRedisContext(id)->RunAsync(GetLogAppendCommand(command_type_), id,
                                       fbb.GetBufferPointer(), fbb.GetSize(), prefix_,
                                       pubsub_channel_, std::move(callback), log_length);
}

template <typename ID, typename Data>
Status Log<ID, Data>::Lookup(const DriverID &driver_id, const ID &id,
                             const Callback &lookup) {
  num_lookups_++;
  auto callback = [this, id, lookup](const std::string &data) {
    if (lookup != nullptr) {
      std::vector<DataT> results;
      if (!data.empty()) {
        auto root = flatbuffers::GetRoot<GcsTableEntry>(data.data());
        RAY_CHECK(from_flatbuf<ID>(*root->id()) == id);
        for (size_t i = 0; i < root->entries()->size(); i++) {
          DataT result;
          auto data_root = flatbuffers::GetRoot<Data>(root->entries()->Get(i)->data());
          data_root->UnPackTo(&result);
          results.emplace_back(std::move(result));
        }
      }
      lookup(client_, id, results);
    }
  };
  std::vector<uint8_t> nil;
  return GetRedisContext(id)->RunAsync("RAY.TABLE_LOOKUP", id, nil.data(), nil.size(),
                                       prefix_, pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
Status Log<ID, Data>::Subscribe(const DriverID &driver_id, const ClientID &client_id,
                                const Callback &subscribe,
                                const SubscriptionCallback &done) {
  auto subscribe_wrapper = [subscribe](AsyncGcsClient *client, const ID &id,
                                       const GcsTableNotificationMode notification_mode,
                                       const std::vector<DataT> &data) {
    RAY_CHECK(notification_mode != GcsTableNotificationMode::REMOVE);
    subscribe(client, id, data);
  };
  return Subscribe(driver_id, client_id, subscribe_wrapper, done);
}

template <typename ID, typename Data>
Status Log<ID, Data>::Subscribe(const DriverID &driver_id, const ClientID &client_id,
                                const NotificationCallback &subscribe,
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
        ID id;
        if (root->id()->size() > 0) {
          id = from_flatbuf<ID>(*root->id());
        }
        std::vector<DataT> results;
        for (size_t i = 0; i < root->entries()->size(); i++) {
          DataT result;
          auto data_root = flatbuffers::GetRoot<Data>(root->entries()->Get(i)->data());
          data_root->UnPackTo(&result);
          results.emplace_back(std::move(result));
        }
        subscribe(client_, id, root->notification_mode(), results);
      }
    }
  };

  subscribe_callback_index_ = 1;
  for (auto &context : shard_contexts_) {
    RAY_RETURN_NOT_OK(context->SubscribeAsync(client_id, pubsub_channel_, callback,
                                              &subscribe_callback_index_));
  }
  return Status::OK();
}

template <typename ID, typename Data>
Status Log<ID, Data>::RequestNotifications(const DriverID &driver_id, const ID &id,
                                           const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client requested notifications on a key before Subscribe completed";
  return GetRedisContext(id)->RunAsync("RAY.TABLE_REQUEST_NOTIFICATIONS", id,
                                       client_id.data(), client_id.size(), prefix_,
                                       pubsub_channel_, nullptr);
}

template <typename ID, typename Data>
Status Log<ID, Data>::CancelNotifications(const DriverID &driver_id, const ID &id,
                                          const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client canceled notifications on a key before Subscribe completed";
  return GetRedisContext(id)->RunAsync("RAY.TABLE_CANCEL_NOTIFICATIONS", id,
                                       client_id.data(), client_id.size(), prefix_,
                                       pubsub_channel_, nullptr);
}

template <typename ID, typename Data>
void Log<ID, Data>::Delete(const DriverID &driver_id, const std::vector<ID> &ids) {
  if (ids.empty()) {
    return;
  }
  std::unordered_map<RedisContext *, std::ostringstream> sharded_data;
  for (const auto &id : ids) {
    sharded_data[GetRedisContext(id).get()] << id.binary();
  }
  // Breaking really large deletion commands into batches of smaller size.
  const size_t batch_size =
      RayConfig::instance().maximum_gcs_deletion_batch_size() * kUniqueIDSize;
  for (const auto &pair : sharded_data) {
    std::string current_data = pair.second.str();
    for (size_t cur = 0; cur < pair.second.str().size(); cur += batch_size) {
      RAY_IGNORE_EXPR(pair.first->RunAsync(
          "RAY.TABLE_DELETE", UniqueID::nil(),
          reinterpret_cast<const uint8_t *>(current_data.c_str() + cur),
          std::min(batch_size, current_data.size() - cur), prefix_, pubsub_channel_,
          /*redisCallback=*/nullptr));
    }
  }
}

template <typename ID, typename Data>
void Log<ID, Data>::Delete(const DriverID &driver_id, const ID &id) {
  Delete(driver_id, std::vector<ID>({id}));
}

template <typename ID, typename Data>
std::string Log<ID, Data>::DebugString() const {
  std::stringstream result;
  result << "num lookups: " << num_lookups_ << ", num appends: " << num_appends_;
  return result.str();
}

template <typename ID, typename Data>
Status Table<ID, Data>::Add(const DriverID &driver_id, const ID &id,
                            std::shared_ptr<DataT> &dataT, const WriteCallback &done) {
  num_adds_++;
  auto callback = [this, id, dataT, done](const std::string &data) {
    if (done != nullptr) {
      (done)(client_, id, *dataT);
    }
  };
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, dataT.get()));
  return GetRedisContext(id)->RunAsync(GetTableAddCommand(command_type_), id,
                                       fbb.GetBufferPointer(), fbb.GetSize(), prefix_,
                                       pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
Status Table<ID, Data>::Lookup(const DriverID &driver_id, const ID &id,
                               const Callback &lookup, const FailureCallback &failure) {
  num_lookups_++;
  return Log<ID, Data>::Lookup(driver_id, id,
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
Status Table<ID, Data>::Subscribe(const DriverID &driver_id, const ClientID &client_id,
                                  const Callback &subscribe,
                                  const FailureCallback &failure,
                                  const SubscriptionCallback &done) {
  return Log<ID, Data>::Subscribe(
      driver_id, client_id,
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

template <typename ID, typename Data>
std::string Table<ID, Data>::DebugString() const {
  std::stringstream result;
  result << "num lookups: " << num_lookups_ << ", num adds: " << num_adds_;
  return result.str();
}

template <typename ID, typename Data>
Status Set<ID, Data>::Add(const DriverID &driver_id, const ID &id,
                          std::shared_ptr<DataT> &dataT, const WriteCallback &done) {
  num_adds_++;
  auto callback = [this, id, dataT, done](const std::string &data) {
    if (done != nullptr) {
      (done)(client_, id, *dataT);
    }
  };
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, dataT.get()));
  return GetRedisContext(id)->RunAsync("RAY.SET_ADD", id, fbb.GetBufferPointer(),
                                       fbb.GetSize(), prefix_, pubsub_channel_,
                                       std::move(callback));
}

template <typename ID, typename Data>
Status Set<ID, Data>::Remove(const DriverID &driver_id, const ID &id,
                             std::shared_ptr<DataT> &dataT, const WriteCallback &done) {
  num_removes_++;
  auto callback = [this, id, dataT, done](const std::string &data) {
    if (done != nullptr) {
      (done)(client_, id, *dataT);
    }
  };
  flatbuffers::FlatBufferBuilder fbb;
  fbb.ForceDefaults(true);
  fbb.Finish(Data::Pack(fbb, dataT.get()));
  return GetRedisContext(id)->RunAsync("RAY.SET_REMOVE", id, fbb.GetBufferPointer(),
                                       fbb.GetSize(), prefix_, pubsub_channel_,
                                       std::move(callback));
}

template <typename ID, typename Data>
std::string Set<ID, Data>::DebugString() const {
  std::stringstream result;
  result << "num lookups: " << num_lookups_ << ", num adds: " << num_adds_
         << ", num removes: " << num_removes_;
  return result.str();
}

Status ErrorTable::PushErrorToDriver(const DriverID &driver_id, const std::string &type,
                                     const std::string &error_message, double timestamp) {
  auto data = std::make_shared<ErrorTableDataT>();
  data->driver_id = driver_id.binary();
  data->type = type;
  data->error_message = error_message;
  data->timestamp = timestamp;
  return Append(DriverID(driver_id), driver_id, data, /*done_callback=*/nullptr);
}

std::string ErrorTable::DebugString() const {
  return Log<DriverID, ErrorTableData>::DebugString();
}

Status ProfileTable::AddProfileEventBatch(const ProfileTableData &profile_events) {
  auto data = std::make_shared<ProfileTableDataT>();
  // There is some room for optimization here because the Append function will just
  // call "Pack" and undo the "UnPack".
  profile_events.UnPackTo(data.get());

  return Append(DriverID::nil(), UniqueID::from_random(), data,
                /*done_callback=*/nullptr);
}

std::string ProfileTable::DebugString() const {
  return Log<UniqueID, ProfileTableData>::DebugString();
}

Status DriverTable::AppendDriverData(const DriverID &driver_id, bool is_dead) {
  auto data = std::make_shared<DriverTableDataT>();
  data->driver_id = driver_id.binary();
  data->is_dead = is_dead;
  return Append(DriverID(driver_id), driver_id, data, /*done_callback=*/nullptr);
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
      // NOTE(swang): The client should be added to this data structure before
      // the callback gets called, in case the callback depends on the data
      // structure getting updated.
      removed_clients_.insert(client_id);
      if (client_removed_callback_ != nullptr) {
        client_removed_callback_(client, client_id, data);
      }
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
      std::unordered_map<std::string, ClientTableDataT> connected_nodes;
      std::unordered_map<std::string, ClientTableDataT> disconnected_nodes;
      for (auto &notification : notifications) {
        // This is temporary fix for Issue 4140 to avoid connect to dead nodes.
        // TODO(yuhguo): remove this temporary fix after GCS entry is removable.
        if (notification.is_insertion) {
          connected_nodes.emplace(notification.client_id, notification);
        } else {
          auto iter = connected_nodes.find(notification.client_id);
          if (iter != connected_nodes.end()) {
            connected_nodes.erase(iter);
          }
          disconnected_nodes.emplace(notification.client_id, notification);
        }
      }
      for (const auto &pair : connected_nodes) {
        HandleNotification(client, pair.second);
      }
      for (const auto &pair : disconnected_nodes) {
        HandleNotification(client, pair.second);
      }
    };
    // Callback to request notifications from the client table once we've
    // successfully subscribed.
    auto subscription_callback = [this](AsyncGcsClient *c) {
      RAY_CHECK_OK(RequestNotifications(DriverID::nil(), client_log_key_, client_id_));
    };
    // Subscribe to the client table.
    RAY_CHECK_OK(Subscribe(DriverID::nil(), client_id_, notification_callback,
                           subscription_callback));
  };
  return Append(DriverID::nil(), client_log_key_, data, add_callback);
}

Status ClientTable::Disconnect(const DisconnectCallback &callback) {
  auto data = std::make_shared<ClientTableDataT>(local_client_);
  data->is_insertion = false;
  auto add_callback = [this, callback](AsyncGcsClient *client, const ClientID &id,
                                       const ClientTableDataT &data) {
    HandleConnected(client, data);
    RAY_CHECK_OK(CancelNotifications(DriverID::nil(), client_log_key_, id));
    if (callback != nullptr) {
      callback();
    }
  };
  RAY_RETURN_NOT_OK(Append(DriverID::nil(), client_log_key_, data, add_callback));
  // We successfully added the deletion entry. Mark ourselves as disconnected.
  disconnected_ = true;
  return Status::OK();
}

ray::Status ClientTable::MarkDisconnected(const ClientID &dead_client_id) {
  auto data = std::make_shared<ClientTableDataT>();
  data->client_id = dead_client_id.binary();
  data->is_insertion = false;
  return Append(DriverID::nil(), client_log_key_, data, nullptr);
}

void ClientTable::GetClient(const ClientID &client_id,
                            ClientTableDataT &client_info) const {
  RAY_CHECK(!client_id.is_nil());
  auto entry = client_cache_.find(client_id);
  if (entry != client_cache_.end()) {
    client_info = entry->second;
  } else {
    client_info.client_id = ClientID::nil().binary();
  }
}

const std::unordered_map<ClientID, ClientTableDataT> &ClientTable::GetAllClients() const {
  return client_cache_;
}

Status ClientTable::Lookup(const Callback &lookup) {
  RAY_CHECK(lookup != nullptr);
  return Log::Lookup(DriverID::nil(), client_log_key_, lookup);
}

std::string ClientTable::DebugString() const {
  std::stringstream result;
  result << Log<ClientID, ClientTableData>::DebugString();
  result << ", cache size: " << client_cache_.size()
         << ", num removed: " << removed_clients_.size();
  return result.str();
}

Status ActorCheckpointIdTable::AddCheckpointId(const DriverID &driver_id,
                                               const ActorID &actor_id,
                                               const ActorCheckpointID &checkpoint_id) {
  auto lookup_callback = [this, checkpoint_id, driver_id, actor_id](
      ray::gcs::AsyncGcsClient *client, const UniqueID &id,
      const ActorCheckpointIdDataT &data) {
    std::shared_ptr<ActorCheckpointIdDataT> copy =
        std::make_shared<ActorCheckpointIdDataT>(data);
    copy->timestamps.push_back(current_sys_time_ms());
    copy->checkpoint_ids += checkpoint_id.binary();
    auto num_to_keep = RayConfig::instance().num_actor_checkpoints_to_keep();
    while (copy->timestamps.size() > num_to_keep) {
      // Delete the checkpoint from actor checkpoint table.
      const auto &checkpoint_id =
          ActorCheckpointID::from_binary(copy->checkpoint_ids.substr(0, kUniqueIDSize));
      RAY_LOG(DEBUG) << "Deleting checkpoint " << checkpoint_id << " for actor "
                     << actor_id;
      copy->timestamps.erase(copy->timestamps.begin());
      copy->checkpoint_ids.erase(0, kUniqueIDSize);
      client_->actor_checkpoint_table().Delete(driver_id, checkpoint_id);
    }
    RAY_CHECK_OK(Add(driver_id, actor_id, copy, nullptr));
  };
  auto failure_callback = [this, checkpoint_id, driver_id, actor_id](
      ray::gcs::AsyncGcsClient *client, const UniqueID &id) {
    std::shared_ptr<ActorCheckpointIdDataT> data =
        std::make_shared<ActorCheckpointIdDataT>();
    data->actor_id = id.binary();
    data->timestamps.push_back(current_sys_time_ms());
    data->checkpoint_ids = checkpoint_id.binary();
    RAY_CHECK_OK(Add(driver_id, actor_id, data, nullptr));
  };
  return Lookup(driver_id, actor_id, lookup_callback, failure_callback);
}

template class Log<ObjectID, ObjectTableData>;
template class Set<ObjectID, ObjectTableData>;
template class Log<TaskID, ray::protocol::Task>;
template class Table<TaskID, ray::protocol::Task>;
template class Table<TaskID, TaskTableData>;
template class Log<ActorID, ActorTableData>;
template class Log<TaskID, TaskReconstructionData>;
template class Table<TaskID, TaskLeaseData>;
template class Table<ClientID, HeartbeatTableData>;
template class Table<ClientID, HeartbeatBatchTableData>;
template class Log<DriverID, ErrorTableData>;
template class Log<ClientID, ClientTableData>;
template class Log<DriverID, DriverTableData>;
template class Log<UniqueID, ProfileTableData>;
template class Table<ActorCheckpointID, ActorCheckpointData>;
template class Table<ActorID, ActorCheckpointIdData>;

}  // namespace gcs

}  // namespace ray
