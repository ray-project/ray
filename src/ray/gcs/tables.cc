#include "ray/gcs/tables.h"

#include "ray/common/common_protocol.h"
#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/redis_gcs_client.h"
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
Status Log<ID, Data>::Append(const JobID &job_id, const ID &id,
                             const std::shared_ptr<Data> &data,
                             const WriteCallback &done) {
  num_appends_++;
  auto callback = [this, id, data, done](const CallbackReply &reply) {
    const auto status = reply.ReadAsStatus();
    // Failed to append the entry.
    RAY_CHECK(status.ok()) << "Failed to execute command TABLE_APPEND:"
                           << status.ToString();
    if (done != nullptr) {
      (done)(client_, id, *data);
    }
  };
  std::string str = data->SerializeAsString();
  return GetRedisContext(id)->RunAsync(GetLogAppendCommand(command_type_), id, str.data(),
                                       str.length(), prefix_, pubsub_channel_,
                                       std::move(callback));
}

template <typename ID, typename Data>
Status Log<ID, Data>::AppendAt(const JobID &job_id, const ID &id,
                               const std::shared_ptr<Data> &data,
                               const WriteCallback &done, const WriteCallback &failure,
                               int log_length) {
  num_appends_++;
  auto callback = [this, id, data, done, failure](const CallbackReply &reply) {
    const auto status = reply.ReadAsStatus();
    if (status.ok()) {
      if (done != nullptr) {
        (done)(client_, id, *data);
      }
    } else {
      if (failure != nullptr) {
        (failure)(client_, id, *data);
      }
    }
  };
  std::string str = data->SerializeAsString();
  return GetRedisContext(id)->RunAsync(GetLogAppendCommand(command_type_), id, str.data(),
                                       str.length(), prefix_, pubsub_channel_,
                                       std::move(callback), log_length);
}

template <typename ID, typename Data>
Status Log<ID, Data>::Lookup(const JobID &job_id, const ID &id, const Callback &lookup) {
  num_lookups_++;
  auto callback = [this, id, lookup](const CallbackReply &reply) {
    if (lookup != nullptr) {
      std::vector<Data> results;
      if (!reply.IsNil()) {
        GcsEntry gcs_entry;
        gcs_entry.ParseFromString(reply.ReadAsString());
        RAY_CHECK(ID::FromBinary(gcs_entry.id()) == id);
        for (size_t i = 0; i < gcs_entry.entries_size(); i++) {
          Data data;
          data.ParseFromString(gcs_entry.entries(i));
          results.emplace_back(std::move(data));
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
Status Log<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                                const Callback &subscribe,
                                const SubscriptionCallback &done) {
  auto subscribe_wrapper = [subscribe](RedisGcsClient *client, const ID &id,
                                       const GcsChangeMode change_mode,
                                       const std::vector<Data> &data) {
    RAY_CHECK(change_mode != GcsChangeMode::REMOVE);
    subscribe(client, id, data);
  };
  return Subscribe(job_id, client_id, subscribe_wrapper, done);
}

template <typename ID, typename Data>
Status Log<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                                const NotificationCallback &subscribe,
                                const SubscriptionCallback &done) {
  RAY_CHECK(subscribe_callback_index_ == -1)
      << "Client called Subscribe twice on the same table";
  auto callback = [this, subscribe, done](const CallbackReply &reply) {
    const auto data = reply.ReadAsPubsubData();

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
        GcsEntry gcs_entry;
        gcs_entry.ParseFromString(data);
        ID id = ID::FromBinary(gcs_entry.id());
        std::vector<Data> results;
        for (size_t i = 0; i < gcs_entry.entries_size(); i++) {
          Data result;
          result.ParseFromString(gcs_entry.entries(i));
          results.emplace_back(std::move(result));
        }
        subscribe(client_, id, gcs_entry.change_mode(), results);
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
Status Log<ID, Data>::RequestNotifications(const JobID &job_id, const ID &id,
                                           const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client requested notifications on a key before Subscribe completed";
  return GetRedisContext(id)->RunAsync("RAY.TABLE_REQUEST_NOTIFICATIONS", id,
                                       client_id.Data(), client_id.Size(), prefix_,
                                       pubsub_channel_, nullptr);
}

template <typename ID, typename Data>
Status Log<ID, Data>::CancelNotifications(const JobID &job_id, const ID &id,
                                          const ClientID &client_id) {
  RAY_CHECK(subscribe_callback_index_ >= 0)
      << "Client canceled notifications on a key before Subscribe completed";
  return GetRedisContext(id)->RunAsync("RAY.TABLE_CANCEL_NOTIFICATIONS", id,
                                       client_id.Data(), client_id.Size(), prefix_,
                                       pubsub_channel_, nullptr);
}

template <typename ID, typename Data>
void Log<ID, Data>::Delete(const JobID &job_id, const std::vector<ID> &ids) {
  if (ids.empty()) {
    return;
  }
  std::unordered_map<RedisContext *, std::ostringstream> sharded_data;
  for (const auto &id : ids) {
    sharded_data[GetRedisContext(id).get()] << id.Binary();
  }
  // Breaking really large deletion commands into batches of smaller size.
  const size_t batch_size =
      RayConfig::instance().maximum_gcs_deletion_batch_size() * ID::Size();
  for (const auto &pair : sharded_data) {
    std::string current_data = pair.second.str();
    for (size_t cur = 0; cur < pair.second.str().size(); cur += batch_size) {
      size_t data_field_size = std::min(batch_size, current_data.size() - cur);
      uint16_t id_count = data_field_size / ID::Size();
      // Send data contains id count and all the id data.
      std::string send_data(data_field_size + sizeof(id_count), 0);
      uint8_t *buffer = reinterpret_cast<uint8_t *>(&send_data[0]);
      *reinterpret_cast<uint16_t *>(buffer) = id_count;
      RAY_IGNORE_EXPR(
          std::copy_n(reinterpret_cast<const uint8_t *>(current_data.c_str() + cur),
                      data_field_size, buffer + sizeof(uint16_t)));

      RAY_IGNORE_EXPR(
          pair.first->RunAsync("RAY.TABLE_DELETE", UniqueID::Nil(),
                               reinterpret_cast<const uint8_t *>(send_data.c_str()),
                               send_data.size(), prefix_, pubsub_channel_,
                               /*redisCallback=*/nullptr));
    }
  }
}

template <typename ID, typename Data>
void Log<ID, Data>::Delete(const JobID &job_id, const ID &id) {
  Delete(job_id, std::vector<ID>({id}));
}

template <typename ID, typename Data>
std::string Log<ID, Data>::DebugString() const {
  std::stringstream result;
  result << "num lookups: " << num_lookups_ << ", num appends: " << num_appends_;
  return result.str();
}

template <typename ID, typename Data>
Status Table<ID, Data>::Add(const JobID &job_id, const ID &id,
                            const std::shared_ptr<Data> &data,
                            const WriteCallback &done) {
  num_adds_++;
  auto callback = [this, id, data, done](const CallbackReply &reply) {
    if (done != nullptr) {
      (done)(client_, id, *data);
    }
  };
  std::string str = data->SerializeAsString();
  return GetRedisContext(id)->RunAsync(GetTableAddCommand(command_type_), id, str.data(),
                                       str.length(), prefix_, pubsub_channel_,
                                       std::move(callback));
}

template <typename ID, typename Data>
Status Table<ID, Data>::Lookup(const JobID &job_id, const ID &id, const Callback &lookup,
                               const FailureCallback &failure) {
  num_lookups_++;
  return Log<ID, Data>::Lookup(job_id, id,
                               [lookup, failure](RedisGcsClient *client, const ID &id,
                                                 const std::vector<Data> &data) {
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
      [subscribe, failure](RedisGcsClient *client, const ID &id,
                           const std::vector<Data> &data) {
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
Status Set<ID, Data>::Add(const JobID &job_id, const ID &id,
                          const std::shared_ptr<Data> &data, const WriteCallback &done) {
  num_adds_++;
  auto callback = [this, id, data, done](const CallbackReply &reply) {
    if (done != nullptr) {
      (done)(client_, id, *data);
    }
  };
  std::string str = data->SerializeAsString();
  return GetRedisContext(id)->RunAsync("RAY.SET_ADD", id, str.data(), str.length(),
                                       prefix_, pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
Status Set<ID, Data>::Remove(const JobID &job_id, const ID &id,
                             const std::shared_ptr<Data> &data,
                             const WriteCallback &done) {
  num_removes_++;
  auto callback = [this, id, data, done](const CallbackReply &reply) {
    if (done != nullptr) {
      (done)(client_, id, *data);
    }
  };
  std::string str = data->SerializeAsString();
  return GetRedisContext(id)->RunAsync("RAY.SET_REMOVE", id, str.data(), str.length(),
                                       prefix_, pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
std::string Set<ID, Data>::DebugString() const {
  std::stringstream result;
  result << "num lookups: " << num_lookups_ << ", num adds: " << num_adds_
         << ", num removes: " << num_removes_;
  return result.str();
}

template <typename ID, typename Data>
Status Hash<ID, Data>::Update(const JobID &job_id, const ID &id, const DataMap &data_map,
                              const HashCallback &done) {
  num_adds_++;
  auto callback = [this, id, data_map, done](const CallbackReply &reply) {
    if (done != nullptr) {
      (done)(client_, id, data_map);
    }
  };
  GcsEntry gcs_entry;
  gcs_entry.set_id(id.Binary());
  gcs_entry.set_change_mode(GcsChangeMode::APPEND_OR_ADD);
  for (const auto &pair : data_map) {
    gcs_entry.add_entries(pair.first);
    gcs_entry.add_entries(pair.second->SerializeAsString());
  }
  std::string str = gcs_entry.SerializeAsString();
  return GetRedisContext(id)->RunAsync("RAY.HASH_UPDATE", id, str.data(), str.size(),
                                       prefix_, pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
Status Hash<ID, Data>::RemoveEntries(const JobID &job_id, const ID &id,
                                     const std::vector<std::string> &keys,
                                     const HashRemoveCallback &remove_callback) {
  num_removes_++;
  auto callback = [this, id, keys, remove_callback](const CallbackReply &reply) {
    if (remove_callback != nullptr) {
      (remove_callback)(client_, id, keys);
    }
  };
  GcsEntry gcs_entry;
  gcs_entry.set_id(id.Binary());
  gcs_entry.set_change_mode(GcsChangeMode::REMOVE);
  for (const auto &key : keys) {
    gcs_entry.add_entries(key);
  }
  std::string str = gcs_entry.SerializeAsString();
  return GetRedisContext(id)->RunAsync("RAY.HASH_UPDATE", id, str.data(), str.size(),
                                       prefix_, pubsub_channel_, std::move(callback));
}

template <typename ID, typename Data>
std::string Hash<ID, Data>::DebugString() const {
  std::stringstream result;
  result << "num lookups: " << num_lookups_ << ", num adds: " << num_adds_
         << ", num removes: " << num_removes_;
  return result.str();
}

template <typename ID, typename Data>
Status Hash<ID, Data>::Lookup(const JobID &job_id, const ID &id,
                              const HashCallback &lookup) {
  num_lookups_++;
  auto callback = [this, id, lookup](const CallbackReply &reply) {
    if (lookup != nullptr) {
      DataMap results;
      if (!reply.IsNil()) {
        const auto data = reply.ReadAsString();
        GcsEntry gcs_entry;
        gcs_entry.ParseFromString(reply.ReadAsString());
        RAY_CHECK(ID::FromBinary(gcs_entry.id()) == id);
        RAY_CHECK(gcs_entry.entries_size() % 2 == 0);
        for (int i = 0; i < gcs_entry.entries_size(); i += 2) {
          const auto &key = gcs_entry.entries(i);
          const auto value = std::make_shared<Data>();
          value->ParseFromString(gcs_entry.entries(i + 1));
          results.emplace(key, std::move(value));
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
Status Hash<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                                 const HashNotificationCallback &subscribe,
                                 const SubscriptionCallback &done) {
  RAY_CHECK(subscribe_callback_index_ == -1)
      << "Client called Subscribe twice on the same table";
  auto callback = [this, subscribe, done](const CallbackReply &reply) {
    const auto data = reply.ReadAsPubsubData();
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
        GcsEntry gcs_entry;
        gcs_entry.ParseFromString(data);
        ID id = ID::FromBinary(gcs_entry.id());
        DataMap data_map;
        if (gcs_entry.change_mode() == GcsChangeMode::REMOVE) {
          for (const auto &key : gcs_entry.entries()) {
            data_map.emplace(key, std::shared_ptr<Data>());
          }
        } else {
          RAY_CHECK(gcs_entry.entries_size() % 2 == 0);
          for (int i = 0; i < gcs_entry.entries_size(); i += 2) {
            const auto &key = gcs_entry.entries(i);
            const auto value = std::make_shared<Data>();
            value->ParseFromString(gcs_entry.entries(i + 1));
            data_map.emplace(key, std::move(value));
          }
        }
        subscribe(client_, id, gcs_entry.change_mode(), data_map);
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

Status ErrorTable::PushErrorToDriver(const JobID &job_id, const std::string &type,
                                     const std::string &error_message, double timestamp) {
  auto data = std::make_shared<ErrorTableData>();
  data->set_job_id(job_id.Binary());
  data->set_type(type);
  data->set_error_message(error_message);
  data->set_timestamp(timestamp);
  return Append(job_id, job_id, data, /*done_callback=*/nullptr);
}

std::string ErrorTable::DebugString() const {
  return Log<JobID, ErrorTableData>::DebugString();
}

Status ProfileTable::AddProfileEventBatch(const ProfileTableData &profile_events) {
  // TODO(hchen): Change the parameter to shared_ptr to avoid copying data.
  auto data = std::make_shared<ProfileTableData>();
  data->CopyFrom(profile_events);
  return Append(JobID::Nil(), UniqueID::FromRandom(), data,
                /*done_callback=*/nullptr);
}

std::string ProfileTable::DebugString() const {
  return Log<UniqueID, ProfileTableData>::DebugString();
}

Status JobTable::AppendJobData(const JobID &job_id, bool is_dead, int64_t timestamp,
                               const std::string &node_manager_address,
                               int64_t driver_pid) {
  auto data = std::make_shared<JobTableData>();
  data->set_job_id(job_id.Binary());
  data->set_is_dead(is_dead);
  data->set_timestamp(timestamp);
  data->set_node_manager_address(node_manager_address);
  data->set_driver_pid(driver_pid);
  return Append(JobID(job_id), job_id, data, /*done_callback=*/nullptr);
}

void ClientTable::RegisterClientAddedCallback(const ClientTableCallback &callback) {
  client_added_callback_ = callback;
  // Call the callback for any added clients that are cached.
  for (const auto &entry : node_cache_) {
    if (!entry.first.IsNil() && (entry.second.state() == GcsNodeInfo::ALIVE)) {
      client_added_callback_(client_, entry.first, entry.second);
    }
  }
}

void ClientTable::RegisterClientRemovedCallback(const ClientTableCallback &callback) {
  client_removed_callback_ = callback;
  // Call the callback for any removed clients that are cached.
  for (const auto &entry : node_cache_) {
    if (!entry.first.IsNil() && (entry.second.state() == GcsNodeInfo::DEAD)) {
      client_removed_callback_(client_, entry.first, entry.second);
    }
  }
}

void ClientTable::HandleNotification(RedisGcsClient *client,
                                     const GcsNodeInfo &node_info) {
  ClientID node_id = ClientID::FromBinary(node_info.node_id());
  bool is_alive = (node_info.state() == GcsNodeInfo::ALIVE);
  // It's possible to get duplicate notifications from the client table, so
  // check whether this notification is new.
  auto entry = node_cache_.find(node_id);
  bool is_notif_new;
  if (entry == node_cache_.end()) {
    // If the entry is not in the cache, then the notification is new.
    is_notif_new = true;
  } else {
    // If the entry is in the cache, then the notification is new if the client
    // was alive and is now dead or resources have been updated.
    bool was_alive = (entry->second.state() == GcsNodeInfo::ALIVE);
    is_notif_new = was_alive && !is_alive;
    // Once a client with a given ID has been removed, it should never be added
    // again. If the entry was in the cache and the client was deleted, check
    // that this new notification is not an insertion.
    if (!was_alive) {
      RAY_CHECK(!is_alive)
          << "Notification for addition of a client that was already removed:" << node_id;
    }
  }

  // Add the notification to our cache. Notifications are idempotent.
  RAY_LOG(DEBUG) << "[ClientTableNotification] ClientTable Insertion/Deletion "
                    "notification for client id "
                 << node_id << ". IsAlive: " << is_alive
                 << ". Setting the client cache to data.";
  node_cache_[node_id] = node_info;

  // If the notification is new, call any registered callbacks.
  GcsNodeInfo &cache_data = node_cache_[node_id];
  if (is_notif_new) {
    if (is_alive) {
      if (client_added_callback_ != nullptr) {
        client_added_callback_(client, node_id, cache_data);
      }
      RAY_CHECK(removed_nodes_.find(node_id) == removed_nodes_.end());
    } else {
      // NOTE(swang): The node should be added to this data structure before
      // the callback gets called, in case the callback depends on the data
      // structure getting updated.
      removed_nodes_.insert(node_id);
      if (client_removed_callback_ != nullptr) {
        client_removed_callback_(client, node_id, cache_data);
      }
    }
  }
}

void ClientTable::HandleConnected(RedisGcsClient *client, const GcsNodeInfo &node_info) {
  auto connected_node_id = ClientID::FromBinary(node_info.node_id());
  RAY_CHECK(node_id_ == connected_node_id) << connected_node_id << " " << node_id_;
}

const ClientID &ClientTable::GetLocalClientId() const { return node_id_; }

const GcsNodeInfo &ClientTable::GetLocalClient() const { return local_node_info_; }

bool ClientTable::IsRemoved(const ClientID &node_id) const {
  return removed_nodes_.count(node_id) == 1;
}

Status ClientTable::Connect(const GcsNodeInfo &local_node_info) {
  RAY_CHECK(!disconnected_) << "Tried to reconnect a disconnected client.";

  RAY_CHECK(local_node_info.node_id() == local_node_info_.node_id());
  local_node_info_ = local_node_info;

  // Construct the data to add to the client table.
  auto data = std::make_shared<GcsNodeInfo>(local_node_info_);
  data->set_state(GcsNodeInfo::ALIVE);
  // Callback to handle our own successful connection once we've added
  // ourselves.
  auto add_callback = [this](RedisGcsClient *client, const UniqueID &log_key,
                             const GcsNodeInfo &data) {
    RAY_CHECK(log_key == client_log_key_);
    HandleConnected(client, data);

    // Callback for a notification from the client table.
    auto notification_callback = [this](RedisGcsClient *client, const UniqueID &log_key,
                                        const std::vector<GcsNodeInfo> &notifications) {
      RAY_CHECK(log_key == client_log_key_);
      std::unordered_map<std::string, GcsNodeInfo> connected_nodes;
      std::unordered_map<std::string, GcsNodeInfo> disconnected_nodes;
      for (auto &notification : notifications) {
        // This is temporary fix for Issue 4140 to avoid connect to dead nodes.
        // TODO(yuhguo): remove this temporary fix after GCS entry is removable.
        if (notification.state() == GcsNodeInfo::ALIVE) {
          connected_nodes.emplace(notification.node_id(), notification);
        } else {
          auto iter = connected_nodes.find(notification.node_id());
          if (iter != connected_nodes.end()) {
            connected_nodes.erase(iter);
          }
          disconnected_nodes.emplace(notification.node_id(), notification);
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
    auto subscription_callback = [this](RedisGcsClient *c) {
      RAY_CHECK_OK(RequestNotifications(JobID::Nil(), client_log_key_, node_id_));
    };
    // Subscribe to the client table.
    RAY_CHECK_OK(
        Subscribe(JobID::Nil(), node_id_, notification_callback, subscription_callback));
  };
  return Append(JobID::Nil(), client_log_key_, data, add_callback);
}

Status ClientTable::Disconnect(const DisconnectCallback &callback) {
  auto node_info = std::make_shared<GcsNodeInfo>(local_node_info_);
  node_info->set_state(GcsNodeInfo::DEAD);
  auto add_callback = [this, callback](RedisGcsClient *client, const ClientID &id,
                                       const GcsNodeInfo &data) {
    HandleConnected(client, data);
    RAY_CHECK_OK(CancelNotifications(JobID::Nil(), client_log_key_, id));
    if (callback != nullptr) {
      callback();
    }
  };
  RAY_RETURN_NOT_OK(Append(JobID::Nil(), client_log_key_, node_info, add_callback));
  // We successfully added the deletion entry. Mark ourselves as disconnected.
  disconnected_ = true;
  return Status::OK();
}

ray::Status ClientTable::MarkDisconnected(const ClientID &dead_node_id) {
  auto node_info = std::make_shared<GcsNodeInfo>();
  node_info->set_node_id(dead_node_id.Binary());
  node_info->set_state(GcsNodeInfo::DEAD);
  return Append(JobID::Nil(), client_log_key_, node_info, nullptr);
}

void ClientTable::GetClient(const ClientID &node_id, GcsNodeInfo &node_info) const {
  RAY_CHECK(!node_id.IsNil());
  auto entry = node_cache_.find(node_id);
  if (entry != node_cache_.end()) {
    node_info = entry->second;
  } else {
    node_info.set_node_id(ClientID::Nil().Binary());
  }
}

const std::unordered_map<ClientID, GcsNodeInfo> &ClientTable::GetAllClients() const {
  return node_cache_;
}

Status ClientTable::Lookup(const Callback &lookup) {
  RAY_CHECK(lookup != nullptr);
  return Log::Lookup(JobID::Nil(), client_log_key_, lookup);
}

std::string ClientTable::DebugString() const {
  std::stringstream result;
  result << Log<ClientID, GcsNodeInfo>::DebugString();
  result << ", cache size: " << node_cache_.size()
         << ", num removed: " << removed_nodes_.size();
  return result.str();
}

Status ActorCheckpointIdTable::AddCheckpointId(const JobID &job_id,
                                               const ActorID &actor_id,
                                               const ActorCheckpointID &checkpoint_id) {
  auto lookup_callback = [this, checkpoint_id, job_id, actor_id](
                             ray::gcs::RedisGcsClient *client, const ActorID &id,
                             const ActorCheckpointIdData &data) {
    std::shared_ptr<ActorCheckpointIdData> copy =
        std::make_shared<ActorCheckpointIdData>(data);
    copy->add_timestamps(current_sys_time_ms());
    copy->add_checkpoint_ids(checkpoint_id.Binary());
    auto num_to_keep = RayConfig::instance().num_actor_checkpoints_to_keep();
    while (copy->timestamps().size() > num_to_keep) {
      // Delete the checkpoint from actor checkpoint table.
      const auto &to_delete = ActorCheckpointID::FromBinary(copy->checkpoint_ids(0));
      copy->mutable_checkpoint_ids()->erase(copy->mutable_checkpoint_ids()->begin());
      copy->mutable_timestamps()->erase(copy->mutable_timestamps()->begin());
      client_->actor_checkpoint_table().Delete(job_id, to_delete);
    }
    RAY_CHECK_OK(Add(job_id, actor_id, copy, nullptr));
  };
  auto failure_callback = [this, checkpoint_id, job_id, actor_id](
                              ray::gcs::RedisGcsClient *client, const ActorID &id) {
    std::shared_ptr<ActorCheckpointIdData> data =
        std::make_shared<ActorCheckpointIdData>();
    data->set_actor_id(id.Binary());
    data->add_timestamps(current_sys_time_ms());
    *data->add_checkpoint_ids() = checkpoint_id.Binary();
    RAY_CHECK_OK(Add(job_id, actor_id, data, nullptr));
  };
  return Lookup(job_id, actor_id, lookup_callback, failure_callback);
}

template class Log<ObjectID, ObjectTableData>;
template class Set<ObjectID, ObjectTableData>;
template class Log<TaskID, TaskTableData>;
template class Table<TaskID, TaskTableData>;
template class Log<ActorID, ActorTableData>;
template class Log<TaskID, TaskReconstructionData>;
template class Table<TaskID, TaskLeaseData>;
template class Table<ClientID, HeartbeatTableData>;
template class Table<ClientID, HeartbeatBatchTableData>;
template class Log<JobID, ErrorTableData>;
template class Log<ClientID, GcsNodeInfo>;
template class Log<JobID, JobTableData>;
template class Log<UniqueID, ProfileTableData>;
template class Table<ActorCheckpointID, ActorCheckpointData>;
template class Table<ActorID, ActorCheckpointIdData>;

template class Log<ClientID, ResourceTableData>;
template class Hash<ClientID, ResourceTableData>;

}  // namespace gcs

}  // namespace ray
