#include <string.h>
#include <sstream>

#include "ray/common/common_protocol.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/util/logging.h"
#include "redis_string.h"
#include "redismodule.h"

using ray::Status;
using ray::rpc::GcsChangeMode;
using ray::rpc::GcsEntry;
using ray::rpc::TablePrefix;
using ray::rpc::TablePubsub;

#if RAY_USE_NEW_GCS
// Under this flag, ray-project/credis will be loaded.  Specifically, via
// "path/redis-server --loadmodule <credis module> --loadmodule <current
// libray_redis_module>" (dlopen() under the hood) will a definition of "module"
// be supplied.
//
// All commands in this file that depend on "module" must be wrapped by "#if
// RAY_USE_NEW_GCS", until we switch to this launch configuration as the
// default.
#include "chain_module.h"
extern RedisChainModule module;
#endif

#define REPLY_AND_RETURN_IF_FALSE(CONDITION, MESSAGE) \
  if (!(CONDITION)) {                                 \
    RedisModule_ReplyWithError(ctx, (MESSAGE));       \
    return REDISMODULE_ERR;                           \
  }

// This macro can be used at the top level of redis module.
#define REPLY_AND_RETURN_IF_NOT_OK(STATUS)                       \
  {                                                              \
    auto status = (STATUS);                                      \
    if (!status.ok()) {                                          \
      RedisModule_ReplyWithError(ctx, status.message().c_str()); \
      return REDISMODULE_ERR;                                    \
    }                                                            \
  }

// Wrap a Redis command with automatic memory management.
#define AUTO_MEMORY(FUNC)                                             \
  int FUNC(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) { \
    RedisModule_AutoMemory(ctx);                                      \
    return internal_redis_commands::FUNC(ctx, argv, argc);            \
  }

// Commands in this namespace should not be used directly. They should first be
// wrapped with AUTO_MEMORY in the global namespace to enable automatic memory
// management.
// TODO(swang): Ideally, we would make the commands that don't have auto memory
// management inaccessible instead of just using a separate namespace.
namespace internal_redis_commands {

/// Map from pub sub channel to clients that are waiting on that channel.
std::unordered_map<std::string, std::vector<std::string>> notification_map;

/// Parse a Redis string into a TablePubsub channel.
Status ParseTablePubsub(TablePubsub *out, const RedisModuleString *pubsub_channel_str) {
  long long pubsub_channel_long;
  if (RedisModule_StringToLongLong(pubsub_channel_str, &pubsub_channel_long) !=
      REDISMODULE_OK) {
    return Status::RedisError("Pubsub channel must be a valid integer.");
  }
  if (pubsub_channel_long >= static_cast<long long>(TablePubsub::TABLE_PUBSUB_MAX) ||
      pubsub_channel_long <= static_cast<long long>(TablePubsub::TABLE_PUBSUB_MIN)) {
    return Status::RedisError("Pubsub channel must be in the TablePubsub range.");
  } else {
    *out = static_cast<TablePubsub>(pubsub_channel_long);
    return Status::OK();
  }
}

/// Format a pubsub channel for a specific key. pubsub_channel_str should
/// contain a valid TablePubsub.
Status FormatPubsubChannel(RedisModuleString **out, RedisModuleCtx *ctx,
                           const RedisModuleString *pubsub_channel_str,
                           const RedisModuleString *id) {
  // Format the pubsub channel enum to a string. TablePubsub_MAX should be more
  // than enough digits, but add 1 just in case for the null terminator.
  char pubsub_channel[static_cast<int>(TablePubsub::TABLE_PUBSUB_MAX) + 1];
  TablePubsub table_pubsub;
  RAY_RETURN_NOT_OK(ParseTablePubsub(&table_pubsub, pubsub_channel_str));
  sprintf(pubsub_channel, "%d", static_cast<int>(table_pubsub));
  *out = RedisString_Format(ctx, "%s:%S", pubsub_channel, id);
  return Status::OK();
}

/// Parse a Redis string into a TablePrefix channel.
Status ParseTablePrefix(const RedisModuleString *table_prefix_str, TablePrefix *out) {
  long long table_prefix_long;
  if (RedisModule_StringToLongLong(table_prefix_str, &table_prefix_long) !=
      REDISMODULE_OK) {
    return Status::RedisError("Prefix must be a valid TablePrefix integer");
  }
  if (table_prefix_long >= static_cast<long long>(TablePrefix::TABLE_PREFIX_MAX) ||
      table_prefix_long <= static_cast<long long>(TablePrefix::TABLE_PREFIX_MIN)) {
    return Status::RedisError("Prefix must be in the TablePrefix range");
  } else {
    *out = static_cast<TablePrefix>(table_prefix_long);
    return Status::OK();
  }
}

/// Format the string for a table key. `prefix_enum` must be a valid
/// TablePrefix as a RedisModuleString. `keyname` is usually a UniqueID as a
/// RedisModuleString.
RedisModuleString *PrefixedKeyString(RedisModuleCtx *ctx, RedisModuleString *prefix_enum,
                                     RedisModuleString *keyname) {
  TablePrefix prefix;
  if (!ParseTablePrefix(prefix_enum, &prefix).ok()) {
    return nullptr;
  }
  return RedisString_Format(ctx, "%s%S", TablePrefix_Name(prefix).c_str(), keyname);
}

// TODO(swang): This helper function should be deprecated by the version below,
// which uses enums for table prefixes.
RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx, const char *prefix,
                                RedisModuleString *keyname, int mode,
                                RedisModuleString **mutated_key_str) {
  RedisModuleString *prefixed_keyname = RedisString_Format(ctx, "%s%S", prefix, keyname);
  // Pass out the key being mutated, should the caller request so.
  if (mutated_key_str != nullptr) {
    *mutated_key_str = prefixed_keyname;
  }
  RedisModuleKey *key = reinterpret_cast<RedisModuleKey *>(
      RedisModule_OpenKey(ctx, prefixed_keyname, mode));
  return key;
}

Status OpenPrefixedKey(RedisModuleKey **out, RedisModuleCtx *ctx,
                       RedisModuleString *prefix_enum, RedisModuleString *keyname,
                       int mode, RedisModuleString **mutated_key_str) {
  TablePrefix prefix;
  RAY_RETURN_NOT_OK(ParseTablePrefix(prefix_enum, &prefix));
  *out = OpenPrefixedKey(ctx, TablePrefix_Name(prefix).c_str(), keyname, mode,
                         mutated_key_str);
  return Status::OK();
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx, const char *prefix,
                                RedisModuleString *keyname, int mode) {
  return OpenPrefixedKey(ctx, prefix, keyname, mode,
                         /*mutated_key_str=*/nullptr);
}

Status OpenPrefixedKey(RedisModuleKey **out, RedisModuleCtx *ctx,
                       RedisModuleString *prefix_enum, RedisModuleString *keyname,
                       int mode) {
  return OpenPrefixedKey(out, ctx, prefix_enum, keyname, mode,
                         /*mutated_key_str=*/nullptr);
}

/// Open the key used to store the channels that should be published to when an
/// update happens at the given keyname.
Status GetBroadcastKey(RedisModuleCtx *ctx, RedisModuleString *pubsub_channel_str,
                       RedisModuleString *keyname, std::string *out) {
  RedisModuleString *channel;
  RAY_RETURN_NOT_OK(FormatPubsubChannel(&channel, ctx, pubsub_channel_str, keyname));
  RedisModuleString *prefixed_keyname = RedisString_Format(ctx, "BCAST:%S", channel);
  *out = RedisString_ToString(prefixed_keyname);
  return Status::OK();
}

/// A helper function that creates `GcsEntry` protobuf object.
///
/// \param[in] id Id of the entry.
/// \param[in] change_mode Change mode of the entry.
/// \param[in] entries Vector of entries.
/// \param[out] result The created `GcsEntry` object.
inline void CreateGcsEntry(RedisModuleString *id, GcsChangeMode change_mode,
                           const std::vector<RedisModuleString *> &entries,
                           GcsEntry *result) {
  const char *data;
  size_t size;
  data = RedisModule_StringPtrLen(id, &size);
  result->set_id(data, size);
  result->set_change_mode(change_mode);
  for (const auto &entry : entries) {
    data = RedisModule_StringPtrLen(entry, &size);
    result->add_entries(data, size);
  }
}

/// Helper method to publish formatted data to target channel.
///
/// \param pubsub_channel_str The pubsub channel name that notifications for
/// this key should be published to. When publishing to a specific client, the
/// channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key that the notification is about.
/// \param data_buffer The data to publish, which is a GcsEntry buffer.
/// \return OK if there is no error during a publish.
int PublishDataHelper(RedisModuleCtx *ctx, RedisModuleString *pubsub_channel_str,
                      RedisModuleString *id, RedisModuleString *data_buffer) {
  // Write the data back to any subscribers that are listening to all table
  // notifications.
  RedisModuleCallReply *reply =
      RedisModule_Call(ctx, "PUBLISH", "ss", pubsub_channel_str, data_buffer);
  if (reply == NULL) {
    return RedisModule_ReplyWithError(ctx, "error during PUBLISH");
  }

  std::string notification_key;
  REPLY_AND_RETURN_IF_NOT_OK(
      GetBroadcastKey(ctx, pubsub_channel_str, id, &notification_key));
  // Publish the data to any clients who requested notifications on this key.
  auto it = notification_map.find(notification_key);
  if (it != notification_map.end()) {
    for (const std::string &client_channel : it->second) {
      // RedisModule_Call seems to be broken and cannot accept "bb",
      // therefore we construct a temporary redis string here, which
      // will be garbage collected by redis.
      auto channel =
          RedisModule_CreateString(ctx, client_channel.data(), client_channel.size());
      RedisModuleCallReply *reply =
          RedisModule_Call(ctx, "PUBLISH", "ss", channel, data_buffer);
      if (reply == NULL) {
        return RedisModule_ReplyWithError(ctx, "error during PUBLISH");
      }
    }
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/// Publish a notification for an entry update at a key. This publishes a
/// notification to all subscribers of the table, as well as every client that
/// has requested notifications for this key.
///
/// \param pubsub_channel_str The pubsub channel name that notifications for
/// this key should be published to. When publishing to a specific client, the
/// channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key that the notification is about.
/// \param mode the update mode, such as append or remove.
/// \param data The appended/removed data.
/// \return OK if there is no error during a publish.
int PublishTableUpdate(RedisModuleCtx *ctx, RedisModuleString *pubsub_channel_str,
                       RedisModuleString *id, GcsChangeMode change_mode,
                       RedisModuleString *data) {
  // Serialize the notification to send.
  GcsEntry gcs_entry;
  CreateGcsEntry(id, change_mode, {data}, &gcs_entry);
  std::string str = gcs_entry.SerializeAsString();
  auto data_buffer = RedisModule_CreateString(ctx, str.data(), str.size());
  return PublishDataHelper(ctx, pubsub_channel_str, id, data_buffer);
}

// RAY.TABLE_ADD:
//   TableAdd_RedisCommand: the actual command handler.
//   (helper) TableAdd_DoWrite: performs the write to redis state.
//   (helper) TableAdd_DoPublish: performs a publish after the write.
//   ChainTableAdd_RedisCommand: the same command, chain-enabled.

int TableAdd_DoWrite(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                     RedisModuleString **mutated_key_str) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];

  RedisModuleKey *key;
  REPLY_AND_RETURN_IF_NOT_OK(OpenPrefixedKey(
      &key, ctx, prefix_str, id, REDISMODULE_READ | REDISMODULE_WRITE, mutated_key_str));
  RedisModule_StringSet(key, data);
  return REDISMODULE_OK;
}

int TableAdd_DoPublish(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString *pubsub_channel_str = argv[2];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];

  TablePubsub pubsub_channel;
  REPLY_AND_RETURN_IF_NOT_OK(ParseTablePubsub(&pubsub_channel, pubsub_channel_str));

  if (pubsub_channel != TablePubsub::NO_PUBLISH) {
    // All other pubsub channels write the data back directly onto the channel.
    return PublishTableUpdate(ctx, pubsub_channel_str, id, GcsChangeMode::APPEND_OR_ADD,
                              data);
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

/// Add an entry at a key. This overwrites any existing data at the key.
/// Publishes a notification about the update to all subscribers, if a pubsub
/// channel is provided.
///
/// This is called from a client with the command:
///
///    RAY.TABLE_ADD <table_prefix> <pubsub_channel> <id> <data>
///
/// \param table_prefix The prefix string for keys in this table.
/// \param pubsub_channel The pubsub channel name that notifications for
/// this key should be published to. When publishing to a specific client, the
/// channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to set.
/// \param data The data to insert at the key.
/// \return The current value at the key, or OK if there is no value.
int TableAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  TableAdd_DoWrite(ctx, argv, argc, /*mutated_key_str=*/nullptr);
  return TableAdd_DoPublish(ctx, argv, argc);
}

#if RAY_USE_NEW_GCS
int ChainTableAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return module.ChainReplicate(ctx, argv, argc, /*node_func=*/TableAdd_DoWrite,
                               /*tail_func=*/TableAdd_DoPublish);
}
#endif

int TableAppend_DoWrite(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        RedisModuleString **mutated_key_str) {
  if (argc < 5 || argc > 6) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];
  RedisModuleString *index_str = nullptr;
  if (argc == 6) {
    index_str = argv[5];
  }

  // Set the keys in the table.
  RedisModuleKey *key;
  REPLY_AND_RETURN_IF_NOT_OK(OpenPrefixedKey(
      &key, ctx, prefix_str, id, REDISMODULE_READ | REDISMODULE_WRITE, mutated_key_str));
  int type = RedisModule_KeyType(key);
  REPLY_AND_RETURN_IF_FALSE(
      type == REDISMODULE_KEYTYPE_LIST || type == REDISMODULE_KEYTYPE_EMPTY,
      "TABLE_APPEND entries must be a list or an empty list");

  // Determine the index at which the data should be appended. If no index is
  // requested, then is the current length of the log.
  size_t index = RedisModule_ValueLength(key);
  if (index_str != nullptr) {
    // Parse the requested index.
    long long requested_index;
    REPLY_AND_RETURN_IF_FALSE(
        RedisModule_StringToLongLong(index_str, &requested_index) == REDISMODULE_OK,
        "Index is not a number.");
    REPLY_AND_RETURN_IF_FALSE(requested_index >= 0, "Index is less than 0.");
    index = static_cast<size_t>(requested_index);
  }
  // Only perform the append if the requested index matches the current length
  // of the log, or if no index was requested.
  if (index == RedisModule_ValueLength(key)) {
    // The requested index matches the current length of the log or no index
    // was requested. Perform the append.
    if (RedisModule_ListPush(key, REDISMODULE_LIST_TAIL, data) == REDISMODULE_OK) {
      return REDISMODULE_OK;
    } else {
      static const char *reply = "Unexpected error during TABLE_APPEND";
      RedisModule_ReplyWithError(ctx, reply);
      return REDISMODULE_ERR;
    }
  } else {
    // The requested index did not match the current length of the log. Return
    // an error message as a string.
    static const char *reply = "ERR entry exists";
    RedisModule_ReplyWithSimpleString(ctx, reply);
    return REDISMODULE_ERR;
  }
}

int TableAppend_DoPublish(RedisModuleCtx *ctx, RedisModuleString **argv, int /*argc*/) {
  RedisModuleString *pubsub_channel_str = argv[2];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];
  // Publish a message on the requested pubsub channel if necessary.
  TablePubsub pubsub_channel;
  REPLY_AND_RETURN_IF_NOT_OK(ParseTablePubsub(&pubsub_channel, pubsub_channel_str));
  if (pubsub_channel != TablePubsub::NO_PUBLISH) {
    // All other pubsub channels write the data back directly onto the
    // channel.
    return PublishTableUpdate(ctx, pubsub_channel_str, id, GcsChangeMode::APPEND_OR_ADD,
                              data);
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

/// Append an entry to the log stored at a key. Publishes a notification about
/// the update to all subscribers, if a pubsub channel is provided.
///
/// This is called from a client with the command:
//
///    RAY.TABLE_APPEND <table_prefix> <pubsub_channel> <id> <data>
///                     <index (optional)>
///
/// \param table_prefix The prefix string for keys in this table.
/// \param pubsub_channel The pubsub channel name that notifications for this
/// key should be published to. When publishing to a specific client, the
/// channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to append to.
/// \param data The data to append to the key.
/// \param index If this is set, then the data must be appended at this index.
/// If the current log is shorter or longer than the requested index, then the
/// append will fail and an error message will be returned as a string.
/// \return OK if the append succeeds, or an error message string if the append
/// fails.
int TableAppend_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (TableAppend_DoWrite(ctx, argv, argc, /*mutated_key_str=*/nullptr) !=
      REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  return TableAppend_DoPublish(ctx, argv, argc);
}

#if RAY_USE_NEW_GCS
int ChainTableAppend_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                  int argc) {
  return module.ChainReplicate(ctx, argv, argc,
                               /*node_func=*/TableAppend_DoWrite,
                               /*tail_func=*/TableAppend_DoPublish);
}
#endif

int Set_DoPublish(RedisModuleCtx *ctx, RedisModuleString **argv, bool is_add) {
  RedisModuleString *pubsub_channel_str = argv[2];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];
  // Publish a message on the requested pubsub channel if necessary.
  TablePubsub pubsub_channel;
  REPLY_AND_RETURN_IF_NOT_OK(ParseTablePubsub(&pubsub_channel, pubsub_channel_str));
  if (pubsub_channel != TablePubsub::NO_PUBLISH) {
    // All other pubsub channels write the data back directly onto the
    // channel.
    return PublishTableUpdate(
        ctx, pubsub_channel_str, id,
        is_add ? GcsChangeMode::APPEND_OR_ADD : GcsChangeMode::REMOVE, data);
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

int Set_DoWrite(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool is_add,
                bool *changed) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];

  RedisModuleString *key_string = PrefixedKeyString(ctx, prefix_str, id);
  // TODO(kfstorm): According to https://redis.io/topics/modules-intro,
  // set type API is not available yet. We can change RedisModule_Call to
  // set type API later.
  RedisModuleCallReply *reply =
      RedisModule_Call(ctx, is_add ? "SADD" : "SREM", "ss", key_string, data);
  if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ERROR) {
    *changed = RedisModule_CallReplyInteger(reply) > 0;
    if (!is_add && *changed) {
      // try to delete the empty set.
      RedisModuleKey *key;
      REPLY_AND_RETURN_IF_NOT_OK(
          OpenPrefixedKey(&key, ctx, prefix_str, id, REDISMODULE_WRITE));
      auto size = RedisModule_ValueLength(key);
      if (size == 0) {
        REPLY_AND_RETURN_IF_FALSE(RedisModule_DeleteKey(key) == REDISMODULE_OK,
                                  "ERR Failed to delete empty set.");
      }
    }
    return REDISMODULE_OK;
  } else {
    // the SADD/SREM command failed
    RedisModule_ReplyWithCallReply(ctx, reply);
    return REDISMODULE_ERR;
  }
}

/// Add an entry to the set stored at a key. Publishes a notification about
/// the update to all subscribers, if a pubsub channel is provided.
///
/// This is called from a client with the command:
//
///    RAY.SET_ADD <table_prefix> <pubsub_channel> <id> <data>
///
/// \param table_prefix The prefix string for keys in this set.
/// \param pubsub_channel The pubsub channel name that notifications for this
/// key should be published to. When publishing to a specific client, the
/// channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to add to.
/// \param data The data to add to the key.
/// \return OK if the add succeeds, or an error message string if the add fails.
int SetAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  bool changed;
  if (Set_DoWrite(ctx, argv, argc, /*is_add=*/true, &changed) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (changed) {
    return Set_DoPublish(ctx, argv, /*is_add=*/true);
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/// Remove an entry from the set stored at a key. Publishes a notification about
/// the update to all subscribers, if a pubsub channel is provided.
///
/// This is called from a client with the command:
//
///    RAY.SET_REMOVE <table_prefix> <pubsub_channel> <id> <data>
///
/// \param table_prefix The prefix string for keys in this table.
/// \param pubsub_channel The pubsub channel name that notifications for this
/// key should be published to. When publishing to a specific client, the
/// channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to remove from.
/// \param data The data to remove from the key.
/// \return OK if the remove succeeds, or an error message string if the remove
/// fails.
int SetRemove_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  bool changed;
  if (Set_DoWrite(ctx, argv, argc, /*is_add=*/false, &changed) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (changed) {
    return Set_DoPublish(ctx, argv, /*is_add=*/false);
  } else {
    RAY_LOG(ERROR) << "The entry to remove doesn't exist.";
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int Hash_DoPublish(RedisModuleCtx *ctx, RedisModuleString **argv) {
  RedisModuleString *pubsub_channel_str = argv[2];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];
  // Publish a message on the requested pubsub channel if necessary.
  TablePubsub pubsub_channel;
  REPLY_AND_RETURN_IF_NOT_OK(ParseTablePubsub(&pubsub_channel, pubsub_channel_str));
  if (pubsub_channel != TablePubsub::NO_PUBLISH) {
    // All other pubsub channels write the data back directly onto the
    // channel.
    return PublishDataHelper(ctx, pubsub_channel_str, id, data);
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

/// Do the hash table write operation. This is called from by HashUpdate_RedisCommand.
///
/// \param change_mode Output the mode of the operation: APPEND_OR_ADD or REMOVE.
/// \param deleted_data Output data if the deleted data is not the same as required.
int HashUpdate_DoWrite(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                       GcsChangeMode *change_mode, RedisModuleString **changed_data) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];
  RedisModuleString *update_data = argv[4];

  RedisModuleKey *key;
  REPLY_AND_RETURN_IF_NOT_OK(OpenPrefixedKey(
      &key, ctx, prefix_str, id, REDISMODULE_READ | REDISMODULE_WRITE, nullptr));
  int type = RedisModule_KeyType(key);
  REPLY_AND_RETURN_IF_FALSE(
      type == REDISMODULE_KEYTYPE_HASH || type == REDISMODULE_KEYTYPE_EMPTY,
      "HashUpdate_DoWrite: entries must be a hash or an empty hash");

  size_t update_data_len = 0;
  const char *update_data_buf = RedisModule_StringPtrLen(update_data, &update_data_len);

  GcsEntry gcs_entry;
  gcs_entry.ParseFromArray(update_data_buf, update_data_len);
  *change_mode = gcs_entry.change_mode();

  if (*change_mode == GcsChangeMode::APPEND_OR_ADD) {
    // This code path means they are updating command.
    size_t total_size = gcs_entry.entries_size();
    REPLY_AND_RETURN_IF_FALSE(total_size % 2 == 0, "Invalid Hash Update data vector.");
    for (size_t i = 0; i < total_size; i += 2) {
      // Reconstruct a key-value pair from a flattened list.
      RedisModuleString *entry_key = RedisModule_CreateString(
          ctx, gcs_entry.entries(i).data(), gcs_entry.entries(i).size());
      RedisModuleString *entry_value = RedisModule_CreateString(
          ctx, gcs_entry.entries(i + 1).data(), gcs_entry.entries(i + 1).size());
      // Returning 0 if key exists(still updated), 1 if the key is created.
      RAY_IGNORE_EXPR(
          RedisModule_HashSet(key, REDISMODULE_HASH_NONE, entry_key, entry_value, NULL));
    }
    *changed_data = update_data;
  } else {
    // This code path means the command wants to remove the entries.
    GcsEntry updated;
    updated.set_id(gcs_entry.id());
    updated.set_change_mode(gcs_entry.change_mode());

    size_t total_size = gcs_entry.entries_size();
    for (size_t i = 0; i < total_size; i++) {
      RedisModuleString *entry_key = RedisModule_CreateString(
          ctx, gcs_entry.entries(i).data(), gcs_entry.entries(i).size());
      int deleted_num = RedisModule_HashSet(key, REDISMODULE_HASH_NONE, entry_key,
                                            REDISMODULE_HASH_DELETE, NULL);
      if (deleted_num != 0) {
        // The corresponding key is removed.
        updated.add_entries(gcs_entry.entries(i));
      }
    }

    // Serialize updated data.
    std::string str = updated.SerializeAsString();
    *changed_data = RedisModule_CreateString(ctx, str.data(), str.size());
    auto size = RedisModule_ValueLength(key);
    if (size == 0) {
      REPLY_AND_RETURN_IF_FALSE(RedisModule_DeleteKey(key) == REDISMODULE_OK,
                                "ERR Failed to delete empty hash.");
    }
  }
  return REDISMODULE_OK;
}

/// Update entries for a hash table.
///
/// This is called from a client with the command:
//
///    RAY.HASH_UPDATE <table_prefix> <pubsub_channel> <id> <data>
///
/// \param table_prefix The prefix string for keys in this table.
/// \param pubsub_channel The pubsub channel name that notifications for this
/// key should be published to. When publishing to a specific client, the
/// channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to remove from.
/// \param data The GcsEntry protobuf data used to update this hash table.
///     1). For deletion, this is a list of keys.
///     2). For updating, this is a list of pairs with each key followed by the value.
/// \return OK if the remove succeeds, or an error message string if the remove
/// fails.
int HashUpdate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  GcsChangeMode mode;
  RedisModuleString *changed_data = nullptr;
  if (HashUpdate_DoWrite(ctx, argv, argc, &mode, &changed_data) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  // Replace the data with the changed data to do the publish.
  std::vector<RedisModuleString *> new_argv(argv, argv + argc);
  new_argv[4] = changed_data;
  return Hash_DoPublish(ctx, new_argv.data());
}

/// A helper function to create a GcsEntry protobuf, based on the
/// current value or values at the given key.
///
/// \param ctx The Redis module context.
/// \param table_key The Redis key whose entry should be read out. The key must
/// be open when this function is called and may be closed in this function.
/// The key's name format is <prefix_str><entry_id>.
/// \param prefix_str The string prefix associated with the open Redis key.
/// When parsed, this is expected to be a TablePrefix.
/// \param entry_id The UniqueID associated with the open Redis key.
/// \param[out] gcs_entry The created GcsEntry.
Status TableEntryToProtobuf(RedisModuleCtx *ctx, RedisModuleKey *table_key,
                            RedisModuleString *prefix_str, RedisModuleString *entry_id,
                            GcsEntry *gcs_entry) {
  auto key_type = RedisModule_KeyType(table_key);
  switch (key_type) {
  case REDISMODULE_KEYTYPE_STRING: {
    // Build the GcsEntry from the string data.
    CreateGcsEntry(entry_id, GcsChangeMode::APPEND_OR_ADD, {}, gcs_entry);
    size_t data_len = 0;
    char *data_buf = RedisModule_StringDMA(table_key, &data_len, REDISMODULE_READ);
    gcs_entry->add_entries(data_buf, data_len);
  } break;
  case REDISMODULE_KEYTYPE_LIST:
  case REDISMODULE_KEYTYPE_HASH:
  case REDISMODULE_KEYTYPE_SET: {
    RedisModule_CloseKey(table_key);
    // Close the key before executing the command. NOTE(swang): According to
    // https://github.com/RedisLabs/RedisModulesSDK/blob/master/API.md, "While
    // a key is open, it should only be accessed via the low level key API."
    RedisModuleString *table_key_str = PrefixedKeyString(ctx, prefix_str, entry_id);
    // TODO(swang): This could potentially be replaced with the native redis
    // server list iterator, once it is implemented for redis modules.
    RedisModuleCallReply *reply = nullptr;
    switch (key_type) {
    case REDISMODULE_KEYTYPE_LIST:
      reply = RedisModule_Call(ctx, "LRANGE", "sll", table_key_str, 0, -1);
      break;
    case REDISMODULE_KEYTYPE_SET:
      reply = RedisModule_Call(ctx, "SMEMBERS", "s", table_key_str);
      break;
    case REDISMODULE_KEYTYPE_HASH:
      reply = RedisModule_Call(ctx, "HGETALL", "s", table_key_str);
      break;
    }
    // Build the GcsEntry from the set of log entries.
    if (reply == nullptr || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
      return Status::RedisError("Empty list/set/hash or wrong type");
    }
    CreateGcsEntry(entry_id, GcsChangeMode::APPEND_OR_ADD, {}, gcs_entry);
    for (size_t i = 0; i < RedisModule_CallReplyLength(reply); i++) {
      RedisModuleCallReply *element = RedisModule_CallReplyArrayElement(reply, i);
      size_t len;
      const char *element_str = RedisModule_CallReplyStringPtr(element, &len);
      gcs_entry->add_entries(element_str, len);
    }
  } break;
  case REDISMODULE_KEYTYPE_EMPTY: {
    CreateGcsEntry(entry_id, GcsChangeMode::APPEND_OR_ADD, {}, gcs_entry);
  } break;
  default:
    return Status::RedisError("Invalid Redis type during lookup.");
  }
  return Status::OK();
}

/// Lookup the current value or values at a key. Returns the current value or
/// values at the key.
///
/// This is called from a client with the command:
//
///    RAY.TABLE_LOOKUP <table_prefix> <pubsub_channel> <id>
///
/// \param table_prefix The prefix string for keys in this table.
/// \param pubsub_channel The pubsub channel name that notifications for
///        this key should be published to. This field is unused for lookups.
/// \param id The ID of the key to lookup.
/// \return nil if the key is empty, the current value if the key type is a
///         string, or an array of the current values if the key type is a set.
int TableLookup_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];

  // Lookup the data at the key.
  RedisModuleKey *table_key;
  REPLY_AND_RETURN_IF_NOT_OK(
      OpenPrefixedKey(&table_key, ctx, prefix_str, id, REDISMODULE_READ));
  if (table_key == nullptr) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    // Serialize the data to a GcsEntry to return to the client.
    GcsEntry gcs_entry;
    REPLY_AND_RETURN_IF_NOT_OK(
        TableEntryToProtobuf(ctx, table_key, prefix_str, id, &gcs_entry));
    std::string str = gcs_entry.SerializeAsString();
    RedisModule_ReplyWithStringBuffer(ctx, str.data(), str.size());
  }
  return REDISMODULE_OK;
}

// The deleting helper function.
static Status DeleteKeyHelper(RedisModuleCtx *ctx, RedisModuleString *prefix_str,
                              RedisModuleString *id_data) {
  RedisModuleKey *delete_key = nullptr;
  RAY_RETURN_NOT_OK(
      OpenPrefixedKey(&delete_key, ctx, prefix_str, id_data, REDISMODULE_READ));
  if (delete_key == nullptr) {
    return Status::RedisError("Key does not exist.");
  }
  auto key_type = RedisModule_KeyType(delete_key);
  // Set/Hash will delete itself when the length is 0.
  if (key_type == REDISMODULE_KEYTYPE_STRING || key_type == REDISMODULE_KEYTYPE_LIST) {
    // Current Table or Log only has this two types of entries.
    RAY_RETURN_NOT_OK(
        OpenPrefixedKey(&delete_key, ctx, prefix_str, id_data, REDISMODULE_WRITE));
    RedisModule_DeleteKey(delete_key);
  } else {
    std::ostringstream ostream;
    size_t redis_string_size;
    const char *redis_string_str = RedisModule_StringPtrLen(id_data, &redis_string_size);
    auto id_binary = std::string(redis_string_str, redis_string_size);
    ostream << "Undesired type for RAY.TableDelete: " << key_type
            << " id:" << ray::UniqueID::FromBinary(id_binary);
    RAY_LOG(ERROR) << ostream.str();
    return Status::RedisError(ostream.str());
  }
  return Status::OK();
}

/// Delete a list of redis keys in batch mode.
///
/// This is called from a client with the command:
//
///    RAY.TABLE_DELETE <table_prefix> <pubsub_channel> <id> <data>
///
/// \param table_prefix The prefix string for keys in this table.
/// \param pubsub_channel Unused but follow the interface.
/// \param id This id will be ignored but follow the interface.
/// \param data The list of Unique Ids, kUniqueIDSize bytes for each.
/// \return Always return OK unless the arguments are invalid.
int TableDelete_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *data = argv[4];

  size_t len = 0;
  const char *data_ptr = nullptr;
  data_ptr = RedisModule_StringPtrLen(data, &len);
  // The first uint16_t are used to encode the number of ids to delete.
  size_t ids_to_delete = *reinterpret_cast<const uint16_t *>(data_ptr);
  size_t id_length = (len - sizeof(uint16_t)) / ids_to_delete;
  REPLY_AND_RETURN_IF_FALSE((len - sizeof(uint16_t)) % ids_to_delete == 0,
                            "The deletion data length must be multiple of the ID size");
  data_ptr += sizeof(uint16_t);
  for (size_t i = 0; i < ids_to_delete; ++i) {
    RedisModuleString *id_data =
        RedisModule_CreateString(ctx, data_ptr + i * id_length, id_length);
    RAY_IGNORE_EXPR(DeleteKeyHelper(ctx, prefix_str, id_data));
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/// Request notifications for changes to a key. Returns the current value or
/// values at the key. Notifications will be sent to the requesting client for
/// every subsequent TABLE_ADD to the key.
///
/// This is called from a client with the command:
//
///    RAY.TABLE_REQUEST_NOTIFICATIONS <table_prefix> <pubsub_channel> <id>
///        <client_id>
///
/// \param table_prefix The prefix string for keys in this table.
/// \param pubsub_channel The pubsub channel name that notifications for
///        this key should be published to. When publishing to a specific
///        client, the channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to publish notifications for.
/// \param client_id The ID of the client that is being notified.
/// \return nil if the key is empty, the current value if the key type is a
///         string, or an array of the current values if the key type is a set.
int TableRequestNotifications_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                           int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *pubsub_channel_str = argv[2];
  RedisModuleString *id = argv[3];
  RedisModuleString *client_id = argv[4];
  RedisModuleString *client_channel;
  REPLY_AND_RETURN_IF_NOT_OK(
      FormatPubsubChannel(&client_channel, ctx, pubsub_channel_str, client_id));

  // Add this client to the set of clients that should be notified when there
  // are changes to the key.
  std::string notification_key;
  REPLY_AND_RETURN_IF_NOT_OK(
      GetBroadcastKey(ctx, pubsub_channel_str, id, &notification_key));
  notification_map[notification_key].push_back(RedisString_ToString(client_channel));

  // Lookup the current value at the key.
  RedisModuleKey *table_key;
  REPLY_AND_RETURN_IF_NOT_OK(
      OpenPrefixedKey(&table_key, ctx, prefix_str, id, REDISMODULE_READ));
  // Publish the current value at the key to the client that is requesting
  // notifications. An empty notification will be published if the key is
  // empty.
  GcsEntry gcs_entry;
  REPLY_AND_RETURN_IF_NOT_OK(
      TableEntryToProtobuf(ctx, table_key, prefix_str, id, &gcs_entry));
  std::string str = gcs_entry.SerializeAsString();
  RedisModule_Call(ctx, "PUBLISH", "sb", client_channel, str.data(), str.size());

  return RedisModule_ReplyWithNull(ctx);
}

/// Cancel notifications for changes to a key. The client will no longer
/// receive notifications for this key. This does not check if the client
/// first requested notifications before canceling them.
///
/// This is called from a client with the command:
//
///    RAY.TABLE_CANCEL_NOTIFICATIONS <table_prefix> <pubsub_channel> <id>
///        <client_id>
///
/// \param table_prefix The prefix string for keys in this table.
/// \param pubsub_channel The pubsub channel name that notifications for
///        this key should be published to. If publishing to a specific client,
///        then the channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to publish notifications for.
/// \param client_id The ID of the client to cancel notifications for.
/// \return OK.
int TableCancelNotifications_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                          int argc) {
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *pubsub_channel_str = argv[2];
  RedisModuleString *id = argv[3];
  RedisModuleString *client_id = argv[4];
  RedisModuleString *client_channel;
  REPLY_AND_RETURN_IF_NOT_OK(
      FormatPubsubChannel(&client_channel, ctx, pubsub_channel_str, client_id));

  // Remove this client from the set of clients that should be notified when
  // there are changes to the key.
  std::string notification_key;
  REPLY_AND_RETURN_IF_NOT_OK(
      GetBroadcastKey(ctx, pubsub_channel_str, id, &notification_key));
  auto it = notification_map.find(notification_key);
  if (it != notification_map.end()) {
    it->second.erase(std::remove(it->second.begin(), it->second.end(),
                                 RedisString_ToString(client_channel)),
                     it->second.end());
    if (it->second.size() == 0) {
      notification_map.erase(it);
    }
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

Status IsNil(bool *out, const std::string &data) {
  if (data.size() != kUniqueIDSize) {
    return Status::RedisError("Size of data doesn't match size of UniqueID");
  }
  const uint8_t *d = reinterpret_cast<const uint8_t *>(data.data());
  for (size_t i = 0; i < kUniqueIDSize; ++i) {
    if (d[i] != 255) {
      *out = false;
    }
  }
  *out = true;
  return Status::OK();
}

std::string DebugString() {
  std::stringstream result;
  result << "RedisModule:";
  result << "\n- NotificationMap.size = " << notification_map.size();
  result << std::endl;
  return result.str();
}

int DebugString_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(argv);

  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  std::string debug_string = DebugString();
  return RedisModule_ReplyWithStringBuffer(ctx, debug_string.data(), debug_string.size());
}
};  // namespace internal_redis_commands

// Wrap all Redis commands with Redis' auto memory management.
AUTO_MEMORY(TableAdd_RedisCommand);
AUTO_MEMORY(HashUpdate_RedisCommand);
AUTO_MEMORY(TableAppend_RedisCommand);
AUTO_MEMORY(SetAdd_RedisCommand);
AUTO_MEMORY(SetRemove_RedisCommand);
AUTO_MEMORY(TableLookup_RedisCommand);
AUTO_MEMORY(TableRequestNotifications_RedisCommand);
AUTO_MEMORY(TableDelete_RedisCommand);
AUTO_MEMORY(TableCancelNotifications_RedisCommand);
AUTO_MEMORY(DebugString_RedisCommand);
#if RAY_USE_NEW_GCS
AUTO_MEMORY(ChainTableAdd_RedisCommand);
AUTO_MEMORY(ChainTableAppend_RedisCommand);
#endif

extern "C" {

/// This function must be present on each Redis module. It is used in order to
/// register the commands into the Redis server.
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);

  if (RedisModule_Init(ctx, "ray", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_add", TableAdd_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_append", TableAppend_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.set_add", SetAdd_RedisCommand, "write pubsub",
                                0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.set_remove", SetRemove_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_lookup", TableLookup_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_delete", TableDelete_RedisCommand,
                                "write", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.hash_update", HashUpdate_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_request_notifications",
                                TableRequestNotifications_RedisCommand, "write pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_cancel_notifications",
                                TableCancelNotifications_RedisCommand, "write pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.debug_string", DebugString_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

#if RAY_USE_NEW_GCS
  // Chain-enabled commands that depend on ray-project/credis.
  if (RedisModule_CreateCommand(ctx, "ray.chain.table_add", ChainTableAdd_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "ray.chain.table_append",
                                ChainTableAppend_RedisCommand, "write pubsub", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
#endif

  return REDISMODULE_OK;
}

}  /// extern "C"
