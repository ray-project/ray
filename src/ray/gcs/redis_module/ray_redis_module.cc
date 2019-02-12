#include <string.h>

#include "ray/common/common_protocol.h"
#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"
#include "ray/status.h"
#include "ray/util/logging.h"
#include "redis_string.h"
#include "redismodule.h"

using ray::Status;

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

/// Map from pub sub channel to clients that are waiting on that channel.
std::unordered_map<std::string, std::vector<std::string>> notification_map;

/// Parse a Redis string into a TablePubsub channel.
Status ParseTablePubsub(TablePubsub *out, const RedisModuleString *pubsub_channel_str) {
  long long pubsub_channel_long;
  if (RedisModule_StringToLongLong(pubsub_channel_str, &pubsub_channel_long) !=
      REDISMODULE_OK) {
    return Status::RedisError("Pubsub channel must be a valid integer.");
  }
  if (pubsub_channel_long > static_cast<long long>(TablePubsub::MAX) ||
      pubsub_channel_long < static_cast<long long>(TablePubsub::MIN)) {
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
  char pubsub_channel[static_cast<int>(TablePubsub::MAX) + 1];
  TablePubsub table_pubsub;
  RAY_RETURN_NOT_OK(ParseTablePubsub(&table_pubsub, pubsub_channel_str));
  sprintf(pubsub_channel, "%d", static_cast<int>(table_pubsub));
  *out = RedisString_Format(ctx, "%s:%S", pubsub_channel, id);
  return Status::OK();
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
  long long prefix_long;
  if (RedisModule_StringToLongLong(prefix_enum, &prefix_long) != REDISMODULE_OK) {
    return Status::RedisError("Prefix must be a valid TablePrefix integer.");
  }
  if (prefix_long > static_cast<long long>(TablePrefix::MAX) ||
      prefix_long < static_cast<long long>(TablePrefix::MIN)) {
    return Status::RedisError("Prefix must be in the TablePrefix range.");
  }
  auto prefix = static_cast<TablePrefix>(prefix_long);
  *out =
      OpenPrefixedKey(ctx, EnumNameTablePrefix(prefix), keyname, mode, mutated_key_str);
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
                       RedisModuleString *keyname, std::string* out) {
  RedisModuleString *channel;
  RAY_RETURN_NOT_OK(FormatPubsubChannel(&channel, ctx, pubsub_channel_str, keyname));
  RedisModuleString *prefixed_keyname = RedisString_Format(ctx, "BCAST:%S", channel);
  *out = RedisString_ToString(prefixed_keyname);
  return Status::OK();
}

/**
 * This is a helper method to convert a redis module string to a flatbuffer
 * string.
 *
 * @param fbb The flatbuffer builder.
 * @param redis_string The redis string.
 * @return The flatbuffer string.
 */
flatbuffers::Offset<flatbuffers::String> RedisStringToFlatbuf(
    flatbuffers::FlatBufferBuilder &fbb, RedisModuleString *redis_string) {
  size_t redis_string_size;
  const char *redis_string_str =
      RedisModule_StringPtrLen(redis_string, &redis_string_size);
  return fbb.CreateString(redis_string_str, redis_string_size);
}

/// Publish a notification for a new entry at a key. This publishes a
/// notification to all subscribers of the table, as well as every client that
/// has requested notifications for this key.
///
/// \param pubsub_channel_str The pubsub channel name that notifications for
///        this key should be published to. When publishing to a specific
///        client, the channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key that the notification is about.
/// \param data The data to publish.
/// \return OK if there is no error during a publish.
int PublishTableAdd(RedisModuleCtx *ctx, RedisModuleString *pubsub_channel_str,
                    RedisModuleString *id, RedisModuleString *data) {
  // Serialize the notification to send.
  flatbuffers::FlatBufferBuilder fbb;
  auto data_flatbuf = RedisStringToFlatbuf(fbb, data);
  auto message = CreateGcsTableEntry(fbb, RedisStringToFlatbuf(fbb, id),
                                     fbb.CreateVector(&data_flatbuf, 1));
  fbb.Finish(message);

  // Write the data back to any subscribers that are listening to all table
  // notifications.
  RedisModuleCallReply *reply = RedisModule_Call(ctx, "PUBLISH", "sb", pubsub_channel_str,
                                                 fbb.GetBufferPointer(), fbb.GetSize());
  if (reply == NULL) {
    return RedisModule_ReplyWithError(ctx, "error during PUBLISH");
  }

  std::string notification_key;
  REPLY_AND_RETURN_IF_NOT_OK(GetBroadcastKey(ctx, pubsub_channel_str, id, &notification_key));
  // Publish the data to any clients who requested notifications on this key.
  auto it = notification_map.find(notification_key);
  if (it != notification_map.end()) {
    for (const std::string& client_channel : it->second) {
      // RedisModule_Call seems to be broken and cannot accept "bb",
      // therefore we construct a temporary redis string here, which
      // will be garbage collected by redis.
      auto channel = RedisModule_CreateString(
          ctx, client_channel.data(), client_channel.size());
      RedisModuleCallReply *reply = RedisModule_Call(
          ctx, "PUBLISH", "sb", channel, fbb.GetBufferPointer(), fbb.GetSize());
      if (reply == NULL) {
        return RedisModule_ReplyWithError(ctx, "error during PUBLISH");
      }
    }
    notification_map.erase(it);
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
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
    return PublishTableAdd(ctx, pubsub_channel_str, id, data);
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
///  this key should be published to. When publishing to a specific
///  client, the channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to set.
/// \param data The data to insert at the key.
/// \return The current value at the key, or OK if there is no value.
int TableAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  TableAdd_DoWrite(ctx, argv, argc, /*mutated_key_str=*/nullptr);
  return TableAdd_DoPublish(ctx, argv, argc);
}

#if RAY_USE_NEW_GCS
int ChainTableAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
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
    int flags = REDISMODULE_ZADD_NX;
    RedisModule_ZsetAdd(key, index, data, &flags);
    // Check that we actually add a new entry during the append. This is only
    // necessary since we implement the log with a sorted set, so all entries
    // must be unique, or else we will have gaps in the log.
    // TODO(rkn): We need to get rid of this uniqueness requirement. We can
    // easily have multiple log events with the same message.
    if (flags != REDISMODULE_ZADD_ADDED) {
      // The following code is a workaround to store the data at a new unique
      // key. This is so redis doesn't crash (we currently have duplicate keys
      // for error conditions, which get delivered via pubsub).
      size_t len;
      const char *id_str = RedisModule_StringPtrLen(id, &len);
      RAY_LOG(INFO) << "Duplicate key: " << std::string(id_str, len);
      // Store the value into a unique new key, just to keep track of it and
      // make sure the log size grows.
      std::string postfix = std::to_string(index);
      RedisModuleString *new_id =
          RedisString_Format(ctx, "%S:%b", id, postfix.data(), postfix.size());
      RedisModuleKey *new_key;
      REPLY_AND_RETURN_IF_NOT_OK(OpenPrefixedKey(&new_key, ctx, prefix_str, new_id,
                                                 REDISMODULE_READ | REDISMODULE_WRITE,
                                                 mutated_key_str));
      RedisModule_ZsetAdd(new_key, index, data, &flags);
      REPLY_AND_RETURN_IF_FALSE(flags == REDISMODULE_ZADD_ADDED,
                                "Appended a duplicate entry");
    }
    return REDISMODULE_OK;
  } else {
    // The requested index did not match the current length of the log. Return
    // an error message as a string.
    static const char *reply = "ERR entry exists";
    RedisModule_ReplyWithStringBuffer(ctx, reply, strlen(reply));
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
    return PublishTableAdd(ctx, pubsub_channel_str, id, data);
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
/// \param pubsub_channel The pubsub channel name that notifications for
///        this key should be published to. When publishing to a specific
///        client, the channel name should be <pubsub_channel>:<client_id>.
/// \param id The ID of the key to append to.
/// \param data The data to append to the key.
/// \param index If this is set, then the data must be appended at this index.
///        If the current log is shorter or longer than the requested index,
///        then the append will fail and an error message will be returned as a
///        string.
/// \return OK if the append succeeds, or an error message string if the append
///         fails.
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
  RedisModule_AutoMemory(ctx);
  return module.ChainReplicate(ctx, argv, argc,
                               /*node_func=*/TableAppend_DoWrite,
                               /*tail_func=*/TableAppend_DoPublish);
}
#endif

/// A helper function to create and finish a GcsTableEntry, based on the
/// current value or values at the given key.
Status TableEntryToFlatbuf(RedisModuleKey *table_key, RedisModuleString *entry_id,
                           flatbuffers::FlatBufferBuilder &fbb) {
  auto key_type = RedisModule_KeyType(table_key);
  switch (key_type) {
  case REDISMODULE_KEYTYPE_STRING: {
    // Build the flatbuffer from the string data.
    size_t data_len = 0;
    char *data_buf = RedisModule_StringDMA(table_key, &data_len, REDISMODULE_READ);
    auto data = fbb.CreateString(data_buf, data_len);
    auto message = CreateGcsTableEntry(fbb, RedisStringToFlatbuf(fbb, entry_id),
                                       fbb.CreateVector(&data, 1));
    fbb.Finish(message);
  } break;
  case REDISMODULE_KEYTYPE_ZSET: {
    // Build the flatbuffer from the set of log entries.
    if (RedisModule_ZsetFirstInScoreRange(table_key, REDISMODULE_NEGATIVE_INFINITE,
                                          REDISMODULE_POSITIVE_INFINITE, 1,
                                          1) != REDISMODULE_OK) {
      return Status::RedisError("Empty zset or wrong type");
    }
    std::vector<flatbuffers::Offset<flatbuffers::String>> data;
    for (; !RedisModule_ZsetRangeEndReached(table_key);
         RedisModule_ZsetRangeNext(table_key)) {
      data.push_back(RedisStringToFlatbuf(
          fbb, RedisModule_ZsetRangeCurrentElement(table_key, NULL)));
    }
    auto message = CreateGcsTableEntry(fbb, RedisStringToFlatbuf(fbb, entry_id),
                                       fbb.CreateVector(data));
    fbb.Finish(message);
  } break;
  case REDISMODULE_KEYTYPE_EMPTY: {
    auto message = CreateGcsTableEntry(
        fbb, RedisStringToFlatbuf(fbb, entry_id),
        fbb.CreateVector(std::vector<flatbuffers::Offset<flatbuffers::String>>()));
    fbb.Finish(message);
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
  RedisModule_AutoMemory(ctx);

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
    // Serialize the data to a flatbuffer to return to the client.
    flatbuffers::FlatBufferBuilder fbb;
    REPLY_AND_RETURN_IF_NOT_OK(TableEntryToFlatbuf(table_key, id, fbb));
    RedisModule_ReplyWithStringBuffer(
        ctx, reinterpret_cast<const char *>(fbb.GetBufferPointer()), fbb.GetSize());
  }
  return REDISMODULE_OK;
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
  RedisModule_AutoMemory(ctx);

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
  REPLY_AND_RETURN_IF_NOT_OK(GetBroadcastKey(ctx, pubsub_channel_str, id, &notification_key));
  notification_map[notification_key].push_back(RedisString_ToString(client_channel));

  // Lookup the current value at the key.
  RedisModuleKey *table_key;
  REPLY_AND_RETURN_IF_NOT_OK(
      OpenPrefixedKey(&table_key, ctx, prefix_str, id, REDISMODULE_READ));
  // Publish the current value at the key to the client that is requesting
  // notifications. An empty notification will be published if the key is
  // empty.
  flatbuffers::FlatBufferBuilder fbb;
  REPLY_AND_RETURN_IF_NOT_OK(TableEntryToFlatbuf(table_key, id, fbb));
  RedisModule_Call(ctx, "PUBLISH", "sb", client_channel,
                   reinterpret_cast<const char *>(fbb.GetBufferPointer()), fbb.GetSize());

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
  RedisModule_AutoMemory(ctx);

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
  REPLY_AND_RETURN_IF_NOT_OK(GetBroadcastKey(ctx, pubsub_channel_str, id, &notification_key));
  auto it = notification_map.find(notification_key);
  if (it != notification_map.end()) {
    std::remove(it->second.begin(), it->second.end(), RedisString_ToString(client_channel));
    if (it->second.size() == 0) {
      notification_map.erase(it);
    }
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

Status is_nil(bool *out, const std::string &data) {
  if (data.size() != kUniqueIDSize) {
    return Status::RedisError("Size of data doesn't match size of UniqueID");
  }
  const uint8_t *d = reinterpret_cast<const uint8_t *>(data.data());
  for (int i = 0; i < kUniqueIDSize; ++i) {
    if (d[i] != 255) {
      *out = false;
    }
  }
  *out = true;
  return Status::OK();
}

// This is a temporary redis command that will be removed once
// the GCS uses https://github.com/pcmoritz/credis.
// Be careful, this only supports Task Table payloads.
int TableTestAndUpdate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                    int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];
  RedisModuleString *update_data = argv[4];

  RedisModuleKey *key;
  REPLY_AND_RETURN_IF_NOT_OK(
      OpenPrefixedKey(&key, ctx, prefix_str, id, REDISMODULE_READ | REDISMODULE_WRITE));

  size_t value_len = 0;
  char *value_buf = RedisModule_StringDMA(key, &value_len, REDISMODULE_READ);

  size_t update_len = 0;
  const char *update_buf = RedisModule_StringPtrLen(update_data, &update_len);

  auto data =
      flatbuffers::GetMutableRoot<TaskTableData>(reinterpret_cast<void *>(value_buf));

  auto update = flatbuffers::GetRoot<TaskTableTestAndUpdate>(update_buf);

  bool do_update = static_cast<int>(data->scheduling_state()) &
                   static_cast<int>(update->test_state_bitmask());

  bool is_nil_result;
  REPLY_AND_RETURN_IF_NOT_OK(is_nil(&is_nil_result, update->test_scheduler_id()->str()));
  if (!is_nil_result) {
    do_update =
        do_update && update->test_scheduler_id()->str() == data->scheduler_id()->str();
  }

  if (do_update) {
    REPLY_AND_RETURN_IF_FALSE(data->mutate_scheduling_state(update->update_state()),
                              "mutate_scheduling_state failed");
  }
  REPLY_AND_RETURN_IF_FALSE(data->mutate_updated(do_update), "mutate_updated failed");

  int result = RedisModule_ReplyWithStringBuffer(ctx, value_buf, value_len);

  return result;
}

std::string DebugString() {
  std::stringstream result;
  result << "RedisModule:";
  result << "\n- NotificationMap.size = " << notification_map.size();
  result << std::endl;
  return result.str();
}

int DebugString_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
  REDISMODULE_NOT_USED(argv);
  RedisModule_AutoMemory(ctx);

  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  std::string debug_string = DebugString();
  return RedisModule_ReplyWithStringBuffer(ctx, debug_string.data(), debug_string.size());
}

extern "C" {

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
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
                                "write", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_lookup", TableLookup_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR) {
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

  if (RedisModule_CreateCommand(ctx, "ray.table_test_and_update",
                                TableTestAndUpdate_RedisCommand, "write", 0, 0,
                                0) == REDISMODULE_ERR) {
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

} /* extern "C" */
