#include <string.h>

#include "ray/common/common_protocol.h"
#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"
#include "ray/util/logging.h"
#include "redis_string.h"
#include "redismodule.h"

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

// Various tables are maintained in redis:
//
// == OBJECT TABLE ==
//
// This consists of two parts:
// - The object location table, indexed by OL:object_id, which is the set of
//   plasma manager indices that have access to the object.
//   (In redis this is represented by a zset (sorted set).)
//
// - The object info table, indexed by OI:object_id, which is a hashmap of:
//     "hash" -> the hash of the object,
//     "data_size" -> the size of the object in bytes,
//     "task" -> the task ID that generated this object.
//     "is_put" -> 0 or 1.
//
// == TASK TABLE ==
//
// It maps each TT:task_id to a hash:
//   "state" -> the state of the task, encoded as a bit mask of scheduling_state
//              enum values in task.h,
//   "local_scheduler_id" -> the ID of the local scheduler the task is assigned
//                           to,
//   "TaskSpec" -> serialized bytes of a TaskInfo (defined in common.fbs), which
//                 describes the details this task.
//
// See also the definition of TaskReply in common.fbs.

#define OBJECT_INFO_PREFIX "OI:"
#define OBJECT_LOCATION_PREFIX "OL:"
#define OBJECT_NOTIFICATION_PREFIX "ON:"
#define TASK_PREFIX "TT:"
#define OBJECT_BCAST "BCAST"

#define OBJECT_CHANNEL_PREFIX "OC:"

#define CHECK_ERROR(STATUS, MESSAGE)                   \
  if ((STATUS) == REDISMODULE_ERR) {                   \
    return RedisModule_ReplyWithError(ctx, (MESSAGE)); \
  }

/// Parse a Redis string into a TablePubsub channel.
TablePubsub ParseTablePubsub(const RedisModuleString *pubsub_channel_str) {
  long long pubsub_channel_long;
  RAY_CHECK(RedisModule_StringToLongLong(pubsub_channel_str, &pubsub_channel_long) ==
            REDISMODULE_OK)
      << "Pubsub channel must be a valid TablePubsub";
  auto pubsub_channel = static_cast<TablePubsub>(pubsub_channel_long);
  RAY_CHECK(pubsub_channel >= TablePubsub::MIN && pubsub_channel <= TablePubsub::MAX)
      << "Pubsub channel must be a valid TablePubsub";
  return pubsub_channel;
}

/// Format a pubsub channel for a specific key. pubsub_channel_str should
/// contain a valid TablePubsub.
RedisModuleString *FormatPubsubChannel(RedisModuleCtx *ctx,
                                       const RedisModuleString *pubsub_channel_str,
                                       const RedisModuleString *id) {
  // Format the pubsub channel enum to a string. TablePubsub_MAX should be more
  // than enough digits, but add 1 just in case for the null terminator.
  char pubsub_channel[static_cast<int>(TablePubsub::MAX) + 1];
  sprintf(pubsub_channel, "%d", static_cast<int>(ParseTablePubsub(pubsub_channel_str)));
  return RedisString_Format(ctx, "%s:%S", pubsub_channel, id);
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
  RedisModuleKey *key =
      (RedisModuleKey *)RedisModule_OpenKey(ctx, prefixed_keyname, mode);
  return key;
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx, RedisModuleString *prefix_enum,
                                RedisModuleString *keyname, int mode,
                                RedisModuleString **mutated_key_str) {
  long long prefix_long;
  RAY_CHECK(RedisModule_StringToLongLong(prefix_enum, &prefix_long) == REDISMODULE_OK)
      << "Prefix must be a valid TablePrefix";
  auto prefix = static_cast<TablePrefix>(prefix_long);
  RAY_CHECK(prefix != TablePrefix::UNUSED) << "This table has no prefix registered";
  RAY_CHECK(prefix >= TablePrefix::MIN && prefix <= TablePrefix::MAX)
      << "Prefix must be a valid TablePrefix";
  return OpenPrefixedKey(ctx, EnumNameTablePrefix(prefix), keyname, mode,
                         mutated_key_str);
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx, const char *prefix,
                                RedisModuleString *keyname, int mode) {
  return OpenPrefixedKey(ctx, prefix, keyname, mode,
                         /*mutated_key_str=*/nullptr);
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx, RedisModuleString *prefix_enum,
                                RedisModuleString *keyname, int mode) {
  return OpenPrefixedKey(ctx, prefix_enum, keyname, mode,
                         /*mutated_key_str=*/nullptr);
}

/// Open the key used to store the channels that should be published to when an
/// update happens at the given keyname.
RedisModuleKey *OpenBroadcastKey(RedisModuleCtx *ctx,
                                 RedisModuleString *pubsub_channel_str,
                                 RedisModuleString *keyname, int mode) {
  RedisModuleString *channel = FormatPubsubChannel(ctx, pubsub_channel_str, keyname);
  RedisModuleString *prefixed_keyname = RedisString_Format(ctx, "BCAST:%S", channel);
  RedisModuleKey *key =
      (RedisModuleKey *)RedisModule_OpenKey(ctx, prefixed_keyname, mode);
  return key;
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

  // Publish the data to any clients who requested notifications on this key.
  RedisModuleKey *notification_key =
      OpenBroadcastKey(ctx, pubsub_channel_str, id, REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(notification_key) != REDISMODULE_KEYTYPE_EMPTY) {
    // NOTE(swang): Sets are not implemented yet, so we use ZSETs instead.
    CHECK_ERROR(
        RedisModule_ZsetFirstInScoreRange(notification_key, REDISMODULE_NEGATIVE_INFINITE,
                                          REDISMODULE_POSITIVE_INFINITE, 1, 1),
        "Unable to initialize zset iterator");
    for (; !RedisModule_ZsetRangeEndReached(notification_key);
         RedisModule_ZsetRangeNext(notification_key)) {
      RedisModuleString *client_channel =
          RedisModule_ZsetRangeCurrentElement(notification_key, NULL);
      RedisModuleCallReply *reply = RedisModule_Call(
          ctx, "PUBLISH", "sb", client_channel, fbb.GetBufferPointer(), fbb.GetSize());
      if (reply == NULL) {
        return RedisModule_ReplyWithError(ctx, "error during PUBLISH");
      }
    }
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

  RedisModuleKey *key = OpenPrefixedKey(
      ctx, prefix_str, id, REDISMODULE_READ | REDISMODULE_WRITE, mutated_key_str);
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

  TablePubsub pubsub_channel = ParseTablePubsub(pubsub_channel_str);

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
  RedisModuleKey *key = OpenPrefixedKey(
      ctx, prefix_str, id, REDISMODULE_READ | REDISMODULE_WRITE, mutated_key_str);
  // Determine the index at which the data should be appended. If no index is
  // requested, then is the current length of the log.
  size_t index = RedisModule_ValueLength(key);
  if (index_str != nullptr) {
    // Parse the requested index.
    long long requested_index;
    RAY_CHECK(RedisModule_StringToLongLong(index_str, &requested_index) ==
              REDISMODULE_OK);
    RAY_CHECK(requested_index >= 0);
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
    RAY_CHECK(flags == REDISMODULE_ZADD_ADDED) << "Appended a duplicate entry";
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
  TablePubsub pubsub_channel = ParseTablePubsub(pubsub_channel_str);
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
  RedisModule_AutoMemory(ctx);
  const int status = TableAppend_DoWrite(ctx, argv, argc,
                                         /*mutated_key_str=*/nullptr);
  if (status) {
    return status;
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
void TableEntryToFlatbuf(RedisModuleKey *table_key, RedisModuleString *entry_id,
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
    RAY_CHECK(RedisModule_ZsetFirstInScoreRange(table_key, REDISMODULE_NEGATIVE_INFINITE,
                                                REDISMODULE_POSITIVE_INFINITE, 1,
                                                1) == REDISMODULE_OK);
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
    RAY_LOG(FATAL) << "Invalid Redis type during lookup: " << key_type;
  }
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
  RedisModuleKey *table_key = OpenPrefixedKey(ctx, prefix_str, id, REDISMODULE_READ);
  if (table_key == nullptr) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    // Serialize the data to a flatbuffer to return to the client.
    flatbuffers::FlatBufferBuilder fbb;
    TableEntryToFlatbuf(table_key, id, fbb);
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
  RedisModuleString *client_channel =
      FormatPubsubChannel(ctx, pubsub_channel_str, client_id);

  // Add this client to the set of clients that should be notified when there
  // are changes to the key.
  RedisModuleKey *notification_key =
      OpenBroadcastKey(ctx, pubsub_channel_str, id, REDISMODULE_READ | REDISMODULE_WRITE);
  CHECK_ERROR(RedisModule_ZsetAdd(notification_key, 0.0, client_channel, NULL),
              "ZsetAdd failed.");

  // Lookup the current value at the key.
  RedisModuleKey *table_key = OpenPrefixedKey(ctx, prefix_str, id, REDISMODULE_READ);
  // Publish the current value at the key to the client that is requesting
  // notifications. An empty notification will be published if the key is
  // empty.
  flatbuffers::FlatBufferBuilder fbb;
  TableEntryToFlatbuf(table_key, id, fbb);
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
  RedisModuleString *client_channel =
      FormatPubsubChannel(ctx, pubsub_channel_str, client_id);

  // Remove this client from the set of clients that should be notified when
  // there are changes to the key.
  RedisModuleKey *notification_key =
      OpenBroadcastKey(ctx, pubsub_channel_str, id, REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(notification_key) != REDISMODULE_KEYTYPE_EMPTY) {
    RAY_CHECK(RedisModule_ZsetRem(notification_key, client_channel, NULL) ==
              REDISMODULE_OK);
    size_t size = RedisModule_ValueLength(notification_key);
    if (size == 0) {
      CHECK_ERROR(RedisModule_DeleteKey(notification_key), "Unable to delete zset key.");
    }
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

bool is_nil(const std::string &data) {
  RAY_CHECK(data.size() == kUniqueIDSize);
  const uint8_t *d = reinterpret_cast<const uint8_t *>(data.data());
  for (int i = 0; i < kUniqueIDSize; ++i) {
    if (d[i] != 255) {
      return false;
    }
  }
  return true;
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

  RedisModuleKey *key =
      OpenPrefixedKey(ctx, prefix_str, id, REDISMODULE_READ | REDISMODULE_WRITE);

  size_t value_len = 0;
  char *value_buf = RedisModule_StringDMA(key, &value_len, REDISMODULE_READ);

  size_t update_len = 0;
  const char *update_buf = RedisModule_StringPtrLen(update_data, &update_len);

  auto data =
      flatbuffers::GetMutableRoot<TaskTableData>(reinterpret_cast<void *>(value_buf));

  auto update = flatbuffers::GetRoot<TaskTableTestAndUpdate>(update_buf);

  bool do_update = static_cast<int>(data->scheduling_state()) &
                   static_cast<int>(update->test_state_bitmask());

  if (!is_nil(update->test_scheduler_id()->str())) {
    do_update =
        do_update && update->test_scheduler_id()->str() == data->scheduler_id()->str();
  }

  if (do_update) {
    RAY_CHECK(data->mutate_scheduling_state(update->update_state()));
  }
  RAY_CHECK(data->mutate_updated(do_update));

  int result = RedisModule_ReplyWithStringBuffer(ctx, value_buf, value_len);

  return result;
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
