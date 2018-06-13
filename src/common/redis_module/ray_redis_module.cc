#include <string.h>

#include "common_protocol.h"
#include "format/common_generated.h"
#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"
#include "redis_string.h"
#include "redismodule.h"
#include "task.h"

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

// NOTE(swang): The order of prefixes here must match the TablePrefix enum
// defined in src/ray/gcs/format/gcs.fbs.
static const char *table_prefixes[] = {
    NULL,         "TASK:",  "TASK:",     "CLIENT:",
    "OBJECT:",    "ACTOR:", "FUNCTION:", "TASK_RECONSTRUCTION:",
    "HEARTBEAT:",
};

/// Parse a Redis string into a TablePubsub channel.
TablePubsub ParseTablePubsub(const RedisModuleString *pubsub_channel_str) {
  long long pubsub_channel_long;
  RAY_CHECK(RedisModule_StringToLongLong(
                pubsub_channel_str, &pubsub_channel_long) == REDISMODULE_OK)
      << "Pubsub channel must be a valid TablePubsub";
  auto pubsub_channel = static_cast<TablePubsub>(pubsub_channel_long);
  RAY_CHECK(pubsub_channel >= TablePubsub::MIN &&
            pubsub_channel <= TablePubsub::MAX)
      << "Pubsub channel must be a valid TablePubsub";
  return pubsub_channel;
}

/// Format a pubsub channel for a specific key. pubsub_channel_str should
/// contain a valid TablePubsub.
RedisModuleString *FormatPubsubChannel(
    RedisModuleCtx *ctx,
    const RedisModuleString *pubsub_channel_str,
    const RedisModuleString *id) {
  // Format the pubsub channel enum to a string. TablePubsub_MAX should be more
  // than enough digits, but add 1 just in case for the null terminator.
  char pubsub_channel[static_cast<int>(TablePubsub::MAX) + 1];
  sprintf(pubsub_channel, "%d",
          static_cast<int>(ParseTablePubsub(pubsub_channel_str)));
  return RedisString_Format(ctx, "%s:%S", pubsub_channel, id);
}

// TODO(swang): This helper function should be deprecated by the version below,
// which uses enums for table prefixes.
RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                const char *prefix,
                                RedisModuleString *keyname,
                                int mode,
                                RedisModuleString **mutated_key_str) {
  RedisModuleString *prefixed_keyname =
      RedisString_Format(ctx, "%s%S", prefix, keyname);
  // Pass out the key being mutated, should the caller request so.
  if (mutated_key_str != nullptr) {
    *mutated_key_str = prefixed_keyname;
  }
  RedisModuleKey *key =
      (RedisModuleKey *) RedisModule_OpenKey(ctx, prefixed_keyname, mode);
  return key;
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                RedisModuleString *prefix_enum,
                                RedisModuleString *keyname,
                                int mode,
                                RedisModuleString **mutated_key_str) {
  long long prefix_long;
  RAY_CHECK(RedisModule_StringToLongLong(prefix_enum, &prefix_long) ==
            REDISMODULE_OK)
      << "Prefix must be a valid TablePrefix";
  auto prefix = static_cast<TablePrefix>(prefix_long);
  RAY_CHECK(prefix != TablePrefix::UNUSED)
      << "This table has no prefix registered";
  RAY_CHECK(prefix >= TablePrefix::MIN && prefix <= TablePrefix::MAX)
      << "Prefix must be a valid TablePrefix";
  return OpenPrefixedKey(ctx, table_prefixes[static_cast<long long>(prefix)],
                         keyname, mode, mutated_key_str);
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                const char *prefix,
                                RedisModuleString *keyname,
                                int mode) {
  return OpenPrefixedKey(ctx, prefix, keyname, mode,
                         /*mutated_key_str=*/nullptr);
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                RedisModuleString *prefix_enum,
                                RedisModuleString *keyname,
                                int mode) {
  return OpenPrefixedKey(ctx, prefix_enum, keyname, mode,
                         /*mutated_key_str=*/nullptr);
}

/// Open the key used to store the channels that should be published to when an
/// update happens at the given keyname.
RedisModuleKey *OpenBroadcastKey(RedisModuleCtx *ctx,
                                 RedisModuleString *pubsub_channel_str,
                                 RedisModuleString *keyname,
                                 int mode) {
  RedisModuleString *channel =
      FormatPubsubChannel(ctx, pubsub_channel_str, keyname);
  RedisModuleString *prefixed_keyname =
      RedisString_Format(ctx, "BCAST:%S", channel);
  RedisModuleKey *key =
      (RedisModuleKey *) RedisModule_OpenKey(ctx, prefixed_keyname, mode);
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
    flatbuffers::FlatBufferBuilder &fbb,
    RedisModuleString *redis_string) {
  size_t redis_string_size;
  const char *redis_string_str =
      RedisModule_StringPtrLen(redis_string, &redis_string_size);
  return fbb.CreateString(redis_string_str, redis_string_size);
}

/**
 * Publish a notification to a client's notification channel about an insertion
 * or deletion to the db client table.
 *
 * TODO(swang): Use flatbuffers for the notification message.
 * The format for the published notification is:
 *  <ray_client_id>:<client type> <manager_address> <is_insertion>
 * If no manager address is provided, manager_address will be set to ":". If
 * is_insertion is true, then the last field will be "1", else "0".
 *
 * @param ctx The Redis context.
 * @param ray_client_id The ID of the database client that was inserted or
 *        deleted.
 * @param client_type The type of client that was inserted or deleted.
 * @param manager_address An optional secondary address for the object manager
 *        associated with this database client.
 * @param is_insertion A boolean that's true if the update was an insertion and
 *        false if deletion.
 * @return True if the publish was successful and false otherwise.
 */
bool PublishDBClientNotification(RedisModuleCtx *ctx,
                                 RedisModuleString *ray_client_id,
                                 RedisModuleString *client_type,
                                 RedisModuleString *manager_address,
                                 bool is_insertion) {
  /* Construct strings to publish on the db client channel. */
  RedisModuleString *channel_name =
      RedisModule_CreateString(ctx, "db_clients", strlen("db_clients"));
  /* Construct the flatbuffers object to publish over the channel. */
  flatbuffers::FlatBufferBuilder fbb;
  /* Use an empty aux address if one is not passed in. */
  flatbuffers::Offset<flatbuffers::String> manager_address_str;
  if (manager_address != NULL) {
    manager_address_str = RedisStringToFlatbuf(fbb, manager_address);
  } else {
    manager_address_str = fbb.CreateString("", strlen(""));
  }
  /* Create the flatbuffers message. */
  auto message = CreateSubscribeToDBClientTableReply(
      fbb, RedisStringToFlatbuf(fbb, ray_client_id),
      RedisStringToFlatbuf(fbb, client_type), manager_address_str,
      is_insertion);
  fbb.Finish(message);
  /* Create a Redis string to publish by serializing the flatbuffers object. */
  RedisModuleString *client_info = RedisModule_CreateString(
      ctx, (const char *) fbb.GetBufferPointer(), fbb.GetSize());

  /* Publish the client info on the db client channel. */
  RedisModuleCallReply *reply;
  reply = RedisModule_Call(ctx, "PUBLISH", "ss", channel_name, client_info);
  return (reply != NULL);
}

/**
 * Register a client with Redis. This is called from a client with the command:
 *
 *     RAY.CONNECT <ray client id> <node ip address> <client type> <field 1>
 *         <value 1> <field 2> <value 2> ...
 *
 * The command can take an arbitrary number of pairs of field names and keys,
 * and these will be stored in a hashmap associated with this client. Several
 * fields are singled out for special treatment:
 *
 *     manager_address: This is provided by local schedulers and plasma
 *         managers and should be the address of the plasma manager that the
 *         client is associated with.  This is published to the "db_clients"
 *         channel by the RAY.CONNECT command.
 *
 * @param ray_client_id The db client ID of the client.
 * @param node_ip_address The IP address of the node the client is on.
 * @param client_type The type of the client (e.g., plasma_manager).
 * @return OK if the operation was successful.
 */
int Connect_RedisCommand(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  if (argc % 2 != 0) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *ray_client_id = argv[1];
  RedisModuleString *node_ip_address = argv[2];
  RedisModuleString *client_type = argv[3];

  /* Add this client to the Ray db client table. */
  RedisModuleKey *db_client_table_key =
      OpenPrefixedKey(ctx, DB_CLIENT_PREFIX, ray_client_id, REDISMODULE_WRITE);

  if (RedisModule_KeyType(db_client_table_key) != REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "Client already exists");
  }

  /* This will be used to construct a publish message. */
  RedisModuleString *manager_address = NULL;
  RedisModuleString *manager_address_key = RedisModule_CreateString(
      ctx, "manager_address", strlen("manager_address"));
  RedisModuleString *deleted = RedisModule_CreateString(ctx, "0", strlen("0"));

  RedisModule_HashSet(db_client_table_key, REDISMODULE_HASH_CFIELDS,
                      "ray_client_id", ray_client_id, "node_ip_address",
                      node_ip_address, "client_type", client_type, "deleted",
                      deleted, NULL);

  for (int i = 4; i < argc; i += 2) {
    RedisModuleString *key = argv[i];
    RedisModuleString *value = argv[i + 1];
    RedisModule_HashSet(db_client_table_key, REDISMODULE_HASH_NONE, key, value,
                        NULL);
    if (RedisModule_StringCompare(key, manager_address_key) == 0) {
      manager_address = value;
    }
  }
  /* Clean up. */
  if (!PublishDBClientNotification(ctx, ray_client_id, client_type,
                                   manager_address, true)) {
    return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

/**
 * Remove a client from Redis. This is called from a client with the command:
 *
 *     RAY.DISCONNECT <ray client id>
 *
 * This method also publishes a notification to all subscribers to the
 * db_clients channel. The notification consists of a message of the form "<ray
 * client id>:<client type>".
 *
 * @param ray_client_id The db client ID of the client.
 * @return OK if the operation was successful.
 */
int Disconnect_RedisCommand(RedisModuleCtx *ctx,
                            RedisModuleString **argv,
                            int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *ray_client_id = argv[1];

  /* Get the client type. */
  RedisModuleKey *db_client_table_key =
      OpenPrefixedKey(ctx, DB_CLIENT_PREFIX, ray_client_id, REDISMODULE_WRITE);

  RedisModuleString *deleted_string;
  RedisModule_HashGet(db_client_table_key, REDISMODULE_HASH_CFIELDS, "deleted",
                      &deleted_string, NULL);
  long long deleted;
  int parsed = RedisModule_StringToLongLong(deleted_string, &deleted);
  if (parsed != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Unable to parse deleted field");
  }

  bool published = true;
  if (deleted == 0) {
    /* Remove the client from the client table. */
    RedisModuleString *deleted =
        RedisModule_CreateString(ctx, "1", strlen("1"));
    RedisModule_HashSet(db_client_table_key, REDISMODULE_HASH_CFIELDS,
                        "deleted", deleted, NULL);

    RedisModuleString *client_type;
    RedisModuleString *manager_address;
    RedisModule_HashGet(db_client_table_key, REDISMODULE_HASH_CFIELDS,
                        "client_type", &client_type, "manager_address",
                        &manager_address, NULL);

    /* Publish the deletion notification on the db client channel. */
    published = PublishDBClientNotification(ctx, ray_client_id, client_type,
                                            manager_address, false);
  }

  if (!published) {
    /* Return an error message if we weren't able to publish the deletion
     * notification. */
    return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

/**
 * Lookup an entry in the object table.
 *
 * This is called from a client with the command:
 *
 *     RAY.OBJECT_TABLE_LOOKUP <object id>
 *
 * @param object_id A string representing the object ID.
 * @return A list, possibly empty, of plasma manager IDs that are listed in the
 *         object table as having the object. If there was no entry found in
 *         the object table, returns nil.
 */
int ObjectTableLookup_RedisCommand(RedisModuleCtx *ctx,
                                   RedisModuleString **argv,
                                   int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleKey *key =
      OpenPrefixedKey(ctx, OBJECT_LOCATION_PREFIX, argv[1], REDISMODULE_READ);

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    /* Return nil if no entry was found. */
    return RedisModule_ReplyWithNull(ctx);
  }
  if (RedisModule_ValueLength(key) == 0) {
    /* Return empty list if there are no managers. */
    return RedisModule_ReplyWithArray(ctx, 0);
  }

  CHECK_ERROR(
      RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE,
                                        REDISMODULE_POSITIVE_INFINITE, 1, 1),
      "Unable to initialize zset iterator");

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int num_results = 0;
  do {
    RedisModuleString *curr = RedisModule_ZsetRangeCurrentElement(key, NULL);
    RedisModule_ReplyWithString(ctx, curr);
    num_results += 1;
  } while (RedisModule_ZsetRangeNext(key));
  RedisModule_ReplySetArrayLength(ctx, num_results);

  return REDISMODULE_OK;
}

/**
 * Publish a notification to a client's object notification channel if at least
 * one manager is listed as having the object in the object table.
 *
 * @param ctx The Redis context.
 * @param client_id The ID of the client that is being notified.
 * @param object_id The object ID of interest.
 * @param key The opened key for the entry in the object table corresponding to
 *        the object ID of interest.
 * @return True if the publish was successful and false otherwise.
 */
bool PublishObjectNotification(RedisModuleCtx *ctx,
                               RedisModuleString *client_id,
                               RedisModuleString *object_id,
                               RedisModuleString *data_size,
                               RedisModuleKey *key) {
  flatbuffers::FlatBufferBuilder fbb;

  long long data_size_value;
  if (RedisModule_StringToLongLong(data_size, &data_size_value) !=
      REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "data_size must be integer");
  }

  std::vector<flatbuffers::Offset<flatbuffers::String>> manager_ids;
  CHECK_ERROR(
      RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE,
                                        REDISMODULE_POSITIVE_INFINITE, 1, 1),
      "Unable to initialize zset iterator");
  /* Loop over the managers in the object table for this object ID. */
  do {
    RedisModuleString *curr = RedisModule_ZsetRangeCurrentElement(key, NULL);
    manager_ids.push_back(RedisStringToFlatbuf(fbb, curr));
  } while (RedisModule_ZsetRangeNext(key));

  auto message = CreateSubscribeToNotificationsReply(
      fbb, RedisStringToFlatbuf(fbb, object_id), data_size_value,
      fbb.CreateVector(manager_ids));
  fbb.Finish(message);

  /* Publish the notification to the clients notification channel.
   * TODO(rkn): These notifications could be batched together. */
  RedisModuleString *channel_name =
      RedisString_Format(ctx, "%s%S", OBJECT_CHANNEL_PREFIX, client_id);

  RedisModuleString *payload = RedisModule_CreateString(
      ctx, (const char *) fbb.GetBufferPointer(), fbb.GetSize());

  RedisModuleCallReply *reply;
  reply = RedisModule_Call(ctx, "PUBLISH", "ss", channel_name, payload);
  if (reply == NULL) {
    return false;
  }
  return true;
}

// NOTE(pcmoritz): This is a temporary redis command that will be removed once
// the GCS uses https://github.com/pcmoritz/credis.
int PublishTaskTableAdd(RedisModuleCtx *ctx,
                        RedisModuleString *id,
                        RedisModuleString *data) {
  const char *buf = RedisModule_StringPtrLen(data, NULL);
  auto message = flatbuffers::GetRoot<TaskTableData>(buf);
  RAY_CHECK(message != nullptr);

  if (message->scheduling_state() == SchedulingState::WAITING ||
      message->scheduling_state() == SchedulingState::SCHEDULED) {
    /* Build the PUBLISH topic and message for task table subscribers. The
     * topic
     * is a string in the format "TASK_PREFIX:<local scheduler ID>:<state>".
     * The
     * message is a serialized SubscribeToTasksReply flatbuffer object. */
    std::string state =
        std::to_string(static_cast<int>(message->scheduling_state()));
    RedisModuleString *publish_topic = RedisString_Format(
        ctx, "%s%b:%s", TASK_PREFIX, message->scheduler_id()->str().data(),
        sizeof(DBClientID), state.c_str());

    /* Construct the flatbuffers object for the payload. */
    flatbuffers::FlatBufferBuilder fbb;
    /* Create the flatbuffers message. */
    auto msg =
        CreateTaskReply(fbb, RedisStringToFlatbuf(fbb, id),
                        static_cast<long long>(message->scheduling_state()),
                        fbb.CreateString(message->scheduler_id()),
                        fbb.CreateString(message->execution_dependencies()),
                        fbb.CreateString(message->task_info()),
                        message->spillback_count(), true /* not used */);
    fbb.Finish(msg);

    RedisModuleString *publish_message = RedisModule_CreateString(
        ctx, (const char *) fbb.GetBufferPointer(), fbb.GetSize());

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "PUBLISH", "ss", publish_topic, publish_message);

    /* See how many clients received this publish. */
    long long num_clients = RedisModule_CallReplyInteger(reply);
    RAY_CHECK(num_clients <= 1) << "Published to " << num_clients
                                << " clients.";
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
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
int PublishTableAdd(RedisModuleCtx *ctx,
                    RedisModuleString *pubsub_channel_str,
                    RedisModuleString *id,
                    RedisModuleString *data) {
  // Serialize the notification to send.
  flatbuffers::FlatBufferBuilder fbb;
  auto data_flatbuf = RedisStringToFlatbuf(fbb, data);
  auto message = CreateGcsTableEntry(fbb, RedisStringToFlatbuf(fbb, id),
                                     fbb.CreateVector(&data_flatbuf, 1));
  fbb.Finish(message);

  // Write the data back to any subscribers that are listening to all table
  // notifications.
  RedisModuleCallReply *reply =
      RedisModule_Call(ctx, "PUBLISH", "sb", pubsub_channel_str,
                       fbb.GetBufferPointer(), fbb.GetSize());
  if (reply == NULL) {
    return RedisModule_ReplyWithError(ctx, "error during PUBLISH");
  }

  // Publish the data to any clients who requested notifications on this key.
  RedisModuleKey *notification_key = OpenBroadcastKey(
      ctx, pubsub_channel_str, id, REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(notification_key) != REDISMODULE_KEYTYPE_EMPTY) {
    // NOTE(swang): Sets are not implemented yet, so we use ZSETs instead.
    CHECK_ERROR(RedisModule_ZsetFirstInScoreRange(
                    notification_key, REDISMODULE_NEGATIVE_INFINITE,
                    REDISMODULE_POSITIVE_INFINITE, 1, 1),
                "Unable to initialize zset iterator");
    for (; !RedisModule_ZsetRangeEndReached(notification_key);
         RedisModule_ZsetRangeNext(notification_key)) {
      RedisModuleString *client_channel =
          RedisModule_ZsetRangeCurrentElement(notification_key, NULL);
      RedisModuleCallReply *reply =
          RedisModule_Call(ctx, "PUBLISH", "sb", client_channel,
                           fbb.GetBufferPointer(), fbb.GetSize());
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

int TableAdd_DoWrite(RedisModuleCtx *ctx,
                     RedisModuleString **argv,
                     int argc,
                     RedisModuleString **mutated_key_str) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];

  RedisModuleKey *key =
      OpenPrefixedKey(ctx, prefix_str, id, REDISMODULE_READ | REDISMODULE_WRITE,
                      mutated_key_str);
  RedisModule_StringSet(key, data);
  return REDISMODULE_OK;
}

int TableAdd_DoPublish(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString *pubsub_channel_str = argv[2];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];

  TablePubsub pubsub_channel = ParseTablePubsub(pubsub_channel_str);

  if (pubsub_channel == TablePubsub::TASK) {
    // Publish the task to its subscribers.
    // TODO(swang): This is only necessary for legacy Ray and should be removed
    // once we switch to using the new GCS API for the task table.
    return PublishTaskTableAdd(ctx, id, data);
  } else if (pubsub_channel != TablePubsub::NO_PUBLISH) {
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
int TableAdd_RedisCommand(RedisModuleCtx *ctx,
                          RedisModuleString **argv,
                          int argc) {
  RedisModule_AutoMemory(ctx);
  TableAdd_DoWrite(ctx, argv, argc, /*mutated_key_str=*/nullptr);
  return TableAdd_DoPublish(ctx, argv, argc);
}

#if RAY_USE_NEW_GCS
int ChainTableAdd_RedisCommand(RedisModuleCtx *ctx,
                               RedisModuleString **argv,
                               int argc) {
  return module.ChainReplicate(ctx, argv, argc, /*node_func=*/TableAdd_DoWrite,
                               /*tail_func=*/TableAdd_DoPublish);
}
#endif

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
int TableAppend_RedisCommand(RedisModuleCtx *ctx,
                             RedisModuleString **argv,
                             int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc < 5 || argc > 6) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *pubsub_channel_str = argv[2];
  RedisModuleString *id = argv[3];
  RedisModuleString *data = argv[4];
  RedisModuleString *index_str = nullptr;
  if (argc == 6) {
    index_str = argv[5];
  }

  // Set the keys in the table.
  RedisModuleKey *key = OpenPrefixedKey(ctx, prefix_str, id,
                                        REDISMODULE_READ | REDISMODULE_WRITE);
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
    RAY_CHECK(flags == REDISMODULE_ZADD_ADDED) << "Appended a duplicate entry";
    // Publish a message on the requested pubsub channel if necessary.
    TablePubsub pubsub_channel = ParseTablePubsub(pubsub_channel_str);
    if (pubsub_channel != TablePubsub::NO_PUBLISH) {
      // All other pubsub channels write the data back directly onto the
      // channel.
      return PublishTableAdd(ctx, pubsub_channel_str, id, data);
    } else {
      return RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
  } else {
    // The requested index did not match the current length of the log. Return
    // an error message as a string.
    const char *reply = "ERR entry exists";
    return RedisModule_ReplyWithStringBuffer(ctx, reply, strlen(reply));
  }
}

/// A helper function to create and finish a GcsTableEntry, based on the
/// current value or values at the given key.
void TableEntryToFlatbuf(RedisModuleKey *table_key,
                         RedisModuleString *entry_id,
                         flatbuffers::FlatBufferBuilder &fbb) {
  auto key_type = RedisModule_KeyType(table_key);
  switch (key_type) {
  case REDISMODULE_KEYTYPE_STRING: {
    // Build the flatbuffer from the string data.
    size_t data_len = 0;
    char *data_buf =
        RedisModule_StringDMA(table_key, &data_len, REDISMODULE_READ);
    auto data = fbb.CreateString(data_buf, data_len);
    auto message = CreateGcsTableEntry(fbb, RedisStringToFlatbuf(fbb, entry_id),
                                       fbb.CreateVector(&data, 1));
    fbb.Finish(message);
  } break;
  case REDISMODULE_KEYTYPE_ZSET: {
    // Build the flatbuffer from the set of log entries.
    RAY_CHECK(RedisModule_ZsetFirstInScoreRange(
                  table_key, REDISMODULE_NEGATIVE_INFINITE,
                  REDISMODULE_POSITIVE_INFINITE, 1, 1) == REDISMODULE_OK);
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
int TableLookup_RedisCommand(RedisModuleCtx *ctx,
                             RedisModuleString **argv,
                             int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];

  // Lookup the data at the key.
  RedisModuleKey *table_key =
      OpenPrefixedKey(ctx, prefix_str, id, REDISMODULE_READ);
  if (table_key == nullptr) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    // Serialize the data to a flatbuffer to return to the client.
    flatbuffers::FlatBufferBuilder fbb;
    TableEntryToFlatbuf(table_key, id, fbb);
    RedisModule_ReplyWithStringBuffer(
        ctx, reinterpret_cast<const char *>(fbb.GetBufferPointer()),
        fbb.GetSize());
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
int TableRequestNotifications_RedisCommand(RedisModuleCtx *ctx,
                                           RedisModuleString **argv,
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
  RedisModuleKey *notification_key = OpenBroadcastKey(
      ctx, pubsub_channel_str, id, REDISMODULE_READ | REDISMODULE_WRITE);
  CHECK_ERROR(RedisModule_ZsetAdd(notification_key, 0.0, client_channel, NULL),
              "ZsetAdd failed.");

  // Lookup the current value at the key.
  RedisModuleKey *table_key =
      OpenPrefixedKey(ctx, prefix_str, id, REDISMODULE_READ);
  if (table_key != nullptr) {
    // Publish the current value at the key to the client that is requesting
    // notifications.
    flatbuffers::FlatBufferBuilder fbb;
    TableEntryToFlatbuf(table_key, id, fbb);
    RedisModule_Call(ctx, "PUBLISH", "sb", client_channel,
                     reinterpret_cast<const char *>(fbb.GetBufferPointer()),
                     fbb.GetSize());
  }

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
int TableCancelNotifications_RedisCommand(RedisModuleCtx *ctx,
                                          RedisModuleString **argv,
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
  RedisModuleKey *notification_key = OpenBroadcastKey(
      ctx, pubsub_channel_str, id, REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(notification_key) != REDISMODULE_KEYTYPE_EMPTY) {
    RAY_CHECK(RedisModule_ZsetRem(notification_key, client_channel, NULL) ==
              REDISMODULE_OK);
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
int TableTestAndUpdate_RedisCommand(RedisModuleCtx *ctx,
                                    RedisModuleString **argv,
                                    int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleString *prefix_str = argv[1];
  RedisModuleString *id = argv[3];
  RedisModuleString *update_data = argv[4];

  RedisModuleKey *key = OpenPrefixedKey(ctx, prefix_str, id,
                                        REDISMODULE_READ | REDISMODULE_WRITE);

  size_t value_len = 0;
  char *value_buf = RedisModule_StringDMA(key, &value_len, REDISMODULE_READ);

  size_t update_len = 0;
  const char *update_buf = RedisModule_StringPtrLen(update_data, &update_len);

  auto data = flatbuffers::GetMutableRoot<TaskTableData>(
      reinterpret_cast<void *>(value_buf));

  auto update = flatbuffers::GetRoot<TaskTableTestAndUpdate>(update_buf);

  bool do_update = static_cast<int>(data->scheduling_state()) &
                   static_cast<int>(update->test_state_bitmask());

  if (!is_nil(update->test_scheduler_id()->str())) {
    do_update =
        do_update &&
        update->test_scheduler_id()->str() == data->scheduler_id()->str();
  }

  if (do_update) {
    RAY_CHECK(data->mutate_scheduling_state(update->update_state()));
  }
  RAY_CHECK(data->mutate_updated(do_update));

  int result = RedisModule_ReplyWithStringBuffer(ctx, value_buf, value_len);

  return result;
}

/**
 * Add a new entry to the object table or update an existing one.
 *
 * This is called from a client with the command:
 *
 *     RAY.OBJECT_TABLE_ADD <object id> <data size> <hash string> <manager id>
 *
 * @param object_id A string representing the object ID.
 * @param data_size An integer which is the object size in bytes.
 * @param hash_string A string which is a hash of the object.
 * @param manager A string which represents the manager ID of the plasma manager
 *        that has the object.
 * @return OK if the operation was successful. If the same object_id is already
 *         present with a different hash value, the entry is still added, but
 *         an error with string "hash mismatch" is returned.
 */
int ObjectTableAdd_RedisCommand(RedisModuleCtx *ctx,
                                RedisModuleString **argv,
                                int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *object_id = argv[1];
  RedisModuleString *data_size = argv[2];
  RedisModuleString *new_hash = argv[3];
  RedisModuleString *manager = argv[4];

  long long data_size_value;
  if (RedisModule_StringToLongLong(data_size, &data_size_value) !=
      REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "data_size must be integer");
  }

  /* Set the fields in the object info table. */
  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id,
                        REDISMODULE_READ | REDISMODULE_WRITE);

  /* Check if this object was already registered and if the hashes agree. */
  bool hash_mismatch = false;
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    RedisModuleString *existing_hash;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "hash", &existing_hash,
                        NULL);
    /* The existing hash may be NULL even if the key is present because a call
     * to RAY.RESULT_TABLE_ADD may have already created the key. */
    if (existing_hash != NULL) {
      /* Check whether the new hash value matches the old one. If not, we will
       * later return the "hash mismatch" error. */
      hash_mismatch = (RedisModule_StringCompare(existing_hash, new_hash) != 0);
    }
  }

  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "hash", new_hash, NULL);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "data_size", data_size,
                      NULL);

  /* Add the location in the object location table. */
  RedisModuleKey *table_key;
  table_key = OpenPrefixedKey(ctx, OBJECT_LOCATION_PREFIX, object_id,
                              REDISMODULE_READ | REDISMODULE_WRITE);

  /* Sets are not implemented yet, so we use ZSETs instead. */
  RedisModule_ZsetAdd(table_key, 0.0, manager, NULL);

  RedisModuleString *bcast_client_str =
      RedisModule_CreateString(ctx, OBJECT_BCAST, strlen(OBJECT_BCAST));
  bool success = PublishObjectNotification(ctx, bcast_client_str, object_id,
                                           data_size, table_key);
  if (!success) {
    /* The publish failed somehow. */
    return RedisModule_ReplyWithError(ctx, "PUBLISH BCAST unsuccessful");
  }

  /* Get the zset of clients that requested a notification about the
   * availability of this object. */
  RedisModuleKey *object_notification_key =
      OpenPrefixedKey(ctx, OBJECT_NOTIFICATION_PREFIX, object_id,
                      REDISMODULE_READ | REDISMODULE_WRITE);
  /* If the zset exists, initialize the key to iterate over the zset. */
  if (RedisModule_KeyType(object_notification_key) !=
      REDISMODULE_KEYTYPE_EMPTY) {
    CHECK_ERROR(RedisModule_ZsetFirstInScoreRange(
                    object_notification_key, REDISMODULE_NEGATIVE_INFINITE,
                    REDISMODULE_POSITIVE_INFINITE, 1, 1),
                "Unable to initialize zset iterator");
    /* Iterate over the list of clients that requested notifiations about the
     * availability of this object, and publish notifications to their object
     * notification channels. */

    do {
      RedisModuleString *client_id =
          RedisModule_ZsetRangeCurrentElement(object_notification_key, NULL);
      /* TODO(rkn): Some computation could be saved by batching the string
       * constructions in the multiple calls to PublishObjectNotification
       * together. */
      bool success = PublishObjectNotification(ctx, client_id, object_id,
                                               data_size, table_key);
      if (!success) {
        /* The publish failed somehow. */
        return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
      }
    } while (RedisModule_ZsetRangeNext(object_notification_key));
    /* Now that the clients have been notified, remove the zset of clients
     * waiting for notifications. */
    CHECK_ERROR(RedisModule_DeleteKey(object_notification_key),
                "Unable to delete zset key.");
  }

  if (hash_mismatch) {
    return RedisModule_ReplyWithError(ctx, "hash mismatch");
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
  }
}

/**
 * Remove a manager from a location entry in the object table.
 *
 * This is called from a client with the command:
 *
 *     RAY.OBJECT_TABLE_REMOVE <object id> <manager id>
 *
 * @param object_id A string representing the object ID.
 * @param manager A string which represents the manager ID of the plasma manager
 *        to remove.
 * @return OK if the operation was successful or an error with string
 *         "object not found" if the entry for the object_id doesn't exist. The
 *         operation is counted as a success if the manager was already not in
 *         the entry.
 */
int ObjectTableRemove_RedisCommand(RedisModuleCtx *ctx,
                                   RedisModuleString **argv,
                                   int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *object_id = argv[1];
  RedisModuleString *manager = argv[2];

  /* Remove the location from the object location table. */
  RedisModuleKey *table_key;
  table_key = OpenPrefixedKey(ctx, OBJECT_LOCATION_PREFIX, object_id,
                              REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(table_key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "object not found");
  }

  RedisModule_ZsetRem(table_key, manager, NULL);

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

/**
 * Request notifications about the presence of some object IDs. This command
 * takes a list of object IDs. For each object ID, the reply will be the list
 * of plasma managers that contain the object. If the list of plasma managers
 * is currently nonempty, then the reply will happen immediately. Else, the
 * reply will come later, on the first invocation of `RAY.OBJECT_TABLE_ADD`
 * following this call.
 *
 * This is called from a client with the command:
 *
 *    RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS <client id> <object id1>
 *        <object id2> ...
 *
 * @param client_id The ID of the client that is requesting the notifications.
 * @param object_id(n) The ID of the nth object ID that is passed to this
 *        command. This command can take any number of object IDs.
 * @return OK if the operation was successful.
 */
int ObjectTableRequestNotifications_RedisCommand(RedisModuleCtx *ctx,
                                                 RedisModuleString **argv,
                                                 int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  /* The first argument is the client ID. The other arguments are object IDs. */
  RedisModuleString *client_id = argv[1];

  /* Loop over the object ID arguments to this command. */
  for (int i = 2; i < argc; ++i) {
    RedisModuleString *object_id = argv[i];
    RedisModuleKey *key = OpenPrefixedKey(ctx, OBJECT_LOCATION_PREFIX,
                                          object_id, REDISMODULE_READ);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY ||
        RedisModule_ValueLength(key) == 0) {
      /* This object ID is currently not present, so make a note that this
       * client should be notified when this object ID becomes available. */
      RedisModuleKey *object_notification_key =
          OpenPrefixedKey(ctx, OBJECT_NOTIFICATION_PREFIX, object_id,
                          REDISMODULE_READ | REDISMODULE_WRITE);
      /* Add this client to the list of clients that will be notified when this
       * object becomes available. */
      CHECK_ERROR(
          RedisModule_ZsetAdd(object_notification_key, 0.0, client_id, NULL),
          "ZsetAdd failed.");
    } else {
      /* Publish a notification to the client's object notification channel. */
      /* Extract the data_size first. */
      RedisModuleKey *object_info_key;
      object_info_key =
          OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_READ);
      if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, "requested object not found");
      }
      RedisModuleString *existing_data_size;
      RedisModule_HashGet(object_info_key, REDISMODULE_HASH_CFIELDS,
                          "data_size", &existing_data_size, NULL);
      if (existing_data_size == NULL) {
        return RedisModule_ReplyWithError(ctx,
                                          "no data_size field in object info");
      }

      bool success = PublishObjectNotification(ctx, client_id, object_id,
                                               existing_data_size, key);
      if (!success) {
        /* The publish failed somehow. */
        return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
      }
    }
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

int ObjectInfoSubscribe_RedisCommand(RedisModuleCtx *ctx,
                                     RedisModuleString **argv,
                                     int argc) {
  RedisModule_AutoMemory(ctx);

  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

/**
 * Add a new entry to the result table or update an existing one.
 *
 * This is called from a client with the command:
 *
 *     RAY.RESULT_TABLE_ADD <object id> <task id> <is_put>
 *
 * @param object_id A string representing the object ID.
 * @param task_id A string representing the task ID of the task that produced
 *        the object.
 * @param is_put An integer that is 1 if the object was created through ray.put
 *        and 0 if created by return value.
 * @return OK if the operation was successful.
 */
int ResultTableAdd_RedisCommand(RedisModuleCtx *ctx,
                                RedisModuleString **argv,
                                int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }

  /* Set the task ID under field "task" in the object info table. */
  RedisModuleString *object_id = argv[1];
  RedisModuleString *task_id = argv[2];
  RedisModuleString *is_put = argv[3];

  /* Check to make sure the is_put field was a 0 or a 1. */
  long long is_put_integer;
  if ((RedisModule_StringToLongLong(is_put, &is_put_integer) !=
       REDISMODULE_OK) ||
      (is_put_integer != 0 && is_put_integer != 1)) {
    return RedisModule_ReplyWithError(
        ctx, "The is_put field must be either a 0 or a 1.");
  }

  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_WRITE);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "task", task_id, "is_put",
                      is_put, NULL);

  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

/**
 * Reply with information about a task ID. This is used by
 * RAY.RESULT_TABLE_LOOKUP and RAY.TASK_TABLE_GET.
 *
 * @param ctx The Redis context.
 * @param task_id The task ID of the task to reply about.
 * @param updated A boolean representing whether the task was updated during
 *        this operation. This field is only used for
 *        RAY.TASK_TABLE_TEST_AND_UPDATE operations.
 * @return NIL if the task ID is not in the task table. An error if the task ID
 *         is in the task table but the appropriate fields are not there, and
 *         an array of the task scheduling state, the local scheduler ID, and
 *         the task spec for the task otherwise.
 */
int ReplyWithTask(RedisModuleCtx *ctx,
                  RedisModuleString *task_id,
                  bool updated) {
  RedisModuleKey *key =
      OpenPrefixedKey(ctx, TASK_PREFIX, task_id, REDISMODULE_READ);

  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    /* If the key exists, look up the fields and return them in an array. */
    RedisModuleString *state = NULL;
    RedisModuleString *local_scheduler_id = NULL;
    RedisModuleString *execution_dependencies = NULL;
    RedisModuleString *task_spec = NULL;
    RedisModuleString *spillback_count = NULL;
    RedisModule_HashGet(
        key, REDISMODULE_HASH_CFIELDS, "state", &state, "local_scheduler_id",
        &local_scheduler_id, "execution_dependencies", &execution_dependencies,
        "TaskSpec", &task_spec, "spillback_count", &spillback_count, NULL);
    if (state == NULL || local_scheduler_id == NULL ||
        execution_dependencies == NULL || task_spec == NULL ||
        spillback_count == NULL) {
      /* We must have either all fields or no fields. */
      return RedisModule_ReplyWithError(
          ctx, "Missing fields in the task table entry");
    }

    long long state_integer;
    long long spillback_count_val;
    if ((RedisModule_StringToLongLong(state, &state_integer) !=
         REDISMODULE_OK) ||
        (state_integer < 0) ||
        (RedisModule_StringToLongLong(spillback_count, &spillback_count_val) !=
         REDISMODULE_OK) ||
        (spillback_count_val < 0)) {
      return RedisModule_ReplyWithError(
          ctx, "Found invalid scheduling state or spillback count.");
    }

    flatbuffers::FlatBufferBuilder fbb;
    auto message = CreateTaskReply(
        fbb, RedisStringToFlatbuf(fbb, task_id), state_integer,
        RedisStringToFlatbuf(fbb, local_scheduler_id),
        RedisStringToFlatbuf(fbb, execution_dependencies),
        RedisStringToFlatbuf(fbb, task_spec), spillback_count_val, updated);
    fbb.Finish(message);

    RedisModuleString *reply = RedisModule_CreateString(
        ctx, (char *) fbb.GetBufferPointer(), fbb.GetSize());
    RedisModule_ReplyWithString(ctx, reply);
  } else {
    /* If the key does not exist, return nil. */
    RedisModule_ReplyWithNull(ctx);
  }

  return REDISMODULE_OK;
}

/**
 * Lookup an entry in the result table.
 *
 * This is called from a client with the command:
 *
 *     RAY.RESULT_TABLE_LOOKUP <object id>
 *
 * @param object_id A string representing the object ID.
 * @return NIL if the object ID is not in the result table. Otherwise, this
 *         returns a ResultTableReply flatbuffer.
 */
int ResultTableLookup_RedisCommand(RedisModuleCtx *ctx,
                                   RedisModuleString **argv,
                                   int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  /* Get the task ID under field "task" in the object info table. */
  RedisModuleString *object_id = argv[1];

  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_READ);

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithNull(ctx);
  }

  RedisModuleString *task_id;
  RedisModuleString *is_put;
  RedisModuleString *data_size;
  RedisModuleString *hash;
  RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "task", &task_id, "is_put",
                      &is_put, "data_size", &data_size, "hash", &hash, NULL);

  if (task_id == NULL || is_put == NULL) {
    return RedisModule_ReplyWithNull(ctx);
  }

  /* Check to make sure the is_put field was a 0 or a 1. */
  long long is_put_integer;
  if (RedisModule_StringToLongLong(is_put, &is_put_integer) != REDISMODULE_OK ||
      (is_put_integer != 0 && is_put_integer != 1)) {
    return RedisModule_ReplyWithError(
        ctx, "The is_put field must be either a 0 or a 1.");
  }

  /* Make and return the flatbuffer reply. */
  flatbuffers::FlatBufferBuilder fbb;
  long long data_size_value;

  if (data_size == NULL) {
    data_size_value = -1;
  } else {
    RedisModule_StringToLongLong(data_size, &data_size_value);
    RAY_CHECK(RedisModule_StringToLongLong(data_size, &data_size_value) ==
              REDISMODULE_OK);
  }

  flatbuffers::Offset<flatbuffers::String> hash_str;
  if (hash == NULL) {
    hash_str = fbb.CreateString("", strlen(""));
  } else {
    hash_str = RedisStringToFlatbuf(fbb, hash);
  }

  flatbuffers::Offset<ResultTableReply> message =
      CreateResultTableReply(fbb, RedisStringToFlatbuf(fbb, task_id),
                             bool(is_put_integer), data_size_value, hash_str);

  fbb.Finish(message);
  RedisModuleString *reply = RedisModule_CreateString(
      ctx, (const char *) fbb.GetBufferPointer(), fbb.GetSize());
  RedisModule_ReplyWithString(ctx, reply);

  return REDISMODULE_OK;
}

int TaskTableWrite(RedisModuleCtx *ctx,
                   RedisModuleString *task_id,
                   RedisModuleString *state,
                   RedisModuleString *local_scheduler_id,
                   RedisModuleString *execution_dependencies,
                   RedisModuleString *spillback_count,
                   RedisModuleString *task_spec) {
  /* Extract the scheduling state. */
  long long state_value;
  if (RedisModule_StringToLongLong(state, &state_value) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "scheduling state must be integer");
  }

  long long spillback_count_value;
  if (RedisModule_StringToLongLong(spillback_count, &spillback_count_value) !=
      REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "spillback count must be integer");
  }
  /* Add the task to the task table. If no spec was provided, get the existing
   * spec out of the task table so we can publish it. */
  RedisModuleString *existing_task_spec = NULL;
  RedisModuleKey *key =
      OpenPrefixedKey(ctx, TASK_PREFIX, task_id, REDISMODULE_WRITE);
  if (task_spec == NULL) {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state,
                        "local_scheduler_id", local_scheduler_id,
                        "execution_dependencies", execution_dependencies,
                        "spillback_count", spillback_count, NULL);
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "TaskSpec",
                        &existing_task_spec, NULL);
    if (existing_task_spec == NULL) {
      return RedisModule_ReplyWithError(
          ctx, "Cannot update a task that doesn't exist yet");
    }
  } else {
    RedisModule_HashSet(
        key, REDISMODULE_HASH_CFIELDS, "state", state, "local_scheduler_id",
        local_scheduler_id, "execution_dependencies", execution_dependencies,
        "TaskSpec", task_spec, "spillback_count", spillback_count, NULL);
  }

  if (static_cast<TaskStatus>(state_value) == TaskStatus::WAITING ||
      static_cast<TaskStatus>(state_value) == TaskStatus::SCHEDULED) {
    /* Build the PUBLISH topic and message for task table subscribers. The
     * topic is a string in the format
     * "TASK_PREFIX:<local scheduler ID>:<state>". The message is a serialized
     * SubscribeToTasksReply flatbuffer object. */
    RedisModuleString *publish_topic = RedisString_Format(
        ctx, "%s%S:%S", TASK_PREFIX, local_scheduler_id, state);

    /* Construct the flatbuffers object for the payload. */
    flatbuffers::FlatBufferBuilder fbb;
    /* Use the old task spec if the current one is NULL. */
    RedisModuleString *task_spec_to_use;
    if (task_spec != NULL) {
      task_spec_to_use = task_spec;
    } else {
      task_spec_to_use = existing_task_spec;
    }
    /* Create the flatbuffers message. */
    auto message = CreateTaskReply(
        fbb, RedisStringToFlatbuf(fbb, task_id), state_value,
        RedisStringToFlatbuf(fbb, local_scheduler_id),
        RedisStringToFlatbuf(fbb, execution_dependencies),
        RedisStringToFlatbuf(fbb, task_spec_to_use), spillback_count_value,
        true);  // The updated field is not used.
    fbb.Finish(message);

    RedisModuleString *publish_message = RedisModule_CreateString(
        ctx, (const char *) fbb.GetBufferPointer(), fbb.GetSize());

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "PUBLISH", "ss", publish_topic, publish_message);

    /* See how many clients received this publish. */
    long long num_clients = RedisModule_CallReplyInteger(reply);
    RAY_CHECK(num_clients <= 1) << "Published to " << num_clients
                                << " clients.";

    if (reply == NULL) {
      return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
    }

    if (num_clients == 0) {
      /* This reply will be received by redis_task_table_update_callback or
       * redis_task_table_add_task_callback in redis.cc, which will then reissue
       * the command. */
      return RedisModule_ReplyWithError(ctx,
                                        "No subscribers received message.");
    }
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

/**
 * Add a new entry to the task table. This will overwrite any existing entry
 * with the same task ID.
 *
 * This is called from a client with the command:
 *
 *     RAY.TASK_TABLE_ADD <task ID> <state> <local scheduler ID>
 *         <execution dependencies> <task spec>
 *
 * @param task_id A string that is the ID of the task.
 * @param state A string that is the current scheduling state (a
 *        scheduling_state enum instance).
 * @param local_scheduler_id A string that is the ray client ID of the
 *        associated local scheduler, if any.
 * @param execution_dependencies A string that is the list of execution
 *        dependencies.
 * @param task_spec A string that is the specification of the task, which can
 *        be cast to a `task_spec`.
 * @return OK if the operation was successful.
 */
int TaskTableAddTask_RedisCommand(RedisModuleCtx *ctx,
                                  RedisModuleString **argv,
                                  int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 7) {
    return RedisModule_WrongArity(ctx);
  }

  return TaskTableWrite(ctx, argv[1], argv[2], argv[3], argv[4], argv[5],
                        argv[6]);
}

/**
 * Update an entry in the task table. This does not update the task
 * specification in the table.
 *
 * This is called from a client with the command:
 *
 *     RAY.TASK_TABLE_UPDATE <task ID> <state> <local scheduler ID>
 *         <execution dependencies>
 *
 * @param task_id A string that is the ID of the task.
 * @param state A string that is the current scheduling state (a
 *        scheduling_state enum instance).
 * @param ray_client_id A string that is the ray client ID of the associated
 *        local scheduler, if any.
 * @param execution_dependencies A string that is the list of execution
 *        dependencies.
 * @return OK if the operation was successful.
 */
int TaskTableUpdate_RedisCommand(RedisModuleCtx *ctx,
                                 RedisModuleString **argv,
                                 int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 6) {
    return RedisModule_WrongArity(ctx);
  }

  return TaskTableWrite(ctx, argv[1], argv[2], argv[3], argv[4], argv[5], NULL);
}

/**
 * Test and update an entry in the task table if the current value matches the
 * test value bitmask. This does not update the task specification in the
 * table.
 *
 * This is called from a client with the command:
 *
 *     RAY.TASK_TABLE_TEST_AND_UPDATE <task ID> <test state bitmask> <state>
 *         <local scheduler ID> <test local scheduler ID (optional)>
 *
 * @param task_id A string that is the ID of the task.
 * @param test_state_bitmask A string that is the test bitmask for the
 *        scheduling state. The update happens if and only if the current
 *        scheduling state AND-ed with the bitmask is greater than 0.
 * @param state A string that is the scheduling state (a scheduling_state enum
 *        instance) to update the task entry with.
 * @param ray_client_id A string that is the ray client ID of the associated
 *        local scheduler, if any, to update the task entry with.
 * @param test_local_scheduler_id A string to test the local scheduler ID. If
 *        provided, and if the current local scheduler ID does not match it,
 *        then the update does not happen.
 * @return Returns the task entry as a TaskReply. The reply will reflect the
 *         update, if it happened.
 */
int TaskTableTestAndUpdate_RedisCommand(RedisModuleCtx *ctx,
                                        RedisModuleString **argv,
                                        int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc < 5 || argc > 6) {
    return RedisModule_WrongArity(ctx);
  }
  /* If a sixth argument was provided, then we should also test the current
   * local scheduler ID. */
  bool test_local_scheduler = (argc == 6);

  RedisModuleString *task_id = argv[1];
  RedisModuleString *test_state = argv[2];
  RedisModuleString *update_state = argv[3];
  RedisModuleString *local_scheduler_id = argv[4];

  RedisModuleKey *key = OpenPrefixedKey(ctx, TASK_PREFIX, task_id,
                                        REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithNull(ctx);
  }

  /* If the key exists, look up the fields and return them in an array. */
  RedisModuleString *current_state = NULL;
  RedisModuleString *current_local_scheduler_id = NULL;
  RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "state", &current_state,
                      "local_scheduler_id", &current_local_scheduler_id, NULL);

  long long current_state_integer;
  if (RedisModule_StringToLongLong(current_state, &current_state_integer) !=
      REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "current_state must be integer");
  }

  if (current_state_integer < 0) {
    return RedisModule_ReplyWithError(ctx, "Found invalid scheduling state.");
  }
  long long test_state_bitmask;
  int status = RedisModule_StringToLongLong(test_state, &test_state_bitmask);
  if (status != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(
        ctx, "Invalid test value for scheduling state");
  }

  bool update = false;
  if (current_state_integer & test_state_bitmask) {
    if (test_local_scheduler) {
      /* A test local scheduler ID was provided. Test whether it is equal to
       * the current local scheduler ID before performing the update. */
      RedisModuleString *test_local_scheduler_id = argv[5];
      if (RedisModule_StringCompare(current_local_scheduler_id,
                                    test_local_scheduler_id) == 0) {
        /* If the current local scheduler ID does matches the test ID, then
         * perform the update. */
        update = true;
      }
    } else {
      /* No test local scheduler ID was provided. Perform the update. */
      update = true;
    }
  }

  /* If the scheduling state and local scheduler ID tests passed, then perform
   * the update. */
  if (update) {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", update_state,
                        "local_scheduler_id", local_scheduler_id, NULL);
  }

  /* Construct a reply by getting the task from the task ID. */
  return ReplyWithTask(ctx, task_id, update);
}

/**
 * Get an entry from the task table.
 *
 * This is called from a client with the command:
 *
 *     RAY.TASK_TABLE_GET <task ID>
 *
 * @param task_id A string of the task ID to look up.
 * @return An array of strings representing the task fields in the following
 *         order: 1) (integer) scheduling state 2) (string) associated local
 *         scheduler ID, if any 3) (string) the task specification, which can be
 *         cast to a task_spec. If the task ID is not in the table, returns nil.
 */
int TaskTableGet_RedisCommand(RedisModuleCtx *ctx,
                              RedisModuleString **argv,
                              int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  /* Construct a reply by getting the task from the task ID. */
  return ReplyWithTask(ctx, argv[1], false);
}

extern "C" {

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);

  if (RedisModule_Init(ctx, "ray", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.connect", Connect_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.disconnect", Disconnect_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_add", TableAdd_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_append",
                                TableAppend_RedisCommand, "write", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_lookup",
                                TableLookup_RedisCommand, "readonly", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_request_notifications",
                                TableRequestNotifications_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_cancel_notifications",
                                TableCancelNotifications_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.table_test_and_update",
                                TableTestAndUpdate_RedisCommand, "write", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_lookup",
                                ObjectTableLookup_RedisCommand, "readonly", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_add",
                                ObjectTableAdd_RedisCommand, "write pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_remove",
                                ObjectTableRemove_RedisCommand, "write", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_request_notifications",
                                ObjectTableRequestNotifications_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_info_subscribe",
                                ObjectInfoSubscribe_RedisCommand, "pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.result_table_add",
                                ResultTableAdd_RedisCommand, "write", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.result_table_lookup",
                                ResultTableLookup_RedisCommand, "readonly", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_add",
                                TaskTableAddTask_RedisCommand, "write pubsub",
                                0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_update",
                                TaskTableUpdate_RedisCommand, "write pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_test_and_update",
                                TaskTableTestAndUpdate_RedisCommand,
                                "write pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_get",
                                TaskTableGet_RedisCommand, "readonly", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

#if RAY_USE_NEW_GCS
  // Chain-enabled commands that depend on ray-project/credis.
  if (RedisModule_CreateCommand(ctx, "ray.chain.table_add",
                                ChainTableAdd_RedisCommand, "write pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
#endif

  return REDISMODULE_OK;
}

} /* extern "C" */
