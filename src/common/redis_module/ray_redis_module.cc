#include "redismodule.h"
#include <stdbool.h>
#include <string.h>

#include "redis_string.h"

#include "format/common_generated.h"
#include "task.h"

#include "common_protocol.h"

/**
 * Various tables are maintained in redis:
 *
 * == OBJECT TABLE ==
 *
 * This consists of two parts:
 * - The object location table, indexed by OL:object_id, which is the set of
 *   plasma manager indices that have access to the object.
 * - The object info table, indexed by OI:object_id, which is a hashmap with key
 *   "hash" for the hash of the object and key "data_size" for the size of the
 *   object in bytes.
 *
 * == TASK TABLE ==
 *
 * TODO(pcm): Fill this out.
 */

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

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                const char *prefix,
                                RedisModuleString *keyname,
                                int mode) {
  RedisModuleString *prefixed_keyname =
      RedisString_Format(ctx, "%s%S", prefix, keyname);
  RedisModuleKey *key =
      (RedisModuleKey *) RedisModule_OpenKey(ctx, prefixed_keyname, mode);
  RedisModule_FreeString(ctx, prefixed_keyname);
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
 *  <ray_client_id>:<client type> <aux_address> <is_insertion>
 * If no auxiliary address is provided, aux_address will be set to ":". If
 * is_insertion is true, then the last field will be "1", else "0".
 *
 * @param ctx The Redis context.
 * @param ray_client_id The ID of the database client that was inserted or
 *        deleted.
 * @param client_type The type of client that was inserted or deleted.
 * @param aux_address An optional secondary address associated with the
 *        database client.
 * @param is_insertion A boolean that's true if the update was an insertion and
 *        false if deletion.
 * @return True if the publish was successful and false otherwise.
 */
bool PublishDBClientNotification(RedisModuleCtx *ctx,
                                 RedisModuleString *ray_client_id,
                                 RedisModuleString *client_type,
                                 RedisModuleString *aux_address,
                                 bool is_insertion) {
  /* Construct strings to publish on the db client channel. */
  RedisModuleString *channel_name =
      RedisModule_CreateString(ctx, "db_clients", strlen("db_clients"));
  /* Construct the flatbuffers object to publish over the channel. */
  flatbuffers::FlatBufferBuilder fbb;
  /* Use an empty aux address if one is not passed in. */
  flatbuffers::Offset<flatbuffers::String> aux_address_str;
  if (aux_address != NULL) {
    aux_address_str = RedisStringToFlatbuf(fbb, aux_address);
  } else {
    aux_address_str = fbb.CreateString("", strlen(""));
  }
  /* Create the flatbuffers message. */
  auto message = CreateSubscribeToDBClientTableReply(
      fbb, RedisStringToFlatbuf(fbb, ray_client_id),
      RedisStringToFlatbuf(fbb, client_type), aux_address_str, is_insertion);
  fbb.Finish(message);
  /* Create a Redis string to publish by serializing the flatbuffers object. */
  RedisModuleString *client_info = RedisModule_CreateString(
      ctx, (const char *) fbb.GetBufferPointer(), fbb.GetSize());

  /* Publish the client info on the db client channel. */
  RedisModuleCallReply *reply;
  reply = RedisModule_Call(ctx, "PUBLISH", "ss", channel_name, client_info);
  RedisModule_FreeString(ctx, channel_name);
  RedisModule_FreeString(ctx, client_info);
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
 *     address: This is provided by plasma managers and it should be an address
 *         like "127.0.0.1:1234". It is returned by RAY.GET_CLIENT_ADDRESS so
 *         that other plasma managers know how to fetch objects.
 *     aux_address: This is provided by local schedulers and should be the
 *         address of the plasma manager that the local scheduler is connected
 *         to. This is published to the "db_clients" channel by the RAY.CONNECT
 *         command and is used by the global scheduler to determine which plasma
 *         managers and local schedulers are connected.
 *
 * @param ray_client_id The db client ID of the client.
 * @param node_ip_address The IP address of the node the client is on.
 * @param client_type The type of the client (e.g., plasma_manager).
 * @return OK if the operation was successful.
 */
int Connect_RedisCommand(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc) {
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
  RedisModuleString *aux_address = NULL;
  RedisModuleString *aux_address_key =
      RedisModule_CreateString(ctx, "aux_address", strlen("aux_address"));
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
    if (RedisModule_StringCompare(key, aux_address_key) == 0) {
      aux_address = value;
    }
  }
  /* Clean up. */
  RedisModule_FreeString(ctx, deleted);
  RedisModule_FreeString(ctx, aux_address_key);
  RedisModule_CloseKey(db_client_table_key);
  if (!PublishDBClientNotification(ctx, ray_client_id, client_type, aux_address,
                                   true)) {
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
  RedisModule_FreeString(ctx, deleted_string);
  if (parsed != REDISMODULE_OK) {
    RedisModule_CloseKey(db_client_table_key);
    return RedisModule_ReplyWithError(ctx, "Unable to parse deleted field");
  }

  bool published = true;
  if (deleted == 0) {
    /* Remove the client from the client table. */
    RedisModuleString *deleted =
        RedisModule_CreateString(ctx, "1", strlen("1"));
    RedisModule_HashSet(db_client_table_key, REDISMODULE_HASH_CFIELDS,
                        "deleted", deleted, NULL);
    RedisModule_FreeString(ctx, deleted);

    RedisModuleString *client_type;
    RedisModuleString *aux_address;
    RedisModule_HashGet(db_client_table_key, REDISMODULE_HASH_CFIELDS,
                        "client_type", &client_type, "aux_address",
                        &aux_address, NULL);

    /* Publish the deletion notification on the db client channel. */
    published = PublishDBClientNotification(ctx, ray_client_id, client_type,
                                            aux_address, false);
    if (aux_address != NULL) {
      RedisModule_FreeString(ctx, aux_address);
    }
    RedisModule_FreeString(ctx, client_type);
  }

  RedisModule_CloseKey(db_client_table_key);

  if (!published) {
    /* Return an error message if we weren't able to publish the deletion
     * notification. */
    return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

/**
 * Get the address of a client from its db client ID. This is called from a
 * client with the command:
 *
 *     RAY.GET_CLIENT_ADDRESS <ray client id>
 *
 * @param ray_client_id The db client ID of the client.
 * @return The address of the client if the operation was successful.
 */
int GetClientAddress_RedisCommand(RedisModuleCtx *ctx,
                                  RedisModuleString **argv,
                                  int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *ray_client_id = argv[1];
  /* Get the request client address from the db client table. */
  RedisModuleKey *db_client_table_key =
      OpenPrefixedKey(ctx, DB_CLIENT_PREFIX, ray_client_id, REDISMODULE_READ);
  if (db_client_table_key == NULL) {
    /* There is no client with this ID. */
    RedisModule_CloseKey(db_client_table_key);
    return RedisModule_ReplyWithError(ctx, "invalid client ID");
  }
  RedisModuleString *address;
  RedisModule_HashGet(db_client_table_key, REDISMODULE_HASH_CFIELDS, "address",
                      &address, NULL);
  if (address == NULL) {
    /* The key did not exist. This should not happen. */
    RedisModule_CloseKey(db_client_table_key);
    return RedisModule_ReplyWithError(
        ctx, "Client does not have an address field. This shouldn't happen.");
  }

  RedisModule_ReplyWithString(ctx, address);

  /* Cleanup. */
  RedisModule_CloseKey(db_client_table_key);
  RedisModule_FreeString(ctx, address);

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

  /* Clean up. */
  RedisModule_CloseKey(key);

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
  RedisModule_FreeString(ctx, channel_name);
  RedisModule_FreeString(ctx, payload);
  if (reply == NULL) {
    return false;
  }
  return true;
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
      RedisModule_FreeString(ctx, existing_hash);
    }
  }

  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "hash", new_hash, NULL);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "data_size", data_size,
                      NULL);
  RedisModule_CloseKey(key);

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
  RedisModule_FreeString(ctx, bcast_client_str);

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
        RedisModule_CloseKey(object_notification_key);
        return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
      }
    } while (RedisModule_ZsetRangeNext(object_notification_key));
    /* Now that the clients have been notified, remove the zset of clients
     * waiting for notifications. */
    CHECK_ERROR(RedisModule_DeleteKey(object_notification_key),
                "Unable to delete zset key.");
    RedisModule_CloseKey(object_notification_key);
  }

  RedisModule_CloseKey(table_key);
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
    RedisModule_CloseKey(table_key);
    return RedisModule_ReplyWithError(ctx, "object not found");
  }

  RedisModule_ZsetRem(table_key, manager, NULL);
  RedisModule_CloseKey(table_key);

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
      RedisModule_CloseKey(object_notification_key);
    } else {
      /* Publish a notification to the client's object notification channel. */
      /* Extract the data_size first. */
      RedisModuleKey *object_info_key;
      object_info_key =
          OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_READ);
      if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(object_info_key);
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, "requested object not found");
      }
      RedisModuleString *existing_data_size;
      RedisModule_HashGet(object_info_key, REDISMODULE_HASH_CFIELDS,
                          "data_size", &existing_data_size, NULL);
      RedisModule_CloseKey(object_info_key); /* No longer needed. */
      if (existing_data_size == NULL) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,
                                          "no data_size field in object info");
      }

      bool success = PublishObjectNotification(ctx, client_id, object_id,
                                               existing_data_size, key);
      RedisModule_FreeString(ctx, existing_data_size);
      if (!success) {
        /* The publish failed somehow. */
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
      }
    }
    /* Clean up. */
    RedisModule_CloseKey(key);
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

int ObjectInfoSubscribe_RedisCommand(RedisModuleCtx *ctx,
                                     RedisModuleString **argv,
                                     int argc) {
  REDISMODULE_NOT_USED(ctx);
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

  /* Clean up. */
  RedisModule_CloseKey(key);
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
    RedisModuleString *task_spec = NULL;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "state", &state,
                        "local_scheduler_id", &local_scheduler_id, "TaskSpec",
                        &task_spec, NULL);
    if (state == NULL || local_scheduler_id == NULL || task_spec == NULL) {
      /* We must have either all fields or no fields. */
      RedisModule_CloseKey(key);
      return RedisModule_ReplyWithError(
          ctx, "Missing fields in the task table entry");
    }

    long long state_integer;
    if (RedisModule_StringToLongLong(state, &state_integer) != REDISMODULE_OK ||
        state_integer < 0) {
      RedisModule_CloseKey(key);
      RedisModule_FreeString(ctx, state);
      RedisModule_FreeString(ctx, local_scheduler_id);
      RedisModule_FreeString(ctx, task_spec);
      return RedisModule_ReplyWithError(ctx, "Found invalid scheduling state.");
    }

    flatbuffers::FlatBufferBuilder fbb;
    auto message =
        CreateTaskReply(fbb, RedisStringToFlatbuf(fbb, task_id), state_integer,
                        RedisStringToFlatbuf(fbb, local_scheduler_id),
                        RedisStringToFlatbuf(fbb, task_spec), updated);
    fbb.Finish(message);

    RedisModuleString *reply = RedisModule_CreateString(
        ctx, (char *) fbb.GetBufferPointer(), fbb.GetSize());
    RedisModule_ReplyWithString(ctx, reply);

    RedisModule_FreeString(ctx, state);
    RedisModule_FreeString(ctx, local_scheduler_id);
    RedisModule_FreeString(ctx, task_spec);
  } else {
    /* If the key does not exist, return nil. */
    RedisModule_ReplyWithNull(ctx);
  }

  RedisModule_CloseKey(key);

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
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  /* Get the task ID under field "task" in the object info table. */
  RedisModuleString *object_id = argv[1];

  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_READ);

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithNull(ctx);
  }

  RedisModuleString *task_id;
  RedisModuleString *is_put;
  RedisModuleString *data_size;
  RedisModuleString *hash;
  RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "task", &task_id, "is_put",
                      &is_put, "data_size", &data_size, "hash", &hash, NULL);
  RedisModule_CloseKey(key);

  if (task_id == NULL || is_put == NULL) {
    return RedisModule_ReplyWithNull(ctx);
  }

  /* Check to make sure the is_put field was a 0 or a 1. */
  long long is_put_integer;
  if (RedisModule_StringToLongLong(is_put, &is_put_integer) != REDISMODULE_OK ||
      (is_put_integer != 0 && is_put_integer != 1)) {
    RedisModule_FreeString(ctx, is_put);
    RedisModule_FreeString(ctx, task_id);
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
    CHECK(RedisModule_StringToLongLong(data_size, &data_size_value) ==
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

  /* Clean up. */
  RedisModule_FreeString(ctx, reply);
  RedisModule_FreeString(ctx, is_put);
  RedisModule_FreeString(ctx, task_id);

  if (data_size != NULL) {
    RedisModule_FreeString(ctx, data_size);
  }

  if (hash != NULL) {
    RedisModule_FreeString(ctx, hash);
  }

  return REDISMODULE_OK;
}

int TaskTableWrite(RedisModuleCtx *ctx,
                   RedisModuleString *task_id,
                   RedisModuleString *state,
                   RedisModuleString *local_scheduler_id,
                   RedisModuleString *task_spec) {
  /* Extract the scheduling state. */
  long long state_value;
  if (RedisModule_StringToLongLong(state, &state_value) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "scheduling state must be integer");
  }
  /* Add the task to the task table. If no spec was provided, get the existing
   * spec out of the task table so we can publish it. */
  RedisModuleString *existing_task_spec = NULL;
  RedisModuleKey *key =
      OpenPrefixedKey(ctx, TASK_PREFIX, task_id, REDISMODULE_WRITE);
  if (task_spec == NULL) {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state,
                        "local_scheduler_id", local_scheduler_id, NULL);
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "TaskSpec",
                        &existing_task_spec, NULL);
    if (existing_task_spec == NULL) {
      RedisModule_CloseKey(key);
      return RedisModule_ReplyWithError(
          ctx, "Cannot update a task that doesn't exist yet");
    }
  } else {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state,
                        "local_scheduler_id", local_scheduler_id, "TaskSpec",
                        task_spec, NULL);
  }
  RedisModule_CloseKey(key);

  if (state_value == TASK_STATUS_WAITING ||
      state_value == TASK_STATUS_SCHEDULED) {
    /* Build the PUBLISH topic and message for task table subscribers. The topic
     * is a string in the format "TASK_PREFIX:<local scheduler ID>:<state>". The
     * message is a serialized SubscribeToTasksReply flatbuffer object. */
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
    auto message =
        CreateTaskReply(fbb, RedisStringToFlatbuf(fbb, task_id), state_value,
                        RedisStringToFlatbuf(fbb, local_scheduler_id),
                        RedisStringToFlatbuf(fbb, task_spec_to_use));
    fbb.Finish(message);

    RedisModuleString *publish_message = RedisModule_CreateString(
        ctx, (const char *) fbb.GetBufferPointer(), fbb.GetSize());

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "PUBLISH", "ss", publish_topic, publish_message);

    /* See how many clients received this publish. */
    long long num_clients = RedisModule_CallReplyInteger(reply);
    CHECKM(num_clients <= 1, "Published to %lld clients.", num_clients);

    RedisModule_FreeString(ctx, publish_message);
    RedisModule_FreeString(ctx, publish_topic);
    if (existing_task_spec != NULL) {
      RedisModule_FreeString(ctx, existing_task_spec);
    }

    if (reply == NULL) {
      return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
    }

    if (num_clients == 0) {
      LOG_WARN(
          "No subscribers received this publish. This most likely means that "
          "either the intended recipient has not subscribed yet or that the "
          "pubsub connection to the intended recipient has been broken.");
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
 *     RAY.TASK_TABLE_ADD <task ID> <state> <local scheduler ID> <task spec>
 *
 * @param task_id A string that is the ID of the task.
 * @param state A string that is the current scheduling state (a
 *        scheduling_state enum instance).
 * @param local_scheduler_id A string that is the ray client ID of the
 *        associated local scheduler, if any.
 * @param task_spec A string that is the specification of the task, which can
 *        be cast to a `task_spec`.
 * @return OK if the operation was successful.
 */
int TaskTableAddTask_RedisCommand(RedisModuleCtx *ctx,
                                  RedisModuleString **argv,
                                  int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }

  return TaskTableWrite(ctx, argv[1], argv[2], argv[3], argv[4]);
}

/**
 * Update an entry in the task table. This does not update the task
 * specification in the table.
 *
 * This is called from a client with the command:
 *
 *     RAY.TASK_TABLE_UPDATE <task ID> <state> <local scheduler ID>
 *
 * @param task_id A string that is the ID of the task.
 * @param state A string that is the current scheduling state (a
 *        scheduling_state enum instance).
 * @param ray_client_id A string that is the ray client ID of the associated
 *        local scheduler, if any.
 * @return OK if the operation was successful.
 */
int TaskTableUpdate_RedisCommand(RedisModuleCtx *ctx,
                                 RedisModuleString **argv,
                                 int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }

  return TaskTableWrite(ctx, argv[1], argv[2], argv[3], NULL);
}

/**
 * Test and update an entry in the task table if the current value matches the
 * test value bitmask. This does not update the task specification in the
 * table.
 *
 * This is called from a client with the command:
 *
 *     RAY.TASK_TABLE_TEST_AND_UPDATE <task ID> <test state bitmask> <state>
 *         <local scheduler ID>
 *
 * @param task_id A string that is the ID of the task.
 * @param test_state_bitmask A string that is the test bitmask for the
 *        scheduling state. The update happens if and only if the current
 *        scheduling state AND-ed with the bitmask is greater than 0.
 * @param state A string that is the scheduling state (a scheduling_state enum
 *        instance) to update the task entry with.
 * @param ray_client_id A string that is the ray client ID of the associated
 *        local scheduler, if any, to update the task entry with.
 * @return Returns the task entry as a TaskReply. The reply will reflect the
 *         update, if it happened.
 */
int TaskTableTestAndUpdate_RedisCommand(RedisModuleCtx *ctx,
                                        RedisModuleString **argv,
                                        int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *state = argv[3];

  RedisModuleKey *key = OpenPrefixedKey(ctx, TASK_PREFIX, argv[1],
                                        REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithNull(ctx);
  }

  /* If the key exists, look up the fields and return them in an array. */
  RedisModuleString *current_state = NULL;
  RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "state", &current_state,
                      NULL);
  long long current_state_integer;
  if (RedisModule_StringToLongLong(current_state, &current_state_integer) !=
      REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "current_state must be integer");
  }

  if (current_state_integer < 0) {
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, "Found invalid scheduling state.");
  }
  long long test_state_bitmask;
  int status = RedisModule_StringToLongLong(argv[2], &test_state_bitmask);
  if (status != REDISMODULE_OK) {
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(
        ctx, "Invalid test value for scheduling state");
  }

  bool updated = false;
  if (current_state_integer & test_state_bitmask) {
    /* The test passed, so perform the update. */
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state,
                        "local_scheduler_id", argv[4], NULL);
    updated = true;
  }

  /* Clean up. */
  RedisModule_CloseKey(key);
  /* Construct a reply by getting the task from the task ID. */
  return ReplyWithTask(ctx, argv[1], updated);
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

  if (RedisModule_CreateCommand(ctx, "ray.get_client_address",
                                GetClientAddress_RedisCommand, "write", 0, 0,
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

  return REDISMODULE_OK;
}

} /* extern "C" */
