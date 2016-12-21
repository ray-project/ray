#include "redismodule.h"

#include <stdbool.h>
#include <string.h>

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

#define DB_CLIENT_PREFIX "CL:"
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

RedisModuleString *CreatePrefixedString(RedisModuleCtx *ctx,
                                        const char *prefix,
                                        RedisModuleString *keyname) {
  size_t length;
  const char *value = RedisModule_StringPtrLen(keyname, &length);
  RedisModuleString *prefixed_keyname =
      RedisModule_CreateString(ctx, prefix, strlen(prefix));
  /* Using RedisModule_CreateStringPrintf in the past did not handle NULL
   * characters properly. */
  RedisModule_StringAppendBuffer(ctx, prefixed_keyname, value, length);
  return prefixed_keyname;
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                const char *prefix,
                                RedisModuleString *keyname,
                                int mode) {
  RedisModuleString *prefixed_keyname =
      CreatePrefixedString(ctx, prefix, keyname);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, prefixed_keyname, mode);
  RedisModule_FreeString(ctx, prefixed_keyname);
  return key;
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

  /* This will be used to construct a publish message. */
  RedisModuleString *aux_address = NULL;
  RedisModuleString *aux_address_key =
      RedisModule_CreateString(ctx, "aux_address", strlen("aux_address"));

  RedisModule_HashSet(db_client_table_key, REDISMODULE_HASH_CFIELDS,
                      "ray_client_id", ray_client_id, "node_ip_address",
                      node_ip_address, "client_type", client_type, NULL);

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
  RedisModule_FreeString(ctx, aux_address_key);
  RedisModule_CloseKey(db_client_table_key);

  /* Construct strings to publish on the db client channel. */
  RedisModuleString *channel_name =
      RedisModule_CreateString(ctx, "db_clients", strlen("db_clients"));
  RedisModuleString *client_info =
      RedisModule_CreateStringFromString(ctx, ray_client_id);
  RedisModule_StringAppendBuffer(ctx, client_info, ":", strlen(":"));
  /* Append the client type. */
  size_t client_type_size;
  const char *client_type_str =
      RedisModule_StringPtrLen(client_type, &client_type_size);
  RedisModule_StringAppendBuffer(ctx, client_info, client_type_str,
                                 client_type_size);
  /* Append a space. */
  RedisModule_StringAppendBuffer(ctx, client_info, " ", strlen(" "));
  /* Append the aux address. */
  if (aux_address == NULL) {
    RedisModule_StringAppendBuffer(ctx, client_info, ":", strlen(":"));
  } else {
    size_t aux_address_size;
    const char *aux_address_str =
        RedisModule_StringPtrLen(aux_address, &aux_address_size);
    RedisModule_StringAppendBuffer(ctx, client_info, aux_address_str,
                                   aux_address_size);
  }
  /* Publish the client info on the db client channel. */
  RedisModuleCallReply *reply;
  reply = RedisModule_Call(ctx, "PUBLISH", "ss", channel_name, client_info);
  RedisModule_FreeString(ctx, channel_name);
  RedisModule_FreeString(ctx, client_info);
  if (reply == NULL) {
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
 * @return A list of plasma manager IDs that are listed in the object table as
 *         having the object.
 */
int ObjectTableLookup_RedisCommand(RedisModuleCtx *ctx,
                                   RedisModuleString **argv,
                                   int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleKey *key =
      OpenPrefixedKey(ctx, OBJECT_LOCATION_PREFIX, argv[1], REDISMODULE_READ);

  int keytype = RedisModule_KeyType(key);
  if (keytype == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ValueLength(key) == 0) {
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
  /* Create a string formatted as "<object id> MANAGERS <manager id1>
   * <manager id2> ..." */
  RedisModuleString *manager_list =
      RedisModule_CreateStringFromString(ctx, object_id);
  long long data_size_value;
  if (RedisModule_StringToLongLong(data_size, &data_size_value) !=
      REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "data_size must be integer");
  }

  /* Add a space to the payload for human readability. */
  RedisModule_StringAppendBuffer(ctx, manager_list, " ", strlen(" "));

  /* Append binary data size for this object. */
  RedisModule_StringAppendBuffer(ctx, manager_list,
                                 (const char *) &data_size_value,
                                 sizeof(data_size_value));

  RedisModule_StringAppendBuffer(ctx, manager_list, " MANAGERS",
                                 strlen(" MANAGERS"));

  CHECK_ERROR(
      RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE,
                                        REDISMODULE_POSITIVE_INFINITE, 1, 1),
      "Unable to initialize zset iterator");

  /* Loop over the managers in the object table for this object ID. */
  do {
    RedisModuleString *curr = RedisModule_ZsetRangeCurrentElement(key, NULL);
    RedisModule_StringAppendBuffer(ctx, manager_list, " ", 1);
    size_t size;
    const char *val = RedisModule_StringPtrLen(curr, &size);
    RedisModule_StringAppendBuffer(ctx, manager_list, val, size);
  } while (RedisModule_ZsetRangeNext(key));

  /* Publish the notification to the clients notification channel.
   * TODO(rkn): These notifications could be batched together. */
  RedisModuleString *channel_name =
      CreatePrefixedString(ctx, OBJECT_CHANNEL_PREFIX, client_id);
  RedisModuleCallReply *reply;
  reply = RedisModule_Call(ctx, "PUBLISH", "ss", channel_name, manager_list);
  RedisModule_FreeString(ctx, channel_name);
  RedisModule_FreeString(ctx, manager_list);
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
 * @return OK if the operation was successful and an error with string
 *         "hash mismatch" if the same object_id is already present with a
 *         different hash value.
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

  int keytype = RedisModule_KeyType(key);
  /* Check if this object was already registered and if the hashes agree. */
  if (keytype != REDISMODULE_KEYTYPE_EMPTY) {
    RedisModuleString *existing_hash;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "hash", &existing_hash,
                        NULL);
    if (RedisModule_StringCompare(existing_hash, new_hash) != 0) {
      RedisModule_CloseKey(key);
      return RedisModule_ReplyWithError(ctx, "hash mismatch");
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
  int object_notification_keytype =
      RedisModule_KeyType(object_notification_key);
  if (object_notification_keytype != REDISMODULE_KEYTYPE_EMPTY) {
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
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
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
  int keytype = RedisModule_KeyType(table_key);
  if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
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
 * takes a list of object IDs. There will be an immediate reply acknowledging
 * the call and containing a list of all the object IDs that are already
 * present in the object table along with vectors of the plasma managers that
 * contain each object. For each object ID that is not already present in the
 * object table, there will be a separate subsequent reply that returns the list
 * of manager vectors conaining the object ID, and this will be called as soon
 * as the object is added to the object table.
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
    int keytype = RedisModule_KeyType(key);
    if (keytype == REDISMODULE_KEYTYPE_EMPTY ||
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
      int keytype = RedisModule_KeyType(key);
      if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(object_info_key);
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, "requested object not found");
      }
      RedisModuleString *existing_data_size;
      RedisModule_HashGet(object_info_key, REDISMODULE_HASH_CFIELDS,
                          "data_size", &existing_data_size, NULL);
      RedisModule_CloseKey(object_info_key); /* No longer needed. */

      bool success = PublishObjectNotification(ctx, client_id, object_id,
                                               existing_data_size, key);
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
 *     RAY.RESULT_TABLE_ADD <object id> <task id>
 *
 * @param object_id A string representing the object ID.
 * @param task_id A string representing the task ID of the task that produced
 *        the object.
 * @return OK if the operation was successful.
 */
int ResultTableAdd_RedisCommand(RedisModuleCtx *ctx,
                                RedisModuleString **argv,
                                int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  /* Set the task ID under field "task" in the object info table. */
  RedisModuleString *object_id = argv[1];
  RedisModuleString *task_id = argv[2];

  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_WRITE);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "task", task_id, NULL);

  /* Clean up. */
  RedisModule_CloseKey(key);
  RedisModule_ReplyWithSimpleString(ctx, "OK");

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
 * @return An empty string if the object ID is not in the result table and the
 *         task ID of the task that created the object ID otherwise.
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

  int keytype = RedisModule_KeyType(key);
  if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithStringBuffer(ctx, "", 0);
  }

  RedisModuleString *task_id;
  RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "task", &task_id, NULL);
  if (task_id == NULL) {
    return RedisModule_ReplyWithStringBuffer(ctx, "", 0);
  }
  RedisModule_ReplyWithString(ctx, task_id);

  /* Clean up. */
  RedisModule_FreeString(ctx, task_id);
  RedisModule_CloseKey(key);

  return REDISMODULE_OK;
}

int TaskTableWrite(RedisModuleCtx *ctx,
                   RedisModuleString *task_id,
                   RedisModuleString *state,
                   RedisModuleString *node_id,
                   RedisModuleString *task_spec) {
  /* Pad the state integer to a fixed-width integer, and make sure it has width
   * less than or equal to 2. */
  long long state_integer;
  int status = RedisModule_StringToLongLong(state, &state_integer);
  if (status != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(
        ctx, "Invalid scheduling state (must be an integer)");
  }
  state = RedisModule_CreateStringPrintf(ctx, "%2d", state_integer);
  size_t length;
  RedisModule_StringPtrLen(state, &length);
  if (length != 2) {
    return RedisModule_ReplyWithError(
        ctx, "Invalid scheduling state width (must have width 2)");
  }

  /* Add the task to the task table. If no spec was provided, get the existing
   * spec out of the task table so we can publish it. */
  RedisModuleKey *key =
      OpenPrefixedKey(ctx, TASK_PREFIX, task_id, REDISMODULE_WRITE);
  if (task_spec == NULL) {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state, "node",
                        node_id, NULL);
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "task_spec", &task_spec,
                        NULL);
    if (task_spec == NULL) {
      return RedisModule_ReplyWithError(
          ctx, "Cannot update a task that doesn't exist yet");
    }
  } else {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state, "node",
                        node_id, "task_spec", task_spec, NULL);
  }
  RedisModule_CloseKey(key);

  /* Build the PUBLISH topic and message for task table subscribers. The topic
   * is a string in the format "TASK_PREFIX:<node ID>:<state>". The
   * message is a string in the format: "<task ID> <state> <node ID> <task
   * specification>". */
  RedisModuleString *publish_topic =
      CreatePrefixedString(ctx, TASK_PREFIX, node_id);
  RedisModule_StringAppendBuffer(ctx, publish_topic, ":", strlen(":"));
  const char *state_string = RedisModule_StringPtrLen(state, &length);
  RedisModule_StringAppendBuffer(ctx, publish_topic, state_string, length);
  /* Append the fields to the PUBLISH message. */
  RedisModuleString *publish_message =
      RedisModule_CreateStringFromString(ctx, task_id);
  const char *publish_field;
  /* Append the scheduling state. */
  publish_field = state_string;
  RedisModule_StringAppendBuffer(ctx, publish_message, " ", strlen(" "));
  RedisModule_StringAppendBuffer(ctx, publish_message, publish_field, length);
  /* Append the node ID. */
  publish_field = RedisModule_StringPtrLen(node_id, &length);
  RedisModule_StringAppendBuffer(ctx, publish_message, " ", strlen(" "));
  RedisModule_StringAppendBuffer(ctx, publish_message, publish_field, length);
  /* Append the task specification. */
  publish_field = RedisModule_StringPtrLen(task_spec, &length);
  RedisModule_StringAppendBuffer(ctx, publish_message, " ", strlen(" "));
  RedisModule_StringAppendBuffer(ctx, publish_message, publish_field, length);

  RedisModuleCallReply *reply =
      RedisModule_Call(ctx, "PUBLISH", "ss", publish_topic, publish_message);
  if (reply == NULL) {
    return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
  }

  RedisModule_FreeString(ctx, publish_message);
  RedisModule_FreeString(ctx, publish_topic);
  RedisModule_ReplyWithSimpleString(ctx, "ok");

  return REDISMODULE_OK;
}

/**
 * Add a new entry to the task table. This will overwrite any existing entry
 * with the same task ID.
 *
 * This is called from a client with the command:
 *
 *     RAY.task_table_add <task ID> <state> <node ID> <task spec>
 *
 * @param task_id A string that is the ID of the task.
 * @param state A string that is the current scheduling state (a
 *        scheduling_state enum instance). The string's value must be a
 *        nonnegative integer less than 100, so that it has width at most 2. If
 *        less than 2, the value will be left-padded with spaces to a width of
 *        2.
 * @param node_id A string that is the ID of the associated node, if any.
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
 *     RAY.task_table_update_task <task ID> <state> <node ID>
 *
 * @param task_id A string that is the ID of the task.
 * @param state A string that is the current scheduling state (a
 *        scheduling_state enum instance). The string's value must be a
 *        nonnegative integer less than 100, so that it has width at most 2. If
 *        less than 2, the value will be left-padded with spaces to a width of
 *        2.
 * @param node_id A string that is the ID of the associated node, if any.
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
 * Get an entry from the task table.
 *
 * This is called from a client with the command:
 *
 *     RAY.task_table_get <task ID>
 *
 * @param task_id A string of the task ID to look up.
 * @return An array of strings representing the task fields in the following
 *         order: 1) (integer) scheduling state 2) (string) associated node ID,
 *         if any 3) (string) the task specification, which can be casted to a
 *         task_spec. If the task ID is not in the table, returns nil.
 */
int TaskTableGetTask_RedisCommand(RedisModuleCtx *ctx,
                                  RedisModuleString **argv,
                                  int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleKey *key =
      OpenPrefixedKey(ctx, TASK_PREFIX, argv[1], REDISMODULE_READ);

  int keytype = RedisModule_KeyType(key);
  if (keytype != REDISMODULE_KEYTYPE_EMPTY) {
    /* If the key exists, look up the fields and return them in an array. */
    RedisModuleString *state = NULL, *node = NULL, *task_spec = NULL;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "state", &state, "node",
                        &node, "task_spec", &task_spec, NULL);
    if (state == NULL || node == NULL || task_spec == NULL) {
      /* We must have either all fields or no fields. */
      return RedisModule_ReplyWithError(
          ctx, "Missing fields in the task table entry");
    }

    size_t state_length;
    const char *state_string = RedisModule_StringPtrLen(state, &state_length);
    int state_integer;
    int scanned = sscanf(state_string, "%2d", &state_integer);
    if (scanned != 1 || state_length != 2) {
      return RedisModule_ReplyWithError(ctx,
                                        "Found invalid scheduling state (must "
                                        "be an integer of width 2");
    }

    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithLongLong(ctx, state_integer);
    RedisModule_ReplyWithString(ctx, node);
    RedisModule_ReplyWithString(ctx, task_spec);

    RedisModule_FreeString(ctx, task_spec);
    RedisModule_FreeString(ctx, node);
    RedisModule_FreeString(ctx, state);
  } else {
    /* If the key does not exist, return nil. */
    RedisModule_ReplyWithNull(ctx);
  }

  RedisModule_CloseKey(key);

  return REDISMODULE_OK;
}

int TaskTableSubscribe_RedisCommand(RedisModuleCtx *ctx,
                                    RedisModuleString **argv,
                                    int argc) {
  /* TODO(swang): Implement this. */
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

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
                                "write", 0, 0, 0) == REDISMODULE_ERR) {
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

  if (RedisModule_CreateCommand(ctx, "ray.task_table_get",
                                TaskTableGetTask_RedisCommand, "readonly", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_subscribe",
                                TaskTableSubscribe_RedisCommand, "pubsub", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
