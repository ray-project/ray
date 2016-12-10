#include "redismodule.h"

#include <string.h>
#include <stdlib.h>
#include "uthash.h"

#define OBJECT_INFO_PREFIX "OI:"
#define OBJECT_TABLE_PREFIX "OT:"
#define SUBSCRIPTION_TABLE_PREFIX "ST:"

/* The object table has the following format:
 * "obj:(object id)" "hash" (hash of the object)
 * "obj:(object id)" "data_size" (size of the object)
 * "obj:(object id):set" (set of managers that have the object)
 */

/* This is called like
 * ray.object_table_add "obj:(object id)" (data_size) "hash" [manager list]
 */
int ObjectTableAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(argc);

  RedisModuleString *object_id = argv[1];
  RedisModuleString *data_size = argv[2];
  RedisModuleString *new_hash = argv[3];
  RedisModuleString *manager = argv[4];

  /*
  size_t object_id_length;
  const char *object_id_value = RedisModule_StringPtrLen(object_id, &object_id_length);
  char *object_key_value = malloc(object_id_length + PREFIX_LENGTH);
  memcpy(object_key_value, OBJECT_INFO_PREFIX, PREFIX_LENGTH);
  memcpy(object_key_value + PREFIX_LENGTH, object_id_value, object_id_length);
  RedisModuleString *object_key = RedisModule_CreateString(ctx, object_key_value, object_id_length + PREFIX_LENGTH);
  */

  const char *object_id_value = RedisModule_StringPtrLen(object_id, NULL);
  RedisModuleString *object_info_key = RedisModule_CreateStringPrintf(ctx, OBJECT_INFO_PREFIX "%s", object_id_value);
  RedisModuleString *object_table_key = RedisModule_CreateStringPrintf(ctx, OBJECT_TABLE_PREFIX "%s", object_id_value);

  /* TODO(pcm): Free the object_info_key and object_table_key. */

  RedisModuleKey *key;
  key = RedisModule_OpenKey(ctx, object_info_key, REDISMODULE_READ | REDISMODULE_WRITE);
  int keytype = RedisModule_KeyType(key);
  /* Check if this object was already registered and if the hashes agree. */
  if (keytype != REDISMODULE_KEYTYPE_EMPTY) {
    RedisModuleString *existing_hash;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "hash", &existing_hash, NULL);
    size_t existing_hash_length;
    const char *existing_hash_value = RedisModule_StringPtrLen(existing_hash, &existing_hash_length);
    size_t new_hash_length;
    const char *new_hash_value = RedisModule_StringPtrLen(new_hash, &new_hash_length);
    if (existing_hash_length != new_hash_length || memcmp(existing_hash_value, new_hash_value, new_hash_length) != 0) {
      RedisModule_CloseKey(key);
      return RedisModule_ReplyWithError(ctx, "object with this id already present with different hash");
    }
  }
  
  // 
  // if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
  //   RedisModule_CloseKey(key);
  //   return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  // }
  // RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], argv[3]);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "hash", new_hash, NULL);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "data_size", data_size, NULL);

  RedisModuleKey *table_key;
  table_key = RedisModule_OpenKey(ctx, object_table_key, REDISMODULE_READ | REDISMODULE_WRITE);
  
  /* Sets are not implemented yet, so we use ZSETs instead. */
  RedisModule_ZsetAdd(table_key, 0.0, manager, NULL);
  RedisModule_CloseKey(key);
  RedisModule_CloseKey(table_key);
  RedisModule_ReplyWithLongLong(ctx, RedisModule_GetSelectedDb(ctx));
  return REDISMODULE_OK;
}

/* This function will get back to the client when the object appears in the
 * object table */
int ObjectTableSubscribe_ReplyFunction(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithSimpleString(ctx,"Hello!");
}

int ObjectTableSubscribe_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModuleString *object_id = argv[1];
  const char *object_id_value = RedisModule_StringPtrLen(object_id, NULL);
  RedisModuleString *object_subscription_key = RedisModule_CreateStringPrintf(ctx, SUBSCRIPTION_TABLE_PREFIX "%s", object_id_value);

  RedisModuleBlockedClient *blocker = RedisModule_BlockClient(ctx, ObjectTableSubscribe_ReplyFunction, NULL, NULL, 1000000000);
  RedisModuleKey *subscription_key;
  subscription_key = RedisModule_OpenKey(ctx, object_subscription_key, REDISMODULE_READ | REDISMODULE_WRITE);
  /* Clean this up. */
  RedisModuleString *blocker_pointer = RedisModule_CreateStringFromLongLong(ctx, (long long) blocker);
  /* Sets are not implemented yet, so we use ZSETs instead. */
  RedisModule_ZsetAdd(subscription_key, 0.0, blocker_pointer, NULL);
  RedisModule_CloseKey(subscription_key);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);

  if (RedisModule_Init(ctx, "ray", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  /* TODO(pcm): What is "readonly" about? */
  if (RedisModule_CreateCommand(ctx, "ray.object_table_add", ObjectTableAdd_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  /* TODO(pcm): What is "readonly" about? */
  if (RedisModule_CreateCommand(ctx, "ray.object_table_subscribe", ObjectTableSubscribe_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
