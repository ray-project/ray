/* Redis implementation of the global state store */

#include <assert.h>

#include "common.h"
#include "db.h"
#include "object_table.h"
#include "event_loop.h"
#include "redis.h"

static void poll_add_read(void *privdata) {
  db_conn *conn = (db_conn *) privdata;
  if (!conn->reading) {
    conn->reading = 1;
    event_loop_get(conn->loop, 0)->events |= POLLIN;
  }
}

static void poll_del_read(void *privdata) {
  db_conn *conn = (db_conn *) privdata;
  if (conn->reading) {
    conn->reading = 0;
    event_loop_get(conn->loop, 0)->events &= ~POLLIN;
  }
}

static void poll_add_write(void *privdata) {
  db_conn *conn = (db_conn *) privdata;
  if (!conn->writing) {
    conn->writing = 1;
    event_loop_get(conn->loop, 0)->events |= POLLOUT;
  }
}

static void poll_del_write(void *privdata) {
  db_conn *conn = (db_conn *) privdata;
  if (conn->writing) {
    conn->writing = 0;
    event_loop_get(conn->loop, 0)->events &= ~POLLOUT;
  }
}

#define LOG_REDIS_ERR(context, M, ...)                                        \
  fprintf(stderr, "[ERROR] (%s:%d: message: %s) " M "\n", __FILE__, __LINE__, \
          context->errstr, ##__VA_ARGS__)

#define CHECK_REDIS_CONNECT(CONTEXT_TYPE, context, M, ...) \
  do {                                                     \
    CONTEXT_TYPE *_context = (context);                    \
    if (!_context) {                                       \
      LOG_ERR("could not allocate redis context");         \
      exit(-1);                                            \
    }                                                      \
    if (_context->err) {                                   \
      LOG_REDIS_ERR(_context, M, ##__VA_ARGS__);           \
      exit(-1);                                            \
    }                                                      \
  } while (0);

void db_connect(const char *address,
                int port,
                const char *client_type,
                const char *client_addr,
                int client_port,
                db_conn *db) {
  /* Sync connection for initial handshake */
  redisReply *reply;
  long long num_clients;
  redisContext *context = redisConnect(address, port);
  CHECK_REDIS_CONNECT(redisContext, context, "could not connect to redis %s:%d",
                      address, port);
  /* Add new client using optimistic locking. */
  while (1) {
    reply = redisCommand(context, "WATCH %s", client_type);
    freeReplyObject(reply);
    reply = redisCommand(context, "HLEN %s", client_type);
    num_clients = reply->integer;
    freeReplyObject(reply);
    reply = redisCommand(context, "MULTI");
    freeReplyObject(reply);
    reply = redisCommand(context, "HSET %s %lld %s:%d", client_type,
                         num_clients, client_addr, client_port);
    freeReplyObject(reply);
    reply = redisCommand(context, "EXEC");
    if (reply) {
      freeReplyObject(reply);
      break;
    }
    freeReplyObject(reply);
  }
  redisFree(context);

  db->client_type = strdup(client_type);
  db->client_id = num_clients;
  db->reading = 0;
  db->writing = 0;

  /* Establish async connection */
  db->context = redisAsyncConnect(address, port);
  CHECK_REDIS_CONNECT(redisAsyncContext, db->context,
                      "could not connect to redis %s:%d", address, port);
  db->context->data = (void *) db;
}

void db_event(db_conn *db) {
  if (db->reading) {
    redisAsyncHandleRead(db->context);
  }
  if (db->writing) {
    redisAsyncHandleWrite(db->context);
  }
}

int64_t db_attach(db_conn *db, event_loop *loop, int connection_type) {
  db->loop = loop;

  redisAsyncContext *ac = db->context;
  redisContext *c = &(ac->c);

  if (ac->ev.data != NULL) {
    return REDIS_ERR;
  }

  ac->ev.addRead = poll_add_read;
  ac->ev.delRead = poll_del_read;
  ac->ev.addWrite = poll_add_write;
  ac->ev.delWrite = poll_del_write;
  // TODO(pcm): Implement cleanup function

  ac->ev.data = db;

  return event_loop_attach(loop, connection_type, NULL, c->fd,
                           POLLIN | POLLOUT);
}

void object_table_add(db_conn *db, unique_id object_id) {
  static char hex_object_id[2 * UNIQUE_ID_SIZE + 1];
  sha1_to_hex(&object_id.id[0], &hex_object_id[0]);
  redisAsyncCommand(db->context, NULL, NULL, "SADD obj:%s %d",
                    &hex_object_id[0], 0);
  if (db->context->err) {
    LOG_REDIS_ERR(db->context, "could not add object_table entry");
  }
}

void object_table_lookup_callback(redisAsyncContext *c,
                                  void *r,
                                  void *privdata) {
  redisReply *reply = r;
  if (reply == NULL)
    return;
  lookup_callback callback = privdata;
  char *str = malloc(reply->len);
  memcpy(str, reply->str, reply->len);
  callback(str);
}

void object_table_fetch_addr_port(redisAsyncContext *c,
                                  void *r,
                                  void *privdata) {
  redisReply *reply = r;
  if (reply == NULL)
    return;
  long long manager_id = -1;
  if (reply->type == REDIS_REPLY_STRING) {
    manager_id = strtoll(reply->str, NULL, 10);
  } else if (reply->type != REDIS_REPLY_INTEGER) {
    manager_id = reply->integer;
  } else {
    LOG_ERR("expected integer or string, received type %d", reply->type);
    exit(-1);
  }
  db_conn *db = c->data;
  redisAsyncCommand(db->context, object_table_lookup_callback, privdata,
                    "HGET %s %lld", db->client_type, manager_id);
}

void object_table_lookup(db_conn *db,
                         unique_id object_id,
                         lookup_callback callback) {
  static char hex_object_id[2 * UNIQUE_ID_SIZE + 1];
  sha1_to_hex(&object_id.id[0], &hex_object_id[0]);
  redisAsyncCommand(db->context, object_table_fetch_addr_port, callback,
                    "SRANDMEMBER obj:%s", &hex_object_id[0]);
  if (db->context->err) {
    LOG_REDIS_ERR(db->context, "error in object_table lookup");
  }
}
