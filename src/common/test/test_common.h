#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <unistd.h>

#include <cstdio>
#include <string>
#include <vector>

#include "common.h"
#include "io.h"
#include "hiredis/hiredis.h"
#include "state/redis.h"

#ifndef _WIN32
/* This function is actually not declared in standard POSIX, so declare it. */
extern int usleep(useconds_t usec);
#endif

/* I/O helper methods to retry binding to sockets. */
static inline std::string bind_ipc_sock_retry(const char *socket_name_format,
                                              int *fd) {
  std::string socket_name;
  for (int num_retries = 0; num_retries < 5; ++num_retries) {
    RAY_LOG(INFO) << "trying to find plasma socket (attempt " << num_retries
                  << ")";
    size_t size = std::snprintf(nullptr, 0, socket_name_format, rand()) + 1;
    char socket_name_c_str[size];
    std::snprintf(socket_name_c_str, size, socket_name_format, rand());
    socket_name = std::string(socket_name_c_str);

    *fd = bind_ipc_sock(socket_name.c_str(), true);
    if (*fd < 0) {
      /* Sleep for 100ms. */
      usleep(100000);
      continue;
    }
    break;
  }
  return socket_name;
}

static inline int bind_inet_sock_retry(int *fd) {
  int port = -1;
  for (int num_retries = 0; num_retries < 5; ++num_retries) {
    port = 10000 + rand() % 40000;
    *fd = bind_inet_sock(port, true);
    if (*fd < 0) {
      /* Sleep for 100ms. */
      usleep(100000);
      continue;
    }
    break;
  }
  return port;
}

/* Flush redis. */
static inline void flushall_redis(void) {
  /* Flush the primary shard. */
  redisContext *context = redisConnect("127.0.0.1", 6379);
  std::vector<std::string> db_shards_addresses;
  std::vector<int> db_shards_ports;
  get_redis_shards(context, db_shards_addresses, db_shards_ports);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  /* Readd the shard locations. */
  freeReplyObject(redisCommand(context, "SET NumRedisShards %d",
                               db_shards_addresses.size()));
  for (size_t i = 0; i < db_shards_addresses.size(); ++i) {
    freeReplyObject(redisCommand(context, "RPUSH RedisShards %s:%d",
                                 db_shards_addresses[i].c_str(),
                                 db_shards_ports[i]));
  }
  redisFree(context);

  /* Flush the remaining shards. */
  for (size_t i = 0; i < db_shards_addresses.size(); ++i) {
    context = redisConnect(db_shards_addresses[i].c_str(), db_shards_ports[i]);
    freeReplyObject(redisCommand(context, "FLUSHALL"));
    redisFree(context);
  }
}

/* Cleanup method for running tests with the greatest library.
 * Runs the test, then clears the Redis database. */
#define RUN_REDIS_TEST(test) \
  flushall_redis();          \
  RUN_TEST(test);            \
  flushall_redis();

#endif /* TEST_COMMON */
