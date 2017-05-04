#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <unistd.h>
#include <vector>

#include "common.h"
#include "io.h"
#include "hiredis/hiredis.h"
#include "utstring.h"

#ifndef _WIN32
/* This function is actually not declared in standard POSIX, so declare it. */
extern int usleep(useconds_t usec);
#endif

/* I/O helper methods to retry binding to sockets. */
static inline UT_string *bind_ipc_sock_retry(const char *socket_name_format,
                                             int *fd) {
  UT_string *socket_name = NULL;
  for (int num_retries = 0; num_retries < 5; ++num_retries) {
    LOG_INFO("trying to find plasma socket (attempt %d)", num_retries);
    utstring_renew(socket_name);
    utstring_printf(socket_name, socket_name_format, rand());
    *fd = bind_ipc_sock(utstring_body(socket_name), true);
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
static inline void flushall_redis(std::vector<std::string> db_shards_addresses,
                                  std::vector<int> db_shards_ports) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);

  for (int i = 0; i < db_shards_addresses.size(); ++i) {
    context = redisConnect(db_shards_addresses[i].c_str(), db_shards_ports[i]);
    freeReplyObject(redisCommand(context, "FLUSHALL"));
    redisFree(context);
  }
}

/* Cleanup method for running tests with the greatest library.
 * Runs the test, then clears the Redis database. */
#define RUN_REDIS_TEST(db_shards_addresses, db_shards_ports, test) \
  flushall_redis(db_shards_addresses, db_shards_ports);            \
  RUN_TEST(test);                                                  \
  flushall_redis(db_shards_addresses, db_shards_ports);

#endif /* TEST_COMMON */
