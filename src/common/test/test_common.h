#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <unistd.h>

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
static inline void flushall_redis() {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

/* Cleanup method for running tests with the greatest library.
 * Runs the test, then clears the Redis database. */
#define RUN_REDIS_TEST(test) \
  flushall_redis();          \
  RUN_TEST(test);            \
  flushall_redis();

#endif /* TEST_COMMON */
