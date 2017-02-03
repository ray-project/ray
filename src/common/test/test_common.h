#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <unistd.h>

#include "io.h"
#include "hiredis/hiredis.h"
#include "utstring.h"

#include "task.h"

#ifndef _WIN32
/* This function is actually not declared in standard POSIX, so declare it. */
extern int usleep(useconds_t usec);
#endif

const int64_t arg_value_size = 1000;

static inline task_spec *example_task_spec_with_args(int64_t num_args,
                                                     int64_t num_returns,
                                                     object_id arg_ids[]) {
  task_id parent_task_id = globally_unique_id();
  function_id func_id = globally_unique_id();
  task_spec *task =
      start_construct_task_spec(NIL_ID, parent_task_id, 0, NIL_ID, 0, func_id,
                                num_args, num_returns, arg_value_size);
  for (int64_t i = 0; i < num_args; ++i) {
    object_id arg_id;
    if (arg_ids == NULL) {
      arg_id = globally_unique_id();
    } else {
      arg_id = arg_ids[i];
    }
    task_args_add_ref(task, arg_id);
  }
  finish_construct_task_spec(task);
  return task;
}

static inline task_spec *example_task_spec(int64_t num_args,
                                           int64_t num_returns) {
  return example_task_spec_with_args(num_args, num_returns, NULL);
}

static inline task *example_task_with_args(int64_t num_args,
                                           int64_t num_returns,
                                           int task_state,
                                           object_id arg_ids[]) {
  task_spec *spec = example_task_spec_with_args(num_args, num_returns, arg_ids);
  task *instance = alloc_task(spec, task_state, NIL_ID);
  free_task_spec(spec);
  return instance;
}

static inline task *example_task(int64_t num_args,
                                 int64_t num_returns,
                                 int task_state) {
  task_spec *spec = example_task_spec(num_args, num_returns);
  task *instance = alloc_task(spec, task_state, NIL_ID);
  free_task_spec(spec);
  return instance;
}

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
