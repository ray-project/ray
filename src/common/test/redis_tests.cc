#include "greatest.h"

#include <assert.h>
#include <unistd.h>

#include "utarray.h"

#include "event_loop.h"
#include "state/db.h"
#include "state/redis.h"
#include "io.h"
#include "logging.h"
#include "test_common.h"

SUITE(redis_tests);

const char *test_set_format = "SET %s %s";
const char *test_get_format = "GET %s";
const char *test_key = "foo";
const char *test_value = "bar";
UT_array *connections = NULL;

int async_redis_socket_test_callback_called = 0;

void async_redis_socket_test_callback(redisAsyncContext *ac,
                                      void *r,
                                      void *privdata) {
  async_redis_socket_test_callback_called = 1;
  redisContext *context = redisConnect("127.0.0.1", 6379);
  redisReply *reply = (redisReply *) redisCommand(context, test_get_format, test_key);
  redisFree(context);
  CHECK(reply != NULL);
  if (strcmp(reply->str, test_value)) {
    freeReplyObject(reply);
    CHECK(0);
  }
  freeReplyObject(reply);
}

TEST redis_socket_test(void) {
  const char *socket_pathname = "redis-test-socket";
  redisContext *context = redisConnect("127.0.0.1", 6379);
  ASSERT(context != NULL);
  int socket_fd = bind_ipc_sock(socket_pathname, true);
  ASSERT(socket_fd >= 0);

  int client_fd = connect_ipc_sock(socket_pathname);
  ASSERT(client_fd >= 0);
  write_formatted_log_message(client_fd, test_set_format, test_key, test_value);

  int server_fd = accept_client(socket_fd);
  char *cmd = read_log_message(server_fd);
  close(client_fd);
  close(server_fd);
  close(socket_fd);
  unlink(socket_pathname);

  redisReply *reply = (redisReply *) redisCommand(context, cmd, 0, 0);
  freeReplyObject(reply);
  reply = (redisReply *) redisCommand(context, "GET %s", test_key);
  ASSERT(reply != NULL);
  ASSERT_STR_EQ(reply->str, test_value);
  freeReplyObject(reply);

  free(cmd);
  redisFree(context);
  PASS();
}

void redis_read_callback(event_loop *loop, int fd, void *context, int events) {
  DBHandle *db = (DBHandle *) context;
  char *cmd = read_log_message(fd);
  redisAsyncCommand(db->context, async_redis_socket_test_callback, NULL, cmd);
  free(cmd);
}

void redis_accept_callback(event_loop *loop,
                           int socket_fd,
                           void *context,
                           int events) {
  int accept_fd = accept_client(socket_fd);
  CHECK(accept_fd >= 0);
  utarray_push_back(connections, &accept_fd);
  event_loop_add_file(loop, accept_fd, EVENT_LOOP_READ, redis_read_callback,
                      context);
}

int timeout_handler(event_loop *loop, timer_id timer_id, void *context) {
  event_loop_stop(loop);
  return EVENT_LOOP_TIMER_DONE;
}

TEST async_redis_socket_test(void) {
  utarray_new(connections, &ut_int_icd);
  event_loop *loop = event_loop_create();

  /* Start IPC channel. */
  const char *socket_pathname = "async-redis-test-socket";
  int socket_fd = bind_ipc_sock(socket_pathname, true);
  ASSERT(socket_fd >= 0);
  utarray_push_back(connections, &socket_fd);

  /* Start connection to Redis. */
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "test_process", "127.0.0.1", 0, NULL);
  db_attach(db, loop, false);

  /* Send a command to the Redis process. */
  int client_fd = connect_ipc_sock(socket_pathname);
  ASSERT(client_fd >= 0);
  utarray_push_back(connections, &client_fd);
  write_formatted_log_message(client_fd, test_set_format, test_key, test_value);

  event_loop_add_file(loop, client_fd, EVENT_LOOP_READ, redis_read_callback,
                      db);
  event_loop_add_file(loop, socket_fd, EVENT_LOOP_READ, redis_accept_callback,
                      db);
  event_loop_add_timer(loop, 100, timeout_handler, NULL);
  event_loop_run(loop);

  ASSERT(async_redis_socket_test_callback_called);

  db_disconnect(db);
  event_loop_destroy(loop);
  for (int *p = (int *) utarray_front(connections); p != NULL;
       p = (int *) utarray_next(connections, p)) {
    close(*p);
  }
  unlink(socket_pathname);
  utarray_free(connections);
  PASS();
}

int logging_test_callback_called = 0;

void logging_test_callback(redisAsyncContext *ac, void *r, void *privdata) {
  logging_test_callback_called = 1;
  redisContext *context = redisConnect("127.0.0.1", 6379);
  redisReply *reply = (redisReply *) redisCommand(context, "KEYS %s", "log:*");
  redisFree(context);
  CHECK(reply != NULL);
  CHECK(reply->elements > 0);
  freeReplyObject(reply);
}

void logging_read_callback(event_loop *loop,
                           int fd,
                           void *context,
                           int events) {
  DBHandle *conn = (DBHandle *) context;
  char *cmd = read_log_message(fd);
  redisAsyncCommand(conn->context, logging_test_callback, NULL, cmd,
                    (char *) conn->client.id, sizeof(conn->client.id));
  free(cmd);
}

void logging_accept_callback(event_loop *loop,
                             int socket_fd,
                             void *context,
                             int events) {
  int accept_fd = accept_client(socket_fd);
  CHECK(accept_fd >= 0);
  utarray_push_back(connections, &accept_fd);
  event_loop_add_file(loop, accept_fd, EVENT_LOOP_READ, logging_read_callback,
                      context);
}

TEST logging_test(void) {
  utarray_new(connections, &ut_int_icd);
  event_loop *loop = event_loop_create();

  /* Start IPC channel. */
  const char *socket_pathname = "logging-test-socket";
  int socket_fd = bind_ipc_sock(socket_pathname, true);
  ASSERT(socket_fd >= 0);
  utarray_push_back(connections, &socket_fd);

  /* Start connection to Redis. */
  DBHandle *conn =
      db_connect("127.0.0.1", 6379, "test_process", "127.0.0.1", 0, NULL);
  db_attach(conn, loop, false);

  /* Send a command to the Redis process. */
  int client_fd = connect_ipc_sock(socket_pathname);
  ASSERT(client_fd >= 0);
  utarray_push_back(connections, &client_fd);
  RayLogger *logger = RayLogger_init("worker", RAY_INFO, 0, &client_fd);
  RayLogger_log(logger, RAY_INFO, "TEST", "Message");

  event_loop_add_file(loop, socket_fd, EVENT_LOOP_READ, logging_accept_callback,
                      conn);
  event_loop_add_file(loop, client_fd, EVENT_LOOP_READ, logging_read_callback,
                      conn);
  event_loop_add_timer(loop, 100, timeout_handler, NULL);
  event_loop_run(loop);

  ASSERT(logging_test_callback_called);

  RayLogger_free(logger);
  db_disconnect(conn);
  event_loop_destroy(loop);
  for (int *p = (int *) utarray_front(connections); p != NULL;
       p = (int *) utarray_next(connections, p)) {
    close(*p);
  }
  unlink(socket_pathname);
  utarray_free(connections);
  PASS();
}

SUITE(redis_tests) {
  RUN_REDIS_TEST(redis_socket_test);
  RUN_REDIS_TEST(async_redis_socket_test);
  RUN_REDIS_TEST(logging_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(redis_tests);
  GREATEST_MAIN_END();
}
