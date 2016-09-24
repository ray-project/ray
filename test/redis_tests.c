#include "greatest.h"

#include <assert.h>
#include <unistd.h>

#include "event_loop.h"
#include "state/db.h"
#include "state/redis.h"
#include "io.h"
#include "logging.h"

SUITE(redis_tests);

int lookup_successful = 0;
const char *test_set_format = "SET %s %s";
const char *test_get_format = "GET %s";
const char *test_key = "foo";
const char *test_value = "bar";

void async_redis_socket_test_callback(redisAsyncContext *ac,
                                      void *r,
                                      void *privdata) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  redisReply *reply = redisCommand(context, test_get_format, test_key);
  redisFree(context);
  assert(reply != NULL);
  if (strcmp(reply->str, test_value)) {
    freeReplyObject(reply);
    assert(0);
  }
  freeReplyObject(reply);
  lookup_successful = 1;
}

void logging_test_callback(redisAsyncContext *ac, void *r, void *privdata) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  redisReply *reply = redisCommand(context, "KEYS %s", "log:*");
  redisFree(context);
  assert(reply != NULL);
  assert(reply->elements > 0);
  freeReplyObject(reply);
  lookup_successful = 1;
}

TEST redis_socket_test(void) {
  const char *socket_pathname = "redis-test-socket";
  redisContext *context = redisConnect("127.0.0.1", 6379);
  ASSERT(context != NULL);
  int socket_fd = bind_ipc_sock(socket_pathname);
  ASSERT(socket_fd >= 0);

  int client_fd = connect_ipc_sock(socket_pathname);
  ASSERT(client_fd >= 0);
  write_formatted_string(client_fd, test_set_format, test_key, test_value);

  int server_fd = accept_client(socket_fd);
  char *cmd = read_string(server_fd);
  close(client_fd);
  close(server_fd);
  close(socket_fd);
  unlink(socket_pathname);

  redisReply *reply;
  reply = redisCommand(context, cmd, 0, 0);
  freeReplyObject(reply);
  reply = redisCommand(context, "GET %s", test_key);
  ASSERT(reply != NULL);
  ASSERT_STR_EQ(reply->str, test_value);
  freeReplyObject(reply);

  free(cmd);
  redisFree(context);
  PASS();
}

TEST async_redis_socket_test(void) {
  int socket_fd, server_fd, client_fd;
  event_loop loop;
  event_loop_init(&loop);
  /* Start IPC channel. */
  const char *socket_pathname = "async-redis-test-socket";
  socket_fd = bind_ipc_sock(socket_pathname);
  ASSERT(socket_fd >= 0);
  int64_t ipc_index = event_loop_attach(&loop, 1, NULL, socket_fd, POLLIN);

  /* Start connection to Redis. */
  db_conn conn;
  db_connect("127.0.0.1", 6379, "", "", 0, &conn);
  int64_t db_index = db_attach(&conn, &loop, 0);

  /* Send a command to the Redis process. */
  client_fd = connect_ipc_sock(socket_pathname);
  ASSERT(client_fd >= 0);
  write_formatted_string(client_fd, test_set_format, test_key, test_value);

  while (!lookup_successful) {
    int num_ready = event_loop_poll(&loop, -1);
    if (num_ready < 0) {
      exit(-1);
    }
    for (int i = 0; i < event_loop_size(&loop); ++i) {
      struct pollfd *waiting = event_loop_get(&loop, i);
      if (waiting->revents == 0)
        continue;
      if (i == db_index) {
        db_event(&conn);
      } else if (i == ipc_index) {
        /* For some reason, this check is necessary for Travis
         * to pass these tests. */
        ASSERT(waiting->revents & POLLIN);
        server_fd = accept_client(socket_fd);
        ASSERT(server_fd >= 0);
        event_loop_attach(&loop, 1, NULL, server_fd, POLLIN);
      } else {
        char *cmd = read_string(waiting->fd);
        redisAsyncCommand(conn.context, async_redis_socket_test_callback, NULL,
                          cmd, conn.client_id, 0);
        free(cmd);
      }
    }
  }
  db_disconnect(&conn);
  event_loop_free(&loop);
  close(server_fd);
  close(client_fd);
  close(socket_fd);
  unlink(socket_pathname);
  lookup_successful = 0;
  PASS();
}

TEST logging_test(void) {
  int socket_fd, server_fd, client_fd;
  event_loop loop;
  event_loop_init(&loop);
  /* Start IPC channel. */
  const char *socket_pathname = "logging-test-socket";
  socket_fd = bind_ipc_sock(socket_pathname);
  ASSERT(socket_fd >= 0);
  int64_t ipc_index = event_loop_attach(&loop, 1, NULL, socket_fd, POLLIN);

  /* Start connection to Redis. */
  db_conn conn;
  db_connect("127.0.0.1", 6379, "", "", 0, &conn);
  int64_t db_index = db_attach(&conn, &loop, 0);

  /* Send a command to the Redis process. */
  client_fd = connect_ipc_sock(socket_pathname);
  ASSERT(client_fd >= 0);
  ray_logger *logger = init_ray_logger("worker", RAY_INFO, 0, &client_fd);
  ray_log(logger, RAY_INFO, "TEST", "Message");

  while (!lookup_successful) {
    int num_ready = event_loop_poll(&loop, -1);
    if (num_ready < 0) {
      exit(-1);
    }
    for (int i = 0; i < event_loop_size(&loop); ++i) {
      struct pollfd *waiting = event_loop_get(&loop, i);
      if (waiting->revents == 0)
        continue;
      if (i == db_index) {
        db_event(&conn);
      } else if (i == ipc_index) {
        /* For some reason, this check is necessary for Travis
         * to pass these tests. */
        ASSERT(waiting->revents & POLLIN);
        server_fd = accept_client(socket_fd);
        ASSERT(server_fd >= 0);
        event_loop_attach(&loop, 1, NULL, server_fd, POLLIN);
      } else {
        char *cmd = read_string(waiting->fd);
        redisAsyncCommand(conn.context, logging_test_callback, NULL, cmd,
                          conn.client_id, 0);
        free(cmd);
      }
    }
  }
  free_ray_logger(logger);
  db_disconnect(&conn);
  event_loop_free(&loop);
  close(server_fd);
  close(client_fd);
  close(socket_fd);
  unlink(socket_pathname);
  lookup_successful = 0;
  PASS();
}

SUITE(redis_tests) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  RUN_REDIS_TEST(context, redis_socket_test);
  RUN_REDIS_TEST(context, async_redis_socket_test);
  RUN_REDIS_TEST(context, logging_test);
  redisFree(context);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(redis_tests);
  GREATEST_MAIN_END();
}
