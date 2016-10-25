#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <inttypes.h>

#include "io.h"
#include "utstring.h"

SUITE(io_tests);

TEST ipc_socket_test(void) {
  const char *socket_pathname = "test-socket";
  int socket_fd = bind_ipc_sock(socket_pathname);
  ASSERT(socket_fd >= 0);

  char *test_string = "hello world";
  char *test_bytes = "another string";
  pid_t pid = fork();
  if (pid == 0) {
    close(socket_fd);
    socket_fd = connect_ipc_sock(socket_pathname);
    ASSERT(socket_fd >= 0);
    write_log_message(socket_fd, test_string);
    write_message(socket_fd, LOG_MESSAGE, strlen(test_bytes),
                  (uint8_t *) test_bytes);
    close(socket_fd);
    exit(0);
  } else {
    int client_fd = accept_client(socket_fd);
    ASSERT(client_fd >= 0);
    char *message = read_log_message(client_fd);
    ASSERT(message != NULL);
    ASSERT_STR_EQ(test_string, message);
    free(message);
    int64_t type;
    int64_t len;
    uint8_t *bytes;
    read_message(client_fd, &type, &len, &bytes);
    ASSERT(type == LOG_MESSAGE);
    ASSERT(memcmp(test_bytes, bytes, len) == 0);
    free(bytes);
    close(client_fd);
    close(socket_fd);
    unlink(socket_pathname);
  }

  PASS();
}

TEST long_ipc_socket_test(void) {
  const char *socket_pathname = "long-test-socket";
  int socket_fd = bind_ipc_sock(socket_pathname);
  ASSERT(socket_fd >= 0);

  UT_string *test_string;
  utstring_new(test_string);
  for (int i = 0; i < 10000; i++) {
    utstring_printf(test_string, "hello world ");
  }
  char *test_bytes = "another string";
  pid_t pid = fork();
  if (pid == 0) {
    close(socket_fd);
    socket_fd = connect_ipc_sock(socket_pathname);
    ASSERT(socket_fd >= 0);
    write_log_message(socket_fd, utstring_body(test_string));
    write_message(socket_fd, LOG_MESSAGE, strlen(test_bytes),
                  (uint8_t *) test_bytes);
    close(socket_fd);
    exit(0);
  } else {
    int client_fd = accept_client(socket_fd);
    ASSERT(client_fd >= 0);
    char *message = read_log_message(client_fd);
    ASSERT(message != NULL);
    ASSERT_STR_EQ(utstring_body(test_string), message);
    free(message);
    int64_t type;
    int64_t len;
    uint8_t *bytes;
    read_message(client_fd, &type, &len, &bytes);
    ASSERT(type == LOG_MESSAGE);
    ASSERT(memcmp(test_bytes, bytes, len) == 0);
    free(bytes);
    close(client_fd);
    close(socket_fd);
    unlink(socket_pathname);
  }

  utstring_free(test_string);
  PASS();
}

SUITE(io_tests) {
  RUN_TEST(ipc_socket_test);
  RUN_TEST(long_ipc_socket_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(io_tests);
  GREATEST_MAIN_END();
}
