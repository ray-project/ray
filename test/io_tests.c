#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <inttypes.h>

#include "io.h"

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
    write_string(socket_fd, test_string);
    write_bytes(socket_fd, (uint8_t *) test_bytes, strlen(test_bytes));
    close(socket_fd);
    exit(0);
  } else {
    int client_fd = accept_client(socket_fd);
    ASSERT(client_fd >= 0);
    char *message = read_string(client_fd);
    ASSERT(message != NULL);
    ASSERT_STR_EQ(test_string, message);
    free(message);
    int64_t len;
    uint8_t *bytes;
    read_bytes(client_fd, &bytes, &len);
    ASSERT(memcmp(test_bytes, bytes, len) == 0);
    free(bytes);
    close(client_fd);
    close(socket_fd);
    unlink(socket_pathname);
  }

  PASS();
}

SUITE(io_tests) {
  RUN_TEST(ipc_socket_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(io_tests);
  GREATEST_MAIN_END();
}
