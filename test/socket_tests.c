#include "greatest.h"

#include <assert.h>
#include <unistd.h>

#include "sockets.h"

SUITE(socket_tests);

TEST ipc_socket_test(void) {
  const char* socket_pathname = "test-socket";
  int socket_fd = bind_ipc_sock(socket_pathname);
  ASSERT(socket_fd >= 0);

  char* test_string = "hello world";
  pid_t pid = fork();
  if (pid == 0) {
    close(socket_fd);
    socket_fd = connect_ipc_sock(socket_pathname);
    ASSERT(socket_fd >= 0);
    send_ipc_sock(socket_fd, test_string);
    close(socket_fd);
    exit(0);
  } else {
    int client_fd = accept_client(socket_fd);
    ASSERT(client_fd >= 0);
    char* message = recv_ipc_sock(client_fd);
    ASSERT(message != NULL);
    ASSERT_STR_EQ(test_string, message);
    free(message);
    close(client_fd);
    close(socket_fd);
    unlink(socket_pathname);
  }

  PASS();
}

SUITE(socket_tests) {
  RUN_TEST(ipc_socket_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char** argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(socket_tests);
  GREATEST_MAIN_END();
}
