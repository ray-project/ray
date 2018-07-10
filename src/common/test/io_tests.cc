#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <inttypes.h>

#include <sstream>
#include <string>

#include "io.h"

SUITE(io_tests);

TEST ipc_socket_test(void) {
#ifndef _WIN32
  const char *socket_pathname = "test-socket";
  int socket_fd = bind_ipc_sock(socket_pathname, true);
  ASSERT(socket_fd >= 0);

  const char *test_string = "hello world";
  const char *test_bytes = "another string";
  pid_t pid = fork();
  if (pid == 0) {
    close(socket_fd);
    socket_fd = connect_ipc_sock(socket_pathname);
    ASSERT(socket_fd >= 0);
    write_log_message(socket_fd, test_string);
    write_message(socket_fd,
                  static_cast<int64_t>(CommonMessageType::LOG_MESSAGE),
                  strlen(test_bytes), (uint8_t *) test_bytes);
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
    ASSERT(static_cast<CommonMessageType>(type) ==
           CommonMessageType::LOG_MESSAGE);
    ASSERT(memcmp(test_bytes, bytes, len) == 0);
    free(bytes);
    close(client_fd);
    close(socket_fd);
    unlink(socket_pathname);
  }
#endif
  PASS();
}

TEST long_ipc_socket_test(void) {
#ifndef _WIN32
  const char *socket_pathname = "long-test-socket";
  int socket_fd = bind_ipc_sock(socket_pathname, true);
  ASSERT(socket_fd >= 0);

  std::stringstream test_string_ss;
  for (int i = 0; i < 10000; i++) {
    test_string_ss << "hello world ";
  }
  std::string test_string = test_string_ss.str();
  const char *test_bytes = "another string";
  pid_t pid = fork();
  if (pid == 0) {
    close(socket_fd);
    socket_fd = connect_ipc_sock(socket_pathname);
    ASSERT(socket_fd >= 0);
    write_log_message(socket_fd, test_string.c_str());
    write_message(socket_fd,
                  static_cast<int64_t>(CommonMessageType::LOG_MESSAGE),
                  strlen(test_bytes), (uint8_t *) test_bytes);
    close(socket_fd);
    exit(0);
  } else {
    int client_fd = accept_client(socket_fd);
    ASSERT(client_fd >= 0);
    char *message = read_log_message(client_fd);
    ASSERT(message != NULL);
    ASSERT_STR_EQ(test_string.c_str(), message);
    free(message);
    int64_t type;
    int64_t len;
    uint8_t *bytes;
    read_message(client_fd, &type, &len, &bytes);
    ASSERT(static_cast<CommonMessageType>(type) ==
           CommonMessageType::LOG_MESSAGE);
    ASSERT(memcmp(test_bytes, bytes, len) == 0);
    free(bytes);
    close(client_fd);
    close(socket_fd);
    unlink(socket_pathname);
  }

#endif
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
