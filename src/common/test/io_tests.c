#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <inttypes.h>

#include "io.h"
#include "utstring.h"

SUITE(io_tests);

#ifdef _WIN32
#ifdef __cplusplus
extern "C" {
#endif
intptr_t __cdecl _spawnve(int Mode,
                          char const *FileName,
                          char const *const *Arguments,
                          char const *const *Environment);
int _getpid(void);
#ifdef __cplusplus
}
#endif

/* Simple, hacky forking that only really works for test processes.
 * Returns 0 if this is the parent process,
 * the PID of the child process if this is the child, or -1 if neither
 * (in which case the caller should immediately return and not do anything).
 */
pid_t fork_test(char const function_name[]) {
  pid_t result;
  char const key[] = "_TEST_FUNCTION";
  char *const oldv = getenv(key);
  if (oldv == NULL || strlen(oldv) == 0) {
    char *kv = calloc(strlen(key) + 1 + (oldv ? strlen(oldv) : 0) +
                          (function_name ? strlen(function_name) : 0) + 1,
                      sizeof(*kv));
    strcpy(kv, key);
    strcat(kv, "=");
    strcat(kv, function_name);
    putenv(kv);
    result = _spawnve(4, __argv[0], __argv, _environ);
    if (result != -1) {
      if (result) {
        CloseHandle((HANDLE) result);
      }
      result = 1;
    }
    strcpy(kv, key);
    strcat(kv, "=");
    if (oldv) {
      strcat(kv, oldv);
    }
    putenv(kv);
    free(kv);
  } else {
    result = strcmp(oldv, function_name) == 0 ? 0 : -1;
  }
  return result;
}

#define fork() fork_test(__FUNCTION__)
#endif

TEST ipc_socket_test(void) {
  const char *socket_pathname = "test-socket";
  int socket_fd = bind_ipc_sock(socket_pathname, true, false);
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
  } else if (pid > 0) {
    int client_fd = accept_client(socket_fd, false);
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
  int socket_fd = bind_ipc_sock(socket_pathname, true, false);
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
  } else if (pid > 0) {
    int client_fd = accept_client(socket_fd, false);
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
