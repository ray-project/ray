#include <inttypes.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <vector>

#include "status.h"

#define RAY_PROTOCOL_VERSION 0x0000000000000000
#define DISCONNECT_CLIENT 0

arrow::Status WriteBytes(int fd, uint8_t *cursor, size_t length);

arrow::Status WriteMessage(int fd, int64_t type, int64_t length, uint8_t *bytes);

arrow::Status ReadBytes(int fd, uint8_t *cursor, size_t length);

arrow::Status ReadMessage(int fd, int64_t *type, std::vector<uint8_t> &buffer);

int bind_ipc_sock(const char *socket_pathname, bool shall_listen);

int connect_ipc_sock(const char *socket_pathname);

int connect_ipc_sock_retry(const char *socket_pathname,
                           int num_retries,
                           int64_t timeout);

int accept_client(int socket_fd);

uint8_t *read_message_async(int sock);
