#ifndef IO_H
#define IO_H

#include <stdbool.h>
#include <stdint.h>

#include "utarray.h"

#define LOG_MESSAGE_VERSION 0

enum common_message_type {
  /** Disconnect a client. */
  DISCONNECT_CLIENT,
  /** Log a message from a client. */
  LOG_MESSAGE,
  /** Submit a task to the local scheduler. */
  SUBMIT_TASK,
};

/* Helper functions for socket communication. */

int bind_inet_sock(const int port, bool shall_listen);
int bind_ipc_sock(const char *socket_pathname, bool shall_listen);
int connect_ipc_sock(const char *socket_pathname);

int accept_client(int socket_fd);

/* Reading and writing data. */

int write_message(int fd, int64_t version, int64_t type, int64_t length, uint8_t *bytes);
void read_message(int fd, int64_t version, int64_t *type, int64_t *length, uint8_t **bytes);
int64_t read_buffer(int fd, int64_t *type, UT_array *buffer);

void write_log_message(int fd, char *message);
void write_formatted_log_message(int fd, const char *format, ...);
char *read_log_message(int fd);
int read_bytes(int fd, uint8_t *cursor, size_t length);
int write_bytes(int fd, uint8_t *cursor, size_t length);

#endif /* IO_H */
