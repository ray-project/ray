#ifndef IO_H
#define IO_H

#include <stdint.h>

/* Helper functions for socket communication. */

int bind_ipc_sock(const char *socket_pathname);
int connect_ipc_sock(const char *socket_pathname);

int accept_client(int socket_fd);

/* Reading and writing data */

void write_bytes(int fd, uint8_t *bytes, int64_t length);
void read_bytes(int fd, uint8_t **bytes, int64_t *length);

void write_string(int fd, char *message);
char *read_string(int fd);

#endif
