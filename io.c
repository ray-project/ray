#include "io.h"

#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdarg.h>
#include <utstring.h>

#include "common.h"

/* Binds to a Unix domain streaming socket at the given
 * pathname. Removes any existing file at the pathname. Returns
 * a file descriptor for the socket, or -1 if an error
 * occurred. */
int bind_ipc_sock(const char *socket_pathname) {
  struct sockaddr_un socket_address;
  int socket_fd;

  socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    LOG_ERR("socket() failed for pathname %s.", socket_pathname);
    return -1;
  }

  unlink(socket_pathname);
  memset(&socket_address, 0, sizeof(struct sockaddr_un));
  socket_address.sun_family = AF_UNIX;
  if (strlen(socket_pathname) + 1 > sizeof(socket_address.sun_path)) {
    LOG_ERR("Socket pathname is too long.");
    return -1;
  }
  strncpy(socket_address.sun_path, socket_pathname,
          strlen(socket_pathname) + 1);

  if (bind(socket_fd, (struct sockaddr *) &socket_address,
           sizeof(struct sockaddr_un)) != 0) {
    LOG_ERR("Bind failed for pathname %s.", socket_pathname);
    return -1;
  }
  listen(socket_fd, 5);

  return socket_fd;
}

/* Connects to a Unix domain streaming socket at the given
 * pathname. Returns a file descriptor for the socket, or -1 if
 * an error occurred. */
int connect_ipc_sock(const char *socket_pathname) {
  struct sockaddr_un socket_address;
  int socket_fd;

  socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    LOG_ERR("socket() failed for pathname %s.", socket_pathname);
    return -1;
  }

  memset(&socket_address, 0, sizeof(struct sockaddr_un));
  socket_address.sun_family = AF_UNIX;
  if (strlen(socket_pathname) + 1 > sizeof(socket_address.sun_path)) {
    LOG_ERR("Socket pathname is too long.");
    return -1;
  }
  strncpy(socket_address.sun_path, socket_pathname,
          strlen(socket_pathname) + 1);

  if (connect(socket_fd, (struct sockaddr *) &socket_address,
              sizeof(struct sockaddr_un)) != 0) {
    LOG_ERR("Connection to socket failed for pathname %s.", socket_pathname);
    return -1;
  }

  return socket_fd;
}

/* Accept a new client connection on the given socket
 * descriptor. Returns a descriptor for the new socket. */
int accept_client(int socket_fd) {
  struct sockaddr_un client_addr;
  int client_fd, client_len;
  client_len = sizeof(client_addr);
  client_fd = accept(socket_fd, (struct sockaddr *) &client_addr,
                     (socklen_t *) &client_len);
  if (client_fd < 0) {
    LOG_ERR("Error reading from socket.");
    return -1;
  }
  return client_fd;
}

/* Write a sequence of bytes on a file descriptor. */
void write_bytes(int fd, uint8_t *bytes, int64_t length) {
  ssize_t nbytes = write(fd, (char *) &length, sizeof(length));
  if (nbytes == -1) {
    LOG_ERR("Error sending to socket.\n");
    return;
  }
  nbytes = write(fd, (char *) bytes, length * sizeof(char));
  if (nbytes == -1) {
    LOG_ERR("Error sending to socket.\n");
    return;
  }
}

/* Read a sequence of bytes written by write_bytes from a file descriptor.
 * Allocates and returns a pointer to the bytes.
 * NOTE: Caller must free the memory! */
void read_bytes(int fd, uint8_t **bytes, int64_t *length) {
  ssize_t nbytes = read(fd, length, sizeof(int64_t));
  if (nbytes < 0) {
    LOG_ERR("Error reading length of message from socket.");
    *bytes = NULL;
    return;
  }

  *bytes = malloc(*length * sizeof(uint8_t));
  nbytes = read(fd, *bytes, *length);
  if (nbytes < 0) {
    LOG_ERR("Error reading message from socket.");
    free(*bytes);
    *bytes = NULL;
  }
}

/* Write a null-terminated string to a file descriptor. */
void write_string(int fd, char *message) {
  /* Account for the \0 at the end of the string. */
  write_bytes(fd, (uint8_t *) message, strlen(message) + 1);
}

/* Reads a null-terminated string from the file descriptor that has been
 * written by write_string. Allocates and returns a pointer to the string.
 * NOTE: Caller must free the memory! */
char *read_string(int fd) {
  uint8_t *bytes;
  int64_t length;
  read_bytes(fd, &bytes, &length);
  return (char *) bytes;
}

void write_formatted_string(int socket_fd, const char *format, ...) {
  UT_string *cmd;
  va_list ap;

  utstring_new(cmd);
  va_start(ap, format);
  utstring_printf_va(cmd, format, ap);
  va_end(ap);

  write_string(socket_fd, utstring_body(cmd));
  utstring_free(cmd);
}
