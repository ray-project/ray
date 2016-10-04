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
  int socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    LOG_ERR("socket() failed for pathname %s.", socket_pathname);
    return -1;
  }
  /* Tell the system to allow the port to be reused. */
  int on = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, (char *) &on,
                 sizeof(on)) < 0) {
    LOG_ERR("setsockopt failed");
    close(socket_fd);
    exit(-1);
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
  int client_fd = accept(socket_fd, NULL, NULL);
  if (client_fd < 0) {
    LOG_ERR("Error reading from socket.");
    return -1;
  }
  return client_fd;
}

/**
 * Reliably write a sequence of bytes into a file descriptor. This will block
 * until one of the following happens: (1) there is an error (2) end of file,
 * or (3) all length bytes have been written.
 *
 * @param fd The file descriptor to write to.
 * @param cursor The cursor pointing to the beginning of the bytes to send.
 * @param length The size of the bytes sequence to write.
 * @return Void.
 */
void write_bytes(int fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  while (length > 0) {
    /* While we haven't written the whole message, write to the file
     * descriptor, advance the cursor, and decrease the amount left to write. */
    nbytes = write(fd, cursor, length);
    CHECK(nbytes > 0);
    cursor += nbytes;
    length -= nbytes;
  }
}

/**
 * Write a sequence of bytes on a file descriptor. The bytes should then be read
 * by read_message.
 *
 * @param fd The file descriptor to write to.
 * @param type The type of the message to send.
 * @param length The size in bytes of the bytes parameter.
 * @param bytes The address of the message to send.
 * @return Void.
 */
void write_message(int fd, int64_t type, int64_t length, uint8_t *bytes) {
  write_bytes(fd, (uint8_t *) &type, sizeof(type));
  write_bytes(fd, (uint8_t *) &length, sizeof(length));
  write_bytes(fd, bytes, length * sizeof(char));
}

/**
 * Reliably read a sequence of bytes from a file descriptor into a buffer. This
 * will block until one of the following happens: (1) there is an error (2) end
 * of file, or (3) all length bytes have been written.
 *
 * @note The buffer pointed to by cursor must already have length number of
 * bytes allocated before calling this method.
 *
 * @param fd The file descriptor to read from.
 * @param cursor The cursor pointing to the beginning of the buffer.
 * @param length The size of the byte sequence to read.
 * @return Void.
 */
int read_bytes(int fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  while (length > 0) {
    /* While we haven't read the whole message, read from the file descriptor,
     * advance the cursor, and decrease the amount left to read. */
    nbytes = read(fd, cursor, length);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      /* Force an exit if there was any other type of error. */
      CHECK(nbytes < 0);
    }
    if (nbytes == 0) {
      return -1;
    }
    cursor += nbytes;
    length -= nbytes;
  }
  return 0;
}

/**
 * Read a sequence of bytes written by write_message from a file descriptor.
 * This allocates space for the message.
 *
 * @note The caller must free the memory.
 *
 * @param fd The file descriptor to read from.
 * @param type The type of the message that is read will be written at this
          address.
 * @param length The size in bytes of the message that is read will be written
          at this address. This size does not include the bytes used to encode
          the type and length.
 * @param bytes The address at which to write the pointer to the bytes that are
          read and allocated by this function.
 * @return Void.
 */
void read_message(int fd, int64_t *type, int64_t *length, uint8_t **bytes) {
  int closed = read_bytes(fd, (uint8_t *) type, sizeof(int64_t));
  if (closed) {
    goto disconnected;
  }
  closed = read_bytes(fd, (uint8_t *) length, sizeof(int64_t));
  if (closed) {
    goto disconnected;
  }
  *bytes = malloc(*length * sizeof(uint8_t));
  closed = read_bytes(fd, *bytes, *length);
  if (closed) {
    free(*bytes);
    goto disconnected;
  }
  return;

disconnected:
  /* Handle the case in which the socket is closed. */
  *type = DISCONNECT_CLIENT;
  *length = 0;
  *bytes = NULL;
  return;
}

/* Write a null-terminated string to a file descriptor. */
void write_log_message(int fd, char *message) {
  /* Account for the \0 at the end of the string. */
  write_message(fd, LOG_MESSAGE, strlen(message) + 1, (uint8_t *) message);
}

/* Reads a null-terminated string from the file descriptor that has been
 * written by write_log_message. Allocates and returns a pointer to the string.
 * NOTE: Caller must free the memory! */
char *read_log_message(int fd) {
  uint8_t *bytes;
  int64_t type;
  int64_t length;
  read_message(fd, &type, &length, &bytes);
  CHECK(type == LOG_MESSAGE);
  return (char *) bytes;
}

void write_formatted_log_message(int socket_fd, const char *format, ...) {
  UT_string *cmd;
  va_list ap;

  utstring_new(cmd);
  va_start(ap, format);
  utstring_printf_va(cmd, format, ap);
  va_end(ap);

  write_log_message(socket_fd, utstring_body(cmd));
  utstring_free(cmd);
}
