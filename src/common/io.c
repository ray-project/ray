#include "io.h"

#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdarg.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <utstring.h>

#include "common.h"

/**
 * Binds to an Internet socket at the given port. Removes any existing file at
 * the pathname. Returns a non-blocking file descriptor for the socket, or -1
 * if an error occurred.
 *
 * @note Since the returned file descriptor is non-blocking, it is not
 * recommended to use the Linux read and write calls directly, since these
 * might read or write a partial message. Instead, use the provided
 * write_message and read_message methods.
 *
 * @param port The port to bind to.
 * @return A non-blocking file descriptor for the socket, or -1 if an error
 *         occurs.
 */
int bind_inet_sock(const int port) {
  struct sockaddr_in name;
  int socket_fd = socket(PF_INET, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    LOG_ERR("socket() failed for port %d.", port);
    return -1;
  }
  name.sin_family = AF_INET;
  name.sin_port = htons(port);
  name.sin_addr.s_addr = htonl(INADDR_ANY);
  int on = 1;
  /* TODO(pcm): http://stackoverflow.com/q/1150635 */
  if (ioctl(socket_fd, FIONBIO, (char *) &on) < 0) {
    LOG_ERR("ioctl failed");
    close(socket_fd);
    return -1;
  }
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
    LOG_ERR("setsockopt failed for port %d", port);
    close(socket_fd);
    return -1;
  }
  if (bind(socket_fd, (struct sockaddr *) &name, sizeof(name)) < 0) {
    LOG_ERR("Bind failed for port %d", port);
    close(socket_fd);
    return -1;
  }
  if (listen(socket_fd, 5) == -1) {
    LOG_ERR("Could not listen to socket %d", port);
    close(socket_fd);
    return -1;
  }
  return socket_fd;
}

/**
 * Binds to a Unix domain streaming socket at the given
 * pathname. Removes any existing file at the pathname.
 *
 * @param socket_pathname The pathname for the socket.
 * @return A blocking file descriptor for the socket, or -1 if an error
 *         occurs.
 */
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
    LOG_ERR("setsockopt failed for pathname %s", socket_pathname);
    close(socket_fd);
    return -1;
  }

  unlink(socket_pathname);
  memset(&socket_address, 0, sizeof(struct sockaddr_un));
  socket_address.sun_family = AF_UNIX;
  if (strlen(socket_pathname) + 1 > sizeof(socket_address.sun_path)) {
    LOG_ERR("Socket pathname is too long.");
    close(socket_fd);
    return -1;
  }
  strncpy(socket_address.sun_path, socket_pathname,
          strlen(socket_pathname) + 1);

  if (bind(socket_fd, (struct sockaddr *) &socket_address,
           sizeof(struct sockaddr_un)) != 0) {
    LOG_ERR("Bind failed for pathname %s.", socket_pathname);
    close(socket_fd);
    return -1;
  }
  if (listen(socket_fd, 5) == -1) {
    LOG_ERR("Could not listen to socket %s", socket_pathname);
    close(socket_fd);
    return -1;
  }
  return socket_fd;
}

/**
 * Connects to a Unix domain streaming socket at the given
 * pathname. Returns a file descriptor for the socket, or -1 if
 * an error occurred.
 */
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

/**
 * Accept a new client connection on the given socket
 * descriptor. Returns a descriptor for the new socket.
 */
int accept_client(int socket_fd) {
  int client_fd = accept(socket_fd, NULL, NULL);
  if (client_fd < 0) {
    LOG_ERR("Error reading from socket.");
    return -1;
  }
  return client_fd;
}

/**
 * Write a sequence of bytes into a file descriptor. This will block until one
 * of the following happens: (1) there is an error (2) end of file, or (3) all
 * length bytes have been written.
 *
 * @param fd The file descriptor to write to. It can be non-blocking.
 * @param cursor The cursor pointing to the beginning of the bytes to send.
 * @param length The size of the bytes sequence to write.
 * @return int Whether there was an error while writing. 0 corresponds to
 *         success and -1 corresponds to an error (errno will be set).
 */
int write_bytes(int fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  while (length > 0) {
    /* While we haven't written the whole message, write to the file
     * descriptor, advance the cursor, and decrease the amount left to write. */
    nbytes = write(fd, cursor, length);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      /* TODO(swang): Return the error instead of exiting. */
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
 * Write a sequence of bytes on a file descriptor. The bytes should then be read
 * by read_message.
 *
 * @param fd The file descriptor to write to. It can be non-blocking.
 * @param type The type of the message to send.
 * @param length The size in bytes of the bytes parameter.
 * @param bytes The address of the message to send.
 * @return int Whether there was an error while writing. 0 corresponds to
 *         success and -1 corresponds to an error (errno will be set).
 */
int write_message(int fd, int64_t type, int64_t length, uint8_t *bytes) {
  int closed;
  closed = write_bytes(fd, (uint8_t *) &type, sizeof(type));
  if (closed) {
    return closed;
  }
  closed = write_bytes(fd, (uint8_t *) &length, sizeof(length));
  if (closed) {
    return closed;
  }
  closed = write_bytes(fd, bytes, length * sizeof(char));
  if (closed) {
    return closed;
  }
  return 0;
}

/**
 * Read a sequence of bytes from a file descriptor into a buffer. This will
 * block until one of the following happens: (1) there is an error (2) end of
 * file, or (3) all length bytes have been written.
 *
 * @note The buffer pointed to by cursor must already have length number of
 * bytes allocated before calling this method.
 *
 * @param fd The file descriptor to read from. It can be non-blocking.
 * @param cursor The cursor pointing to the beginning of the buffer.
 * @param length The size of the byte sequence to read.
 * @return int Whether there was an error while writing. 0 corresponds to
 *         success and -1 corresponds to an error (errno will be set).
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
 * @param fd The file descriptor to read from. It can be non-blocking.
 * @param type The type of the message that is read will be written at this
          address. If there was an error while reading, this will be
          DISCONNECT_CLIENT.
 * @param length The size in bytes of the message that is read will be written
          at this address. This size does not include the bytes used to encode
          the type and length. If there was an error while reading, this will
          be 0.
 * @param bytes The address at which to write the pointer to the bytes that are
          read and allocated by this function. If there was an error while
          reading, this will be NULL.

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
