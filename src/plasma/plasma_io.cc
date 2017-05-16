#include "plasma_io.h"
#include "plasma_common.h"

using namespace arrow;

/* Number of times we try binding to a socket. */
#define NUM_BIND_ATTEMPTS 5
#define BIND_TIMEOUT_MS 100

/* Number of times we try connecting to a socket. */
#define NUM_CONNECT_ATTEMPTS 50
#define CONNECT_TIMEOUT_MS 100

Status WriteBytes(int fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    /* While we haven't written the whole message, write to the file descriptor,
     * advance the cursor, and decrease the amount left to write. */
    nbytes = write(fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return Status(StatusCode::IOError, std::string(strerror(errno)));
    } else if (nbytes == 0) {
      return Status(StatusCode::IOError, "Encountered unexpected EOF");
    }
    ARROW_CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }

  return Status::OK();
}

Status WriteMessage(int fd, int64_t type, int64_t length, uint8_t *bytes) {
  int64_t version = RAY_PROTOCOL_VERSION;
  RETURN_NOT_OK(WriteBytes(fd, reinterpret_cast<uint8_t *>(&version), sizeof(version)));
  RETURN_NOT_OK(WriteBytes(fd, reinterpret_cast<uint8_t *>(&type), sizeof(type)));
  RETURN_NOT_OK(WriteBytes(fd, reinterpret_cast<uint8_t *>(&length), sizeof(length)));
  return WriteBytes(fd, bytes, length * sizeof(char));
}

Status ReadBytes(int fd, uint8_t *cursor, size_t length) {
  ssize_t nbytes = 0;
  /* Termination condition: EOF or read 'length' bytes total. */
  size_t bytesleft = length;
  size_t offset = 0;
  while (bytesleft > 0) {
    nbytes = read(fd, cursor + offset, bytesleft);
    if (nbytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;
      }
      return Status(StatusCode::IOError, std::string(strerror(errno)));
    } else if (0 == nbytes) {
      return Status(StatusCode::IOError, "Encountered unexpected EOF");
    }
    ARROW_CHECK(nbytes > 0);
    bytesleft -= nbytes;
    offset += nbytes;
  }

  return Status::OK();
}

Status ReadMessage(int fd, int64_t *type, std::vector<uint8_t> &buffer) {
  int64_t version;
  RETURN_NOT_OK_ELSE(ReadBytes(fd, reinterpret_cast<uint8_t *>(&version), sizeof(version)), *type = DISCONNECT_CLIENT);
  ARROW_CHECK(version == RAY_PROTOCOL_VERSION);
  int64_t length;
  RETURN_NOT_OK_ELSE(ReadBytes(fd, reinterpret_cast<uint8_t *>(type), sizeof(*type)), *type = DISCONNECT_CLIENT);
  RETURN_NOT_OK_ELSE(ReadBytes(fd, reinterpret_cast<uint8_t *>(&length), sizeof(length)), *type = DISCONNECT_CLIENT);
  if (length > buffer.size()) {
    buffer.resize(length);
  }
  RETURN_NOT_OK_ELSE(ReadBytes(fd, buffer.data(), length), *type = DISCONNECT_CLIENT);
  return Status::OK();
}

int bind_ipc_sock(const char *socket_pathname, bool shall_listen) {
  struct sockaddr_un socket_address;
  int socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    ARROW_LOG(ERROR) << "socket() failed for pathname " << socket_pathname;
    return -1;
  }
  /* Tell the system to allow the port to be reused. */
  int on = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, (char *) &on,
                 sizeof(on)) < 0) {
    ARROW_LOG(ERROR) << "setsockopt failed for pathname " << socket_pathname;
    close(socket_fd);
    return -1;
  }

  unlink(socket_pathname);
  memset(&socket_address, 0, sizeof(socket_address));
  socket_address.sun_family = AF_UNIX;
  if (strlen(socket_pathname) + 1 > sizeof(socket_address.sun_path)) {
    ARROW_LOG(ERROR) << "Socket pathname is too long.";
    close(socket_fd);
    return -1;
  }
  strncpy(socket_address.sun_path, socket_pathname,
          strlen(socket_pathname) + 1);

  if (bind(socket_fd, (struct sockaddr *) &socket_address,
           sizeof(socket_address)) != 0) {
    ARROW_LOG(ERROR) << "Bind failed for pathname " << socket_pathname;
    close(socket_fd);
    return -1;
  }
  if (shall_listen && listen(socket_fd, 5) == -1) {
    ARROW_LOG(ERROR) << "Could not listen to socket " << socket_pathname;
    close(socket_fd);
    return -1;
  }
  return socket_fd;
}

int connect_ipc_sock_retry(const char *socket_pathname,
                           int num_retries,
                           int64_t timeout) {
  /* Pick the default values if the user did not specify. */
  if (num_retries < 0) {
    num_retries = NUM_CONNECT_ATTEMPTS;
  }
  if (timeout < 0) {
    timeout = CONNECT_TIMEOUT_MS;
  }

  ARROW_CHECK(socket_pathname);
  int fd = -1;
  for (int num_attempts = 0; num_attempts < num_retries; ++num_attempts) {
    fd = connect_ipc_sock(socket_pathname);
    if (fd >= 0) {
      break;
    }
    if (num_attempts == 0) {
      ARROW_LOG(ERROR) << "Connection to socket failed for pathname " <<
                socket_pathname;
    }
    /* Sleep for timeout milliseconds. */
    usleep(timeout * 1000);
  }
  /* If we could not connect to the socket, exit. */
  if (fd == -1) {
    ARROW_LOG(FATAL) << "Could not connect to socket " << socket_pathname;
  }
  return fd;
}

int connect_ipc_sock(const char *socket_pathname) {
  struct sockaddr_un socket_address;
  int socket_fd;

  socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_fd < 0) {
    ARROW_LOG(ERROR) << "socket() failed for pathname " << socket_pathname;
    return -1;
  }

  memset(&socket_address, 0, sizeof(socket_address));
  socket_address.sun_family = AF_UNIX;
  if (strlen(socket_pathname) + 1 > sizeof(socket_address.sun_path)) {
    ARROW_LOG(ERROR) << "Socket pathname is too long.";
    return -1;
  }
  strncpy(socket_address.sun_path, socket_pathname,
          strlen(socket_pathname) + 1);

  if (connect(socket_fd, (struct sockaddr *) &socket_address,
              sizeof(socket_address)) != 0) {
    close(socket_fd);
    return -1;
  }

  return socket_fd;
}

int accept_client(int socket_fd) {
  int client_fd = accept(socket_fd, NULL, NULL);
  if (client_fd < 0) {
    ARROW_LOG(ERROR) << "Error reading from socket.";
    return -1;
  }
  return client_fd;
}

uint8_t *read_message_async(int sock) {
  int64_t size;
  Status s = ReadBytes(sock, (uint8_t *) &size, sizeof(int64_t));
  if (!s.ok()) {
    /* The other side has closed the socket. */
    ARROW_LOG(DEBUG) << "Socket has been closed, or some other error has occurred.";
    close(sock);
    return NULL;
  }
  uint8_t *message = (uint8_t *) malloc(size);
  s = ReadBytes(sock, message, size);
  if (!s.ok()) {
    /* The other side has closed the socket. */
    ARROW_LOG(DEBUG) << "Socket has been closed, or some other error has occurred.";
    close(sock);
    return NULL;
  }
  return message;
}
