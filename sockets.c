#include "sockets.h"

#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <stdio.h>

#include "common.h"

/* Binds to a Unix domain datagram socket at the given
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

/* Connects to a Unix domain datagram socket at the given
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

/* Sends a message on the given socket file descriptor. */
void send_ipc_sock(int socket_fd, char *message) {
  int length = strlen(message);
  int nbytes;
  nbytes = write(socket_fd, (char *) &length, sizeof(length));
  if (nbytes == -1) {
    LOG_ERR("Error sending to socket.\n");
    return;
  }
  nbytes = write(socket_fd, (char *) message, length * sizeof(char));
  if (nbytes == -1) {
    LOG_ERR("Error sending to socket.\n");
    return;
  }
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

/* Receives a message on the given socket file descriptor. Allocates and
 * returns a pointer to the message.
 * NOTE: Caller must free the message! */
char *recv_ipc_sock(int socket_fd) {
  int length;
  int nbytes;
  nbytes = read(socket_fd, &length, sizeof(length));
  if (nbytes < 0) {
    LOG_ERR("Error reading length of message from socket.");
    return NULL;
  }

  char *message = malloc((length + 1) * sizeof(char));
  nbytes = read(socket_fd, message, length);
  if (nbytes < 0) {
    LOG_ERR("Error reading message from socket.");
    free(message);
    return NULL;
  }
  message[length] = '\0';
  return message;
}
