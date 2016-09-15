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

  socket_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
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

  return socket_fd;
}

/* Connects to a Unix domain datagram socket at the given
 * pathname. Returns a file descriptor for the socket, or -1 if
 * an error occurred. */
int connect_ipc_sock(const char *socket_pathname) {
  struct sockaddr_un socket_address;
  int socket_fd;

  socket_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
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
  nbytes = send(socket_fd, (char *) &length, sizeof(length), 0);
  if (nbytes == -1) {
    fprintf(stderr, "Error sending to socket.\n");
    return;
  }
  nbytes = send(socket_fd, (char *) message, length * sizeof(char), 0);
  if (nbytes == -1) {
    fprintf(stderr, "Error sending to socket.\n");
    return;
  }
}

/* Receives a message on the given socket file descriptor. Allocates and
 * returns a pointer to the message.
 * NOTE: Caller must free the message! */
char *recv_ipc_sock(int socket_fd) {
  int length;
  int nbytes;
  nbytes = recv(socket_fd, &length, sizeof(length), 0);
  if (nbytes == -1) {
    fprintf(stderr, "Error receiving from socket.\n");
    return NULL;
  }
  char *message = malloc((length + 1) * sizeof(char));
  nbytes = recv(socket_fd, message, length * sizeof(char), 0);
  if (nbytes == -1) {
    fprintf(stderr, "Error receiving from socket.\n");
    return NULL;
  }
  message[length] = '\0';
  return message;
}
