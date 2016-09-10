/* PLASMA CLIENT: Client library for using the plasma store and manager */

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <strings.h>
#include <netinet/in.h>
#include <netdb.h>

#include "plasma.h"
#include "fling.h"

void plasma_send(int fd, plasma_request *req) {
  int req_count = sizeof(plasma_request);
  if (write(fd, req, req_count) != req_count) {
    LOG_ERR("write error");
    exit(-1);
  }
}

void plasma_create(int conn, plasma_id object_id, int64_t size, void **data) {
  LOG_INFO("called plasma_create on conn %d with size %" PRId64, conn, size);
  plasma_request req = {
      .type = PLASMA_CREATE, .object_id = object_id, .size = size};
  plasma_send(conn, &req);
  plasma_reply reply;
  int fd = recv_fd(conn, (char *) &reply, sizeof(plasma_reply));
  assert(reply.type == PLASMA_OBJECT);
  assert(reply.object_size == size);
  *data =
      mmap(NULL, reply.map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) +
      reply.offset;
  if (*data == MAP_FAILED) {
    LOG_ERR("mmap failed");
    exit(-1);
  }
  close(fd);
}

void plasma_get(int conn, plasma_id object_id, int64_t *size, void **data) {
  plasma_request req = {.type = PLASMA_GET, .object_id = object_id};
  plasma_send(conn, &req);
  plasma_reply reply;
  /* The following loop is run at most twice. */
  int fd = recv_fd(conn, (char *) &reply, sizeof(plasma_reply));
  if (reply.type == PLASMA_FUTURE) {
    int new_fd = recv_fd(fd, (char *) &reply, sizeof(plasma_reply));
    close(fd);
    fd = new_fd;
  }
  assert(reply.type == PLASMA_OBJECT);
  *data =
      mmap(NULL, reply.map_size, PROT_READ, MAP_SHARED, fd, 0) + reply.offset;
  if (*data == MAP_FAILED) {
    LOG_ERR("mmap failed");
    exit(-1);
  }
  close(fd);
  *size = reply.object_size;
}

void plasma_seal(int fd, plasma_id object_id) {
  plasma_request req = {.type = PLASMA_SEAL, .object_id = object_id};
  plasma_send(fd, &req);
}

int plasma_store_connect(const char *socket_name) {
  assert(socket_name);
  struct sockaddr_un addr;
  int fd;
  if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    LOG_ERR("socket error");
    exit(-1);
  }
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, socket_name, sizeof(addr.sun_path) - 1);
  /* Try to connect to the Plasma store. If unsuccessful, retry several times.
   */
  int connected_successfully = 0;
  for (int num_attempts = 0; num_attempts < 50; ++num_attempts) {
    if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) == 0) {
      connected_successfully = 1;
      break;
    }
    /* Sleep for 100 milliseconds. */
    usleep(100000);
  }
  /* If we could not connect to the Plasma store, exit. */
  if (!connected_successfully) {
    LOG_ERR("could not connect to store %s", socket_name);
    exit(-1);
  }
  return fd;
}

#define h_addr h_addr_list[0]

int plasma_manager_connect(const char *ip_addr, int port) {
  int fd = socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    LOG_ERR("could not create socket");
    exit(-1);
  }

  struct hostent *manager = gethostbyname(ip_addr); /* TODO(pcm): cache this */
  if (!manager) {
    LOG_ERR("plasma manager %s not found", ip_addr);
    exit(-1);
  }

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  bcopy(manager->h_addr, &addr.sin_addr.s_addr, manager->h_length);
  addr.sin_port = htons(port);

  int r = connect(fd, (struct sockaddr *) &addr, sizeof(addr));
  if (r < 0) {
    LOG_ERR(
        "could not establish connection to manager with id %s:%d (probably ran "
        "out of ports)",
        &ip_addr[0], port);
    exit(-1);
  }
  return fd;
}

void plasma_transfer(int manager,
                     const char *addr,
                     int port,
                     plasma_id object_id) {
  plasma_request req = {
      .type = PLASMA_TRANSFER, .object_id = object_id, .port = port};
  char *end = NULL;
  for (int i = 0; i < 4; ++i) {
    req.addr[i] = strtol(end ? end : addr, &end, 10);
    /* skip the '.' */
    end += 1;
  }
  plasma_send(manager, &req);
}
