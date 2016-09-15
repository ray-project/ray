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
#include "plasma_client.h"
#include "fling.h"

void plasma_send(int fd, plasma_request *req) {
  int req_count = sizeof(plasma_request);
  if (write(fd, req, req_count) != req_count) {
    LOG_ERR("write error, fd = %d", fd);
    exit(-1);
  }
}

/* If the file descriptor fd has been mmapped in this client process before,
 * return the pointer that was returned by mmap, otherwise mmap it and store the
 * pointer in a hash table. */
uint8_t *lookup_or_mmap(plasma_store_conn *conn,
                        int fd,
                        int store_fd_val,
                        int64_t map_size) {
  client_mmap_table_entry *entry;
  HASH_FIND_INT(conn->mmap_table, &store_fd_val, entry);
  if (entry) {
    close(fd);
    return entry->pointer;
  } else {
    uint8_t *result =
        mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (result == MAP_FAILED) {
      LOG_ERR("mmap failed");
      exit(-1);
    }
    close(fd);
    entry = malloc(sizeof(client_mmap_table_entry));
    entry->key = store_fd_val;
    entry->pointer = result;
    HASH_ADD_INT(conn->mmap_table, key, entry);
    return result;
  }
}

void plasma_create(plasma_store_conn *conn,
                   plasma_id object_id,
                   int64_t data_size,
                   uint8_t *metadata,
                   int64_t metadata_size,
                   uint8_t **data) {
  LOG_INFO(
      "called plasma_create on conn %d with size %d and metadata size "
      "%d" PRId64,
      conn, size, metadata_size);
  plasma_request req = {.type = PLASMA_CREATE,
                        .object_id = object_id,
                        .data_size = data_size,
                        .metadata_size = metadata_size};
  plasma_send(conn->conn, &req);
  plasma_reply reply;
  int fd = recv_fd(conn->conn, (char *) &reply, sizeof(plasma_reply));
  assert(reply.data_size == data_size);
  assert(reply.metadata_size == metadata_size);
  /* The metadata should come right after the data. */
  assert(reply.metadata_offset == reply.data_offset + data_size);
  *data = lookup_or_mmap(conn, fd, reply.store_fd_val, reply.map_size) +
          reply.data_offset;
  /* If plasma_create is being called from a transfer, then we will not copy the
   * metadata here. The metadata will be written along with the data streamed
   * from the transfer. */
  if (metadata != NULL) {
    /* Copy the metadata to the buffer. */
    memcpy(*data + reply.data_size, metadata, metadata_size);
  }
}

/* This method is used to get both the data and the metadata. */
void plasma_get(plasma_store_conn *conn,
                plasma_id object_id,
                int64_t *size,
                uint8_t **data,
                int64_t *metadata_size,
                uint8_t **metadata) {
  plasma_request req = {.type = PLASMA_GET, .object_id = object_id};
  plasma_send(conn->conn, &req);
  plasma_reply reply;
  int fd = recv_fd(conn->conn, (char *) &reply, sizeof(plasma_reply));
  *data = lookup_or_mmap(conn, fd, reply.store_fd_val, reply.map_size) +
          reply.data_offset;
  *size = reply.data_size;
  /* If requested, return the metadata as well. */
  if (metadata != NULL) {
    *metadata = *data + reply.data_size;
    *metadata_size = reply.metadata_size;
  }
}

void plasma_seal(plasma_store_conn *conn, plasma_id object_id) {
  plasma_request req = {.type = PLASMA_SEAL, .object_id = object_id};
  plasma_send(conn->conn, &req);
}

plasma_store_conn *plasma_store_connect(const char *socket_name) {
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
  /* Initialize the store connection struct */
  plasma_store_conn *result = malloc(sizeof(plasma_store_conn));
  result->conn = fd;
  result->mmap_table = NULL;
  return result;
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
  memcpy(&addr.sin_addr.s_addr, manager->h_addr, manager->h_length);
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
