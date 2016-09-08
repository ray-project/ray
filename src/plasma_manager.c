/* PLASMA MANAGER: Local to a node, connects to other managers to send and
 * receive objects from them
 *
 * The storage manager listens on its main listening port, and if a request for
 * transfering an object to another object store comes in, it ships the data
 * using a new connection to the target object manager. */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <strings.h>
#include <poll.h>
#include <assert.h>
#include <netinet/in.h>
#include <netdb.h>

#include "event_loop.h"
#include "plasma.h"
#include "plasma_manager.h"

typedef struct {
  /* Name of the socket connecting to local plasma store. */
  const char* store_socket_name;
  /* Event loop. */
  event_loop* loop;
} plasma_manager_state;

/* Initialize the plasma manager. This function initializes the event loop
 * of the plasma manager, and stores the address 'store_socket_name' of
 * the local plasma store socket. */
void init_plasma_manager(plasma_manager_state *s, const char* store_socket_name) {
  s->loop = malloc(sizeof(event_loop));
  event_loop_init(s->loop);
  s->store_socket_name = store_socket_name;
}

/* Start transfering data to another object store manager. This establishes
 * a connection to both the manager and the local object store and sends
 * the data header to the other object manager. */
void initiate_transfer(plasma_manager_state* s, plasma_request* req) {
  int store_conn = plasma_store_connect(s->store_socket_name);
  plasma_buffer buf = { .object_id = req->object_id, .writable = 0 };
  plasma_get(store_conn, req->object_id, &buf.size, &buf.data);
  
  char ip_addr[16];
  snprintf(ip_addr, 32, "%d.%d.%d.%d",
                    req->addr[0], req->addr[1],
                    req->addr[2], req->addr[3]);

  int fd = plasma_manager_connect(&ip_addr[0], req->port);
  data_connection conn = { .type = DATA_CONNECTION_WRITE,  .store_conn = store_conn, .buf = buf, .cursor = 0 };
  event_loop_attach(s->loop, CONNECTION_DATA, &conn, fd, POLLOUT);

  plasma_request manager_req = { .type = PLASMA_DATA, .object_id = req->object_id, .size = buf.size };
  plasma_send(fd, &manager_req);
}

/* Start reading data from another object manager.
 * Initializes the object we are going to write to in the
 * local plasma store and then switches the data socket to reading mode. */
void start_reading_data(int64_t index, plasma_manager_state* s, plasma_request* req) {
  int store_conn = plasma_store_connect(s->store_socket_name);
  plasma_buffer buf = { .object_id = req->object_id, .size = req->size, .writable = 1 };
  plasma_create(store_conn, req->object_id, req->size, &buf.data);
  data_connection conn = { .type = DATA_CONNECTION_READ, .store_conn = store_conn, .buf = buf, .cursor = 0 };
  event_loop_set_connection(s->loop, index, &conn);
}

/* Handle a command request that came in through a socket (transfering data,
 * or accepting incoming data). */
void process_command(int64_t id, plasma_manager_state* state, plasma_request* req) {
  switch (req->type) {
  case PLASMA_TRANSFER:
    LOG_INFO("transfering object to manager with port %d", req->port);
    initiate_transfer(state, req);
    break;
  case PLASMA_DATA:
    LOG_INFO("starting to stream data");
    start_reading_data(id, state, req);
    break;
  default:
    LOG_ERR("invalid request %d", req->type);
    exit(-1);
  }
}

/* Handle data or command event incoming on socket with index "index". */
void read_from_socket(plasma_manager_state* state, struct pollfd *waiting, int64_t index, plasma_request* req) {
  ssize_t r, s;
  data_connection *conn = event_loop_get_connection(state->loop, index);
  switch (conn->type) {
    case DATA_CONNECTION_HEADER:
      r = read(waiting->fd, req, sizeof(plasma_request));
      if (r == -1) {
        LOG_ERR("read error");
      } else if (r == 0) {
        LOG_INFO("connection with id %" PRId64 " disconnected", index);
        event_loop_detach(state->loop, index, 1);
      } else {
        process_command(index, state, req);
      }
      break;
    case DATA_CONNECTION_READ:
      LOG_DEBUG("polled DATA_CONNECTION_READ");
      r = read(waiting->fd, conn->buf.data + conn->cursor, BUFSIZE);
      if (r == -1) {
        LOG_ERR("read error");
      } else if (r == 0) {
        LOG_INFO("end of file");
      } else {
        conn->cursor += r;
      }
      if (r == 0) {
        LOG_DEBUG("reading on channel %" PRId64 " finished", index);
        plasma_seal(conn->store_conn, conn->buf.object_id);
        close(conn->store_conn);
        event_loop_detach(state->loop, index, 1);
      }
      break;
    case DATA_CONNECTION_WRITE:
      LOG_DEBUG("polled DATA_CONNECTION_WRITE");
      s = conn->buf.size - conn->cursor;
      if (s > BUFSIZE)
        s = BUFSIZE;
      r = write(waiting->fd, conn->buf.data + conn->cursor, s);
      if (r != s) {
        if (r > 0) {
          LOG_ERR("partial write on fd %d", waiting->fd);
        } else {
          LOG_ERR("write error");
          exit(-1);
        }
      } else {
        conn->cursor += r;
      }
      if (r == 0) {
        LOG_DEBUG("writing on channel %" PRId64 " finished", index);
        close(conn->store_conn);
        event_loop_detach(state->loop, index, 1);
      }
      break;
    default:
      LOG_ERR("invalid connection type");
      exit(-1);
  }
}

/* Main event loop of the plasma manager. */
void run_event_loop(int sock, plasma_manager_state* s) {
  /* Add listening socket. */
  event_loop_attach(s->loop, CONNECTION_LISTENER, NULL, sock, POLLIN);
  plasma_request req;
  while (1) {
    int num_ready = event_loop_poll(s->loop);
    if (num_ready < 0) {
      LOG_ERR("poll failed");
      exit(-1);
    }
    for (int i = 0; i < event_loop_size(s->loop); ++i) {
      struct pollfd *waiting = event_loop_get(s->loop, i);
      if (waiting->revents == 0)
        continue;
      if (waiting->fd == sock) {
        /* Handle new incoming connections. */
        int new_socket = accept(sock, NULL, NULL);
        if (new_socket < 0) {
          if (errno != EWOULDBLOCK) {
            LOG_ERR("accept failed");
            exit(-1);
          }
          break;
        }
        data_connection conn = { .type = DATA_CONNECTION_HEADER };
        event_loop_attach(s->loop, CONNECTION_DATA, &conn, new_socket, POLLIN);
        LOG_INFO("new connection with id %" PRId64, event_loop_size(s->loop));
      } else {
        read_from_socket(s, waiting, i, &req);
      }
    }
  }
}

void start_server(const char *store_socket_name, const char* master_addr, int port) {
  struct sockaddr_in name;
  int sock = socket(PF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    LOG_ERR("could not create socket");
    exit(-1);
  }
  name.sin_family = AF_INET;
  name.sin_port = htons(port);
  name.sin_addr.s_addr = htonl(INADDR_ANY);
  int on = 1;
  /* TODO(pcm): http://stackoverflow.com/q/1150635 */
  if (ioctl(sock, FIONBIO, (char*) &on) < 0) {
    LOG_ERR("ioctl failed");
    close(sock);
    exit(-1);
  }
  setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof (on));
  if (bind(sock, (struct sockaddr*) &name, sizeof(name)) < 0) {
    LOG_ERR("could not bind socket");
    exit(-1);
  }
  LOG_INFO("listening on port %d", port);
  if (listen(sock, 5) == -1) {
    LOG_ERR("could not listen to socket");
    exit(-1);
  }
  plasma_manager_state state;
  init_plasma_manager(&state, store_socket_name);
  run_event_loop(sock, &state);
}

int main(int argc, char* argv[]) {
  /* Socket name of the plasma store this manager is connected to. */
  char *store_socket_name = NULL;
  /* IP address of this node. */
  char *master_addr = NULL;
  /* Port number the manager should use. */
  int port;
  int c;
  while ((c = getopt(argc, argv, "s:m:p:")) != -1) {
    switch (c) {
    case 's':
      store_socket_name = optarg;
      break;
    case 'm':
      master_addr = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    default:
      LOG_ERR("unknown option %c", c);
      exit(-1);
    }
  }
  if (!store_socket_name) {
    LOG_ERR("please specify socket for connecting to the plasma store with -s switch");
    exit(-1);
  }
  if (!master_addr) {
    LOG_ERR("please specify ip address of the current host in the format 123.456.789.10 with -m switch");
    exit(-1);
  }
  start_server(store_socket_name, master_addr, port);
}
