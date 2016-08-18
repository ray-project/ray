// PLASMA MANAGER: Local to a node, connects to other managers to send and
// receive objects from them
//
// The storage manager listens on its main listening port, and if a request for
// transfering an object to another object store comes in, it ships the data
// using a new connection to the target object manager. Also keeps a list of
// other object managers.

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

#include "plasma.h"

#define MAX_CONNECTIONS 2048
#define MAX_NUM_MANAGERS 1024

enum conn_type {
  // Connection to send commands to the manager.
  CONN_CONTROL,
  // Connection to send data to another manager.
  CONN_WRITE_DATA,
  // Connection to receive data from another manager.
  CONN_READ_DATA
};

typedef struct {
  // Unique identifier for the connection.
  int id;
  // Of type conn_type.
  int type;
  // Socket of the plasma store that is accessed for reading or writing data for
  // this connection.
  int store_conn;
  // Buffer this connection is reading from or writing to.
  plasma_buffer buf;
  // Current position in the buffer.
  int64_t cursor;
} conn_state;

typedef struct {
  // Address of the manager.
  struct sockaddr_in name;
  // Is this manager connected?
  int connected;
} manager_state;

typedef struct {
  // Name of the socket connecting to local plasma store.
  const char* store_socket_name;
  // Number of connections.
  int num_conn;
  // For the "poll" system call.
  struct pollfd waiting[MAX_CONNECTIONS];
  // Status of connections (both control and data).
  conn_state conn[MAX_CONNECTIONS];
  // Other plasma managers in the cluster.
  manager_state managers[MAX_NUM_MANAGERS];
} plasma_manager_state;

void init_manager_state(plasma_manager_state *s, const char* store_socket_name) {
  memset(&s->waiting, 0, sizeof(s->waiting));
  memset(&s->conn, 0, sizeof(s->conn));
  memset(&s->managers, 0, sizeof(s->managers));
  s->num_conn = 0;
  s->store_socket_name = store_socket_name;
}

#define h_addr h_addr_list[0]

// Add name info for another plasma manager from the cluster.
void add_manager(plasma_manager_state *s, int manager_id, char *ip_address, int port) {
  assert(ip_address);
  assert(s);
  struct hostent *manager = gethostbyname(ip_address);
  if (!manager) {
    LOG_ERR("plasma manager %s not found", ip_address);
    exit(-1);
  }
  s->managers[manager_id].connected = 1;
  struct sockaddr_in *name = &s->managers[manager_id].name;
  name->sin_family = AF_INET;
  bcopy(manager->h_addr, &name->sin_addr.s_addr, manager->h_length);
  name->sin_port = htons(port);
}

// Add connection for sending commands or data to another plasma manager
// (returns the connection id).
int add_conn(plasma_manager_state* s, int type, int fd, int events, plasma_buffer* buf) {
  static int conn_id = 0;
  s->waiting[s->num_conn].fd = fd;
  s->waiting[s->num_conn].events = events;
  s->conn[s->num_conn].id = conn_id;
  s->conn[s->num_conn].type = type;
  if (buf) {
    s->conn[s->num_conn].buf = *buf;
  }
  s->conn[s->num_conn].cursor = 0;
  s->num_conn += 1;
  return conn_id++;
}

// Remove connection with index i by swapping it with the last element.
void remove_conn(plasma_manager_state* s, int i) {
  memcpy(&s->waiting[i], &s->waiting[s->num_conn-1], sizeof(struct pollfd));
  memset(&s->waiting[s->num_conn-1], 0, sizeof(struct pollfd));
  memcpy(&s->conn[i], &s->conn[s->num_conn-1], sizeof(conn_state));
  memset(&s->conn[s->num_conn-1], 0, sizeof(conn_state));
}

#define BUFSIZE 4096

// Start transfering data to another object store manager. This establishes
// a connection to both the manager and the local object store and sends
// the data header to the other object manager.
void initiate_transfer(plasma_manager_state* state, plasma_request* req) {
  int manager_id = req->manager_id;
  int c = plasma_store_connect(state->store_socket_name);
  plasma_buffer buf = plasma_get(c, req->object_id);

  int fd = socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    LOG_ERR("could not create socket");
    exit(-1);
  }
  int r = connect(fd, (struct sockaddr*) &state->managers[manager_id].name, sizeof(state->managers[manager_id].name));
  if (r < 0) {
    LOG_ERR("could not establish connection to manager with id %d", manager_id);
    exit(-1);
  }

  add_conn(state, CONN_WRITE_DATA, fd, POLLOUT, &buf);

  plasma_request manager_req = { .type = PLASMA_DATA, .object_id = req->object_id, .size = buf.size };
  LOG_INFO("filedescriptor is %d", fd);
  plasma_send(fd, &manager_req);
}

void setup_data_connection(int conn_idx, plasma_manager_state* state, plasma_request* req) {
  int store_conn = plasma_store_connect(state->store_socket_name);
  state->conn[conn_idx].type = CONN_READ_DATA;
  state->conn[conn_idx].store_conn = store_conn;
  state->conn[conn_idx].buf = plasma_create(store_conn, req->object_id, req->size);
  state->conn[conn_idx].cursor = 0;
}

// Handle a command request that came in through a socket (transfering data,
// registering object managers, accepting incoming data).
void process_command(int conn_idx, plasma_manager_state* state, plasma_request* req) {
  switch (req->type) {
  case PLASMA_TRANSFER:
    LOG_INFO("transfering object to manager with id %d", req->manager_id);
    initiate_transfer(state, req);
    break;
  case PLASMA_REGISTER: {
      char buff[16];
      snprintf(buff, 32, "%d.%d.%d.%d",
               req->addr[0], req->addr[1],
               req->addr[2], req->addr[3]);
      if (req->manager_id >= MAX_NUM_MANAGERS) {
        LOG_ERR("manager_id %d out of bounds", req->manager_id);
      } else {
        add_manager(state, req->manager_id, buff, req->port);
        LOG_INFO("registering %s:%d with id %d", buff, req->port, req->manager_id);
      }
    }
    break;
  case PLASMA_DATA:
    LOG_INFO("starting to stream data");
    setup_data_connection(conn_idx, state, req);
    break;
  default:
    LOG_ERR("invalid request %d", req->type);
    exit(-1);
  }
}

// Handle data or command event incoming on socket with index i.
void read_from_socket(plasma_manager_state* state, int i, plasma_request* req) {
  ssize_t r, s;
  switch (state->conn[i].type) {
    case CONN_CONTROL:
      r = read(state->waiting[i].fd, req, sizeof(plasma_request));
      if (r == 1) {
        LOG_ERR("read error");
      } else if (r == 0) {
        LOG_INFO("connection with id %d disconnected", state->conn[i].id);
        remove_conn(state, i);
      } else {
        process_command(i, state, req);
      }
      break;
    case CONN_READ_DATA:
      LOG_DEBUG("polled CONN_READ_DATA");
      r = read(state->waiting[i].fd, state->conn[i].buf.data + state->conn[i].cursor, BUFSIZE);
      if (r == -1) {
        LOG_ERR("read error");
      } else if (r == 0) {
        LOG_INFO("end of file");
      } else {
        state->conn[i].cursor += r;
      }
      if (r == 0) {
        close(state->waiting[i].fd);
        state->waiting[i].fd = 0;
        state->waiting[i].events = 0;
        plasma_seal(state->conn[i].store_conn, state->conn[i].buf.object_id);
      }
      break;
    case CONN_WRITE_DATA:
      LOG_DEBUG("polled CONN_WRITE_DATA");
      s = state->conn[i].buf.size - state->conn[i].cursor;
      if (s > BUFSIZE)
        s = BUFSIZE;
      r = write(state->waiting[i].fd, state->conn[i].buf.data + state->conn[i].cursor, s);
      if (r != s) {
        if (r > 0) {
          LOG_ERR("partial write on fd %d", state->waiting[i].fd);
        } else {
          LOG_ERR("write error");
          exit(-1);
        }
      } else {
        state->conn[i].cursor += r;
      }
      if (r == 0) {
        close(state->waiting[i].fd);
        state->waiting[i].fd = 0;
        state->waiting[i].events = 0;
      }
      break;
    default:
      LOG_ERR("invalid connection type");
      exit(-1);
  }
}

// Main event loop of the plasma manager.
void event_loop(int sock, plasma_manager_state* state) {
  // Add listening socket.
  add_conn(state, CONN_CONTROL, sock, POLLIN, NULL);
  plasma_request req;
  while (1) {
    int num_ready = poll(state->waiting, state->num_conn, -1);
    if (num_ready < 0) {
      LOG_ERR("poll failed");
      exit(-1);
    }
    for (int i = 0; i < state->num_conn; ++i) {
      if (state->waiting[i].revents == 0)
        continue;
      if (state->waiting[i].fd == sock) {
        // Handle new incoming connections.
        int new_socket = accept(sock, NULL, NULL);
        if (new_socket < 0) {
          if (errno != EWOULDBLOCK) {
            LOG_ERR("accept failed");
            exit(-1);
          }
          break;
        }
        int conn_id = add_conn(state, CONN_CONTROL, new_socket, POLLIN, NULL);
        LOG_INFO("new connection with id %d", conn_id);
      } else {
        read_from_socket(state, i, &req);
      }
    }
  }
}

// Register this plasma manager with the nameserver.
void register_with_nameserver(const char* nameserver_addr, int nameserver_port,
                              const char* manager_addr, int manager_port) {
  int fd = socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    LOG_ERR("socket for nameserver connection could not be established");
    exit(-1);
  }
  struct hostent *host = gethostbyname(nameserver_addr);
  if (!host) {
    LOG_ERR("nameserver %s not found", nameserver_addr);
    exit(-1);
  }
  struct sockaddr_in nameserver;
  memset(&nameserver, 0, sizeof(struct sockaddr_in));
  nameserver.sin_family = AF_INET;
  bcopy(host->h_addr, &nameserver.sin_addr.s_addr, host->h_length);
  nameserver.sin_port = htons(nameserver_port);
  if (connect(fd, (struct sockaddr*) &nameserver, sizeof(nameserver)) == -1) {
    LOG_ERR("could not connect to nameserver %s:%d", nameserver_addr, nameserver_port);
    exit(-1);
  }
  plasma_request req = { .type = PLASMA_REGISTER, .port = manager_port };
  // TODO(pcm): input validation
  sscanf(manager_addr, "%" SCNu8 ".%" SCNu8 ".%" SCNu8 ".%" SCNu8, &req.addr[0], &req.addr[1], &req.addr[2], &req.addr[3]);
  plasma_send(fd, &req);
  close(fd);
}

void start_server(const char *store_socket_name, const char* master_addr,
                  const char* nameserver_addr, int nameserver_port) {
  struct sockaddr_in name;
  int sock = socket(PF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    LOG_ERR("could not create socket");
    exit(-1);
  }
  name.sin_family = AF_INET;
  name.sin_port = 0;
  name.sin_addr.s_addr = htonl(INADDR_ANY);
  int on = 1;
  // TODO(pcm): http://stackoverflow.com/q/1150635
  if (ioctl(sock, FIONBIO, (char*) &on) < 0) {
    LOG_ERR("ioctl failed");
    close(sock);
    exit(-1);
  }
  if (bind(sock, (struct sockaddr*) &name, sizeof(name)) < 0) {
    LOG_ERR("could not bind socket");
    exit(-1);
  }
  socklen_t len = sizeof(name);
  if (getsockname(sock, (struct sockaddr*) &name, &len) == -1) {
    LOG_ERR("getsockname failed");
  } else {
    LOG_INFO("listening on port %d", ntohs(name.sin_port));
  }
  if (listen(sock, 5) == -1) {
    LOG_ERR("could not listen to socket");
    exit(-1);
  }
  register_with_nameserver(nameserver_addr, nameserver_port, master_addr, ntohs(name.sin_port));
  plasma_manager_state state;
  init_manager_state(&state, store_socket_name);
  event_loop(sock, &state);
}

int main(int argc, char* argv[]) {
  // Socket name of the plasma store this manager is connected to.
  char *store_socket_name = NULL;
  // IP address and port of the nameserver.
  char *nameserver_addr_port = NULL;
  // IP address this host can be reached at from the outside.
  char *master_addr = NULL;
  int c;
  while ((c = getopt(argc, argv, "n:s:m:")) != -1) {
    switch (c) {
    case 's':
      store_socket_name = optarg;
      break;
    case 'n':
      nameserver_addr_port = optarg;
      break;
    case 'm':
      master_addr = optarg;
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
  // Parse nameserver address and port.
  const char *format = "%15[0-9.]:%5[0-9]";
  char nameserver_addr[16] = { 0 };
  char nameserver_port[6] = { 0 };
  if (!nameserver_addr_port || sscanf(nameserver_addr_port, format, nameserver_addr, nameserver_port) != 2) {
    LOG_ERR("need to specify nameserver address in the format 123.456.789.10:12345 with -n switch");
    exit(-1);
  }
  start_server(store_socket_name, master_addr, nameserver_addr, atoi(nameserver_port));
}
