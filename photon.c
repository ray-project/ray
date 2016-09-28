#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "common.h"
#include "event_loop.h"
#include "io.h"
#include "photon.h"
#include "state/db.h"
#include "state/task_queue.h"
#include "task.h"
#include "utarray.h"

typedef struct {
  db_handle *db;
  UT_array *task_queue;
} local_scheduler_state;

event_loop *init_local_scheduler() { return event_loop_create(); };

void process_message(event_loop *loop, int client_sock, void *context,
                     int events) {
  local_scheduler_state *s = context;

  uint8_t *message;
  int64_t type;
  int64_t length;
  read_message(client_sock, &type, &length, &message);

  switch (type) {
  case SUBMIT_TASK: {
    task_spec *task = (task_spec *)message;
    CHECK(task_size(task) == length);
    unique_id id = globally_unique_id();
    task_queue_submit_task(s->db, id, task);
  } break;
  case TASK_DONE: {
  } break;
  case DISCONNECT_CLIENT: {
    LOG_INFO("Disconnecting client on fd %d", client_sock);
    event_loop_remove_file(loop, client_sock);
  } break;
  case LOG_MESSAGE: {
  } break;
  default:
    /* This code should be unreachable. */
    CHECK(0);
  }
  free(message);
}

void new_client_connection(event_loop *loop, int listener_sock, void *context,
                           int events) {
  local_scheduler_state *s = context;
  int new_socket = accept_client(listener_sock);
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message, s);
  LOG_INFO("new connection with fd %d", new_socket);
}

void start_server(const char *socket_name, const char *redis_addr,
                  int redis_port) {
  int fd = bind_ipc_sock(socket_name);
  local_scheduler_state state;
  event_loop *loop = init_local_scheduler();

  state.db = db_connect(redis_addr, redis_port, "photon", "", -1);
  db_attach(state.db, loop);

  /* Run event loop. */
  event_loop_add_file(loop, fd, EVENT_LOOP_READ, new_client_connection, &state);
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  /* Path of the listening socket of the local scheduler. */
  char *scheduler_socket_name = NULL;
  /* IP address and port of redis. */
  char *redis_addr_port = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:r:")) != -1) {
    switch (c) {
    case 's':
      scheduler_socket_name = optarg;
      break;
    case 'r':
      redis_addr_port = optarg;
      break;
    default:
      LOG_ERR("unknown option %c", c);
      exit(-1);
    }
  }
  if (!scheduler_socket_name) {
    LOG_ERR("please specify socket for incoming connections with -s switch");
    exit(-1);
  }
  char redis_addr[16] = {0};
  char redis_port[6] = {0};
  if (!redis_addr_port ||
      sscanf(redis_addr_port, "%15[0-9.]:%5[0-9]", redis_addr, redis_port) !=
          2) {
    LOG_ERR("need to specify redis address like 127.0.0.1:6379 with -r switch");
    exit(-1);
  }
  start_server(scheduler_socket_name, &redis_addr[0], atoi(redis_port));
}
