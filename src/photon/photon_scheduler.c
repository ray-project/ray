#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "common.h"
#include "event_loop.h"
#include "io.h"
#include "photon.h"
#include "photon_algorithm.h"
#include "photon_scheduler.h"
#include "plasma_client.h"
#include "state/db.h"
#include "state/task_log.h"
#include "utarray.h"
#include "uthash.h"

UT_icd task_ptr_icd = {sizeof(task_instance *), NULL, NULL, NULL};
UT_icd worker_icd = {sizeof(worker), NULL, NULL, NULL};

/** Association between the socket fd of a worker and its worker_index. */
typedef struct {
  /** The socket fd of a worker. */
  int sock;
  /** The index of the worker in scheduler_info->workers. */
  int64_t worker_index;
  /** Handle for the hash table. */
  UT_hash_handle hh;
} worker_index;

struct local_scheduler_state {
  /* The local scheduler event loop. */
  event_loop *loop;
  /* The Plasma client. */
  plasma_connection *plasma_conn;
  /* Association between client socket and worker index. */
  worker_index *worker_index;
  /* Info that is exposed to the scheduling algorithm. */
  scheduler_info *scheduler_info;
  /* State for the scheduling algorithm. */
  scheduler_state *scheduler_state;
  /* Input buffer. */
  UT_array *input_buffer;
};

UT_icd byte_icd = {sizeof(uint8_t), NULL, NULL, NULL};

local_scheduler_state *init_local_scheduler(event_loop *loop,
                                            const char *redis_addr,
                                            int redis_port,
                                            const char *plasma_socket_name) {
  local_scheduler_state *state = malloc(sizeof(local_scheduler_state));
  state->loop = loop;
  /* Connect to Plasma. This method will retry if Plasma hasn't started yet.
   * Pass in a NULL manager address and port. */
  state->plasma_conn = plasma_connect(plasma_socket_name, NULL);
  /* Subscribe to notifications about sealed objects. */
  int plasma_fd = plasma_subscribe(state->plasma_conn);
  /* Add the callback that processes the notification to the event loop. */
  event_loop_add_file(loop, plasma_fd, EVENT_LOOP_READ,
                      process_plasma_notification, state);
  state->worker_index = NULL;
  /* Add scheduler info. */
  state->scheduler_info = malloc(sizeof(scheduler_info));
  utarray_new(state->scheduler_info->workers, &worker_icd);
  /* Connect to Redis. */
  state->scheduler_info->db =
      db_connect(redis_addr, redis_port, "photon", "", -1);
  db_attach(state->scheduler_info->db, loop);
  /* Add scheduler state. */
  state->scheduler_state = make_scheduler_state();
  utarray_new(state->input_buffer, &byte_icd);
  return state;
};

void free_local_scheduler(local_scheduler_state *s) {
  db_disconnect(s->scheduler_info->db);
  free(s->plasma_conn);
  worker_index *current_worker_index, *temp_worker_index;
  HASH_ITER(hh, s->worker_index, current_worker_index, temp_worker_index) {
    HASH_DEL(s->worker_index, current_worker_index);
    free(current_worker_index);
  }
  utarray_free(s->scheduler_info->workers);
  free(s->scheduler_info);
  free_scheduler_state(s->scheduler_state);
  utarray_free(s->input_buffer);
  event_loop_destroy(s->loop);
  free(s);
}

void assign_task_to_worker(scheduler_info *info,
                           task_spec *task,
                           int worker_index) {
  CHECK(worker_index < utarray_len(info->workers));
  worker *w = (worker *) utarray_eltptr(info->workers, worker_index);
  write_message(w->sock, EXECUTE_TASK, task_size(task), (uint8_t *) task);
}

void process_plasma_notification(event_loop *loop,
                                 int client_sock,
                                 void *context,
                                 int events) {
  local_scheduler_state *s = context;
  /* Read the notification from Plasma. */
  object_id obj_id;
  recv(client_sock, &obj_id, sizeof(object_id), 0);
  handle_object_available(s->scheduler_info, s->scheduler_state, obj_id);
}

void process_message(event_loop *loop, int client_sock, void *context,
                     int events) {
  local_scheduler_state *s = context;

  int64_t type;
  read_buffer(client_sock, &type, s->input_buffer);

  LOG_DEBUG("New event of type %" PRId64, type);

  switch (type) {
  case SUBMIT_TASK: {
    task_spec *spec = (task_spec *) utarray_front(s->input_buffer);
    handle_task_submitted(s->scheduler_info, s->scheduler_state, spec);
  } break;
  case TASK_DONE: {
  } break;
  case GET_TASK: {
    worker_index *wi;
    HASH_FIND_INT(s->worker_index, &client_sock, wi);
    handle_worker_available(s->scheduler_info, s->scheduler_state,
                            wi->worker_index);
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
}

void new_client_connection(event_loop *loop, int listener_sock, void *context,
                           int events) {
  local_scheduler_state *s = context;
  int new_socket = accept_client(listener_sock);
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message, s);
  LOG_DEBUG("new connection with fd %d", new_socket);
  /* Add worker to list of workers. */
  /* TODO(pcm): Where shall we free this? */
  worker_index *new_worker_index = malloc(sizeof(worker_index));
  new_worker_index->sock = new_socket;
  new_worker_index->worker_index = utarray_len(s->scheduler_info->workers);
  HASH_ADD_INT(s->worker_index, sock, new_worker_index);
  worker worker = {.sock = new_socket};
  utarray_push_back(s->scheduler_info->workers, &worker);
}

/* We need this code so we can clean up when we get a SIGTERM signal. */

local_scheduler_state *g_state;

void signal_handler(int signal) {
  if (signal == SIGTERM) {
    free_local_scheduler(g_state);
    exit(0);
  }
}

/* End of the cleanup code. */

void start_server(const char *socket_name,
                  const char *redis_addr,
                  int redis_port,
                  const char *plasma_socket_name) {
  int fd = bind_ipc_sock(socket_name, true);
  event_loop *loop = event_loop_create();
  g_state =
      init_local_scheduler(loop, redis_addr, redis_port, plasma_socket_name);

  /* Run event loop. */
  event_loop_add_file(loop, fd, EVENT_LOOP_READ, new_client_connection,
                      g_state);
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
  /* Path of the listening socket of the local scheduler. */
  char *scheduler_socket_name = NULL;
  /* IP address and port of redis. */
  char *redis_addr_port = NULL;
  /* Socket name for the local Plasma store. */
  char *plasma_socket_name = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:r:p:")) != -1) {
    switch (c) {
    case 's':
      scheduler_socket_name = optarg;
      break;
    case 'r':
      redis_addr_port = optarg;
      break;
    case 'p':
      plasma_socket_name = optarg;
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
  if (!plasma_socket_name) {
    LOG_ERR("please specify socket for connecting to Plasma with -p switch");
    exit(-1);
  }
  /* Parse the Redis address into an IP address and a port. */
  char redis_addr[16] = {0};
  char redis_port[6] = {0};
  if (!redis_addr_port ||
      sscanf(redis_addr_port, "%15[0-9.]:%5[0-9]", redis_addr, redis_port) !=
          2) {
    LOG_ERR("need to specify redis address like 127.0.0.1:6379 with -r switch");
    exit(-1);
  }
  start_server(scheduler_socket_name, &redis_addr[0], atoi(redis_port),
               plasma_socket_name);
}
