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
#include "object_info.h"
#include "photon.h"
#include "photon_algorithm.h"
#include "photon_scheduler.h"
#include "state/db.h"
#include "state/task_table.h"
#include "state/object_table.h"
#include "utarray.h"
#include "uthash.h"
#include "object_info.h"

UT_icd task_ptr_icd = {sizeof(task *), NULL, NULL, NULL};
UT_icd worker_icd = {sizeof(worker), NULL, NULL, NULL};

UT_icd byte_icd = {sizeof(uint8_t), NULL, NULL, NULL};

local_scheduler_state *init_local_scheduler(
    event_loop *loop,
    const char *redis_addr,
    int redis_port,
    const char *plasma_store_socket_name,
    const char *plasma_manager_socket_name) {
  local_scheduler_state *state = malloc(sizeof(local_scheduler_state));
  state->loop = loop;
  state->worker_index = NULL;
  /* Add scheduler info. */
  utarray_new(state->workers, &worker_icd);
  /* Connect to Redis if a Redis address is provided. */
  if (redis_addr != NULL) {
    state->db = db_connect(redis_addr, redis_port, "photon", "", -1);
    db_attach(state->db, loop, false);
  } else {
    state->db = NULL;
  }
  /* Connect to Plasma. This method will retry if Plasma hasn't started yet.
   * Pass in a NULL manager address and port. */
  state->plasma_conn =
      plasma_connect(plasma_store_socket_name, plasma_manager_socket_name,
                     PLASMA_DEFAULT_RELEASE_DELAY);
  /* Subscribe to notifications about sealed objects. */
  int plasma_fd = plasma_subscribe(state->plasma_conn);
  /* Add the callback that processes the notification to the event loop. */
  event_loop_add_file(loop, plasma_fd, EVENT_LOOP_READ,
                      process_plasma_notification, state);
  /* Add scheduler state. */
  state->algorithm_state = make_scheduling_algorithm_state();
  utarray_new(state->input_buffer, &byte_icd);
  return state;
};

void free_local_scheduler(local_scheduler_state *s) {
  if (s->db != NULL) {
    db_disconnect(s->db);
  }
  plasma_disconnect(s->plasma_conn);
  worker_index *current_worker_index, *temp_worker_index;
  HASH_ITER(hh, s->worker_index, current_worker_index, temp_worker_index) {
    HASH_DEL(s->worker_index, current_worker_index);
    free(current_worker_index);
  }
  utarray_free(s->workers);
  free_scheduling_algorithm_state(s->algorithm_state);
  utarray_free(s->input_buffer);
  event_loop_destroy(s->loop);
  free(s);
}

void assign_task_to_worker(local_scheduler_state *state,
                           task_spec *spec,
                           int worker_index,
                           bool from_global_scheduler) {
  CHECK(worker_index < utarray_len(state->workers));
  worker *w = (worker *) utarray_eltptr(state->workers, worker_index);
  write_message(w->sock, EXECUTE_TASK, task_spec_size(spec), (uint8_t *) spec);
  /* Update the global task table. */
  if (state->db != NULL) {
    retry_info retry;
    memset(&retry, 0, sizeof(retry));
    retry.num_retries = 0;
    retry.timeout = 100;
    retry.fail_callback = NULL;
    task *task =
        alloc_task(spec, TASK_STATUS_RUNNING, get_db_client_id(state->db));
    if (from_global_scheduler) {
      task_table_update(state->db, task, (retry_info *) &retry, NULL, NULL);
    } else {
      task_table_add_task(state->db, task, (retry_info *) &retry, NULL, NULL);
    }
  }
}

void process_plasma_notification(event_loop *loop,
                                 int client_sock,
                                 void *context,
                                 int events) {
  local_scheduler_state *s = context;
  /* Read the notification from Plasma. */
  object_info object_info;
  int error =
      read_bytes(client_sock, (uint8_t *) &object_info, sizeof(object_info));
  if (error < 0) {
    /* The store has closed the socket. */
    LOG_DEBUG(
        "The plasma store has closed the object notification socket, or some "
        "other error has occurred.");
    event_loop_remove_file(loop, client_sock);
    close(client_sock);
    return;
  }
  handle_object_available(s, s->algorithm_state, object_info.obj_id);
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  local_scheduler_state *s = context;

  int64_t type;
  read_buffer(client_sock, &type, s->input_buffer);

  LOG_DEBUG("New event of type %" PRId64, type);

  switch (type) {
  case SUBMIT_TASK: {
    task_spec *spec = (task_spec *) utarray_front(s->input_buffer);
    handle_task_submitted(s, s->algorithm_state, spec);
  } break;
  case TASK_DONE: {
  } break;
  case GET_TASK: {
    worker_index *wi;
    HASH_FIND_INT(s->worker_index, &client_sock, wi);
    handle_worker_available(s, s->algorithm_state, wi->worker_index);
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

void new_client_connection(event_loop *loop,
                           int listener_sock,
                           void *context,
                           int events) {
  local_scheduler_state *s = context;
  int new_socket = accept_client(listener_sock);
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message, s);
  LOG_DEBUG("new connection with fd %d", new_socket);
  /* Add worker to list of workers. */
  /* TODO(pcm): Where shall we free this? */
  worker_index *new_worker_index = malloc(sizeof(worker_index));
  new_worker_index->sock = new_socket;
  new_worker_index->worker_index = utarray_len(s->workers);
  HASH_ADD_INT(s->worker_index, sock, new_worker_index);
  worker worker;
  memset(&worker, 0, sizeof(worker));
  worker.sock = new_socket;
  utarray_push_back(s->workers, &worker);
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

void handle_task_scheduled_callback(task *original_task, void *user_context) {
  handle_task_scheduled(g_state, g_state->algorithm_state,
                        task_task_spec(original_task));
}

void start_server(const char *socket_name,
                  const char *redis_addr,
                  int redis_port,
                  const char *plasma_store_socket_name,
                  const char *plasma_manager_socket_name) {
  int fd = bind_ipc_sock(socket_name, true);
  event_loop *loop = event_loop_create();
  g_state = init_local_scheduler(loop, redis_addr, redis_port,
                                 plasma_store_socket_name,
                                 plasma_manager_socket_name);

  /* Register a callback for registering new clients. */
  event_loop_add_file(loop, fd, EVENT_LOOP_READ, new_client_connection,
                      g_state);
  /* Subscribe to receive notifications about tasks that are assigned to this
   * local scheduler by the global scheduler. TODO(rkn): we also need to get any
   * tasks that were assigned to this local scheduler before the call to
   * subscribe. */
  retry_info retry;
  memset(&retry, 0, sizeof(retry));
  retry.num_retries = 0;
  retry.timeout = 100;
  retry.fail_callback = NULL;
  if (g_state->db != NULL) {
    task_table_subscribe(g_state->db, get_db_client_id(g_state->db),
                         TASK_STATUS_SCHEDULED, handle_task_scheduled_callback,
                         NULL, &retry, NULL, NULL);
  }
  /* Run event loop. */
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
  /* Path of the listening socket of the local scheduler. */
  char *scheduler_socket_name = NULL;
  /* IP address and port of redis. */
  char *redis_addr_port = NULL;
  /* Socket name for the local Plasma store. */
  char *plasma_store_socket_name = NULL;
  /* Socket name for the local Plasma manager. */
  char *plasma_manager_socket_name = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:r:p:m:")) != -1) {
    switch (c) {
    case 's':
      scheduler_socket_name = optarg;
      break;
    case 'r':
      redis_addr_port = optarg;
      break;
    case 'p':
      plasma_store_socket_name = optarg;
      break;
    case 'm':
      plasma_manager_socket_name = optarg;
      break;
    default:
      LOG_FATAL("unknown option %c", c);
    }
  }
  if (!scheduler_socket_name) {
    LOG_FATAL("please specify socket for incoming connections with -s switch");
  }
  if (!plasma_store_socket_name) {
    LOG_FATAL(
        "please specify socket for connecting to Plasma store with -p switch");
  }
  if (!redis_addr_port) {
    /* Start the local scheduler without connecting to Redis. In this case, all
     * submitted tasks will be queued and scheduled locally. */
    if (plasma_manager_socket_name) {
      LOG_FATAL(
          "if a plasma manager socket name is provided with the -m switch, "
          "then a redis address must be provided with the -r switch");
    }
    start_server(scheduler_socket_name, NULL, -1, plasma_store_socket_name,
                 NULL);
  } else {
    /* Parse the Redis address into an IP address and a port. */
    char redis_addr[16] = {0};
    char redis_port[6] = {0};
    int num_assigned =
        sscanf(redis_addr_port, "%15[0-9.]:%5[0-9]", redis_addr, redis_port);
    if (num_assigned != 2) {
      LOG_FATAL(
          "if a redis address is provided with the -r switch, it should be "
          "formatted like 127.0.0.1:6379");
    }
    if (!plasma_manager_socket_name) {
      LOG_FATAL(
          "please specify socket for connecting to Plasma manager with -m "
          "switch");
    }
    start_server(scheduler_socket_name, &redis_addr[0], atoi(redis_port),
                 plasma_store_socket_name, plasma_manager_socket_name);
  }
}
