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
#include "logging.h"
#include "object_info.h"
#include "photon.h"
#include "photon_scheduler.h"
#include "photon_algorithm.h"
#include "state/db.h"
#include "state/task_table.h"
#include "state/object_table.h"
#include "utarray.h"
#include "uthash.h"

UT_icd task_ptr_icd = {sizeof(task *), NULL, NULL, NULL};
UT_icd workers_icd = {sizeof(local_scheduler_client *), NULL, NULL, NULL};

UT_icd byte_icd = {sizeof(uint8_t), NULL, NULL, NULL};

/**
 * A helper function for printing available and requested resource information.
 *
 * @param state Local scheduler state.
 * @param spec Task specification object.
 * @return Void.
 */
void print_resource_info(const local_scheduler_state *state,
                         const task_spec *spec) {
#if RAY_COMMON_LOG_LEVEL <= RAY_COMMON_DEBUG
  /* Print information about available and requested resources. */
  char buftotal[256], bufavail[256], bufresreq[256];
  snprintf(bufavail, sizeof(bufavail), "%8.4f %8.4f",
           state->dynamic_resources[CPU_RESOURCE_INDEX],
           state->dynamic_resources[GPU_RESOURCE_INDEX]);
  snprintf(buftotal, sizeof(buftotal), "%8.4f %8.4f",
           state->static_resources[CPU_RESOURCE_INDEX],
           state->static_resources[GPU_RESOURCE_INDEX]);
  if (spec) {
    snprintf(bufresreq, sizeof(bufresreq), "%8.4f %8.4f",
             task_spec_get_required_resource(spec, CPU_RESOURCE_INDEX),
             task_spec_get_required_resource(spec, GPU_RESOURCE_INDEX));
  }
  LOG_DEBUG("Resources: [total=%s][available=%s][requested=%s]", buftotal,
            bufavail, spec ? bufresreq : "n/a");
#endif
}

local_scheduler_state *init_local_scheduler(
    const char *node_ip_address,
    event_loop *loop,
    const char *redis_addr,
    int redis_port,
    const char *local_scheduler_socket_name,
    const char *plasma_store_socket_name,
    const char *plasma_manager_socket_name,
    const char *plasma_manager_address,
    bool global_scheduler_exists,
    const char *start_worker_command,
    const double static_resource_conf[]) {
  local_scheduler_state *state = malloc(sizeof(local_scheduler_state));
  /* Set the configuration struct for the local scheduler. */
  if (start_worker_command != NULL) {
    state->config.start_worker_command = strdup(start_worker_command);
  } else {
    state->config.start_worker_command = NULL;
  }
  state->config.global_scheduler_exists = global_scheduler_exists;

  state->loop = loop;
  /* Initialize the list of workers. */
  utarray_new(state->workers, &workers_icd);
  /* Connect to Redis if a Redis address is provided. */
  if (redis_addr != NULL) {
    int num_args;
    const char **db_connect_args = NULL;
    if (plasma_manager_address != NULL) {
      num_args = 4;
      db_connect_args = malloc(sizeof(char *) * num_args);
      db_connect_args[0] = "local_scheduler_socket_name";
      db_connect_args[1] = local_scheduler_socket_name;
      db_connect_args[2] = "aux_address";
      db_connect_args[3] = plasma_manager_address;
    } else {
      num_args = 2;
      db_connect_args = malloc(sizeof(char *) * num_args);
      db_connect_args[0] = "local_scheduler_socket_name";
      db_connect_args[1] = local_scheduler_socket_name;
    }
    state->db = db_connect(redis_addr, redis_port, "photon", node_ip_address,
                           num_args, db_connect_args);
    free(db_connect_args);
    db_attach(state->db, loop, false);
  } else {
    state->db = NULL;
  }
  /* Connect to Plasma. This method will retry if Plasma hasn't started yet. */
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
  /* Add the input buffer. This is used to read in messages from clients without
   * having to reallocate a new buffer every time. */
  utarray_new(state->input_buffer, &byte_icd);

  /* Initialize resource vectors. */
  for (int i = 0; i < MAX_RESOURCE_INDEX; i++) {
    state->static_resources[i] = state->dynamic_resources[i] =
        static_resource_conf[i];
  }
  /* Print some debug information about resource configuration. */
  print_resource_info(state, NULL);
  return state;
};

void free_local_scheduler(local_scheduler_state *state) {
  /* Free the command for starting new workers. */
  if (state->config.start_worker_command != NULL) {
    free(state->config.start_worker_command);
    state->config.start_worker_command = NULL;
  }

  /* Disconnect from the database. */
  if (state->db != NULL) {
    db_disconnect(state->db);
    state->db = NULL;
  }
  /* Disconnect from plasma. */
  plasma_disconnect(state->plasma_conn);
  state->plasma_conn = NULL;

  /* Free the list of workers and any tasks that are still in progress on those
   * workers. */
  for (int i = 0; i < utarray_len(state->workers); ++i) {
    local_scheduler_client **worker =
        (local_scheduler_client **) utarray_eltptr(state->workers, i);
    if ((*worker)->task_in_progress != NULL) {
      free_task((*worker)->task_in_progress);
      (*worker)->task_in_progress = NULL;
    }
    free(*worker);
    *worker = NULL;
  }
  utarray_free(state->workers);
  state->workers = NULL;

  /* Free the algorithm state. */
  free_scheduling_algorithm_state(state->algorithm_state);
  state->algorithm_state = NULL;
  /* Free the input buffer. */
  utarray_free(state->input_buffer);
  state->input_buffer = NULL;
  /* Destroy the event loop. */
  event_loop_destroy(state->loop);
  state->loop = NULL;
  /* Free the scheduler state. */
  free(state);
}

void assign_task_to_worker(local_scheduler_state *state,
                           task_spec *spec,
                           local_scheduler_client *worker) {
  if (write_message(worker->sock, EXECUTE_TASK, task_spec_size(spec),
                    (uint8_t *) spec) < 0) {
    if (errno == EPIPE || errno == EBADF) {
      /* TODO(rkn): If this happens, the task should be added back to the task
       * queue. */
      LOG_WARN(
          "Failed to give task to worker on fd %d. The client may have hung "
          "up.",
          worker->sock);
    } else {
      LOG_FATAL("Failed to give task to client on fd %d.", worker->sock);
    }
  }

  /* Resource accounting:
   * Update dynamic resource vector in the local scheduler state. */
  for (int i = 0; i < MAX_RESOURCE_INDEX; i++) {
    state->dynamic_resources[i] -= task_spec_get_required_resource(spec, i);
    CHECKM(state->dynamic_resources[i] >= 0,
           "photon dynamic resources dropped to %8.4f\t%8.4f\n",
           state->dynamic_resources[0], state->dynamic_resources[1]);
  }
  print_resource_info(state, spec);
  task *task = alloc_task(spec, TASK_STATUS_RUNNING,
                          state->db ? get_db_client_id(state->db) : NIL_ID);
  /* Record which task this worker is executing. This will be freed in
   * process_message when the worker sends a GET_TASK message to the local
   * scheduler. */
  worker->task_in_progress = copy_task(task);
  /* Update the global task table. */
  if (state->db != NULL) {
    task_table_update(state->db, task, (retry_info *) &photon_retry, NULL,
                      NULL);
  } else {
    free_task(task);
  }
}

void process_plasma_notification(event_loop *loop,
                                 int client_sock,
                                 void *context,
                                 int events) {
  local_scheduler_state *state = context;
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

  if (object_info.is_deletion) {
    handle_object_removed(state, object_info.obj_id);
  } else {
    handle_object_available(state, state->algorithm_state, object_info.obj_id);
  }
}

void reconstruct_task_update_callback(task *task, void *user_context) {
  if (task == NULL) {
    /* The test-and-set of the task's scheduling state failed, so the task was
     * either not finished yet, or it was already being reconstructed.
     * Suppress the reconstruction request. */
    return;
  }
  /* Otherwise, the test-and-set succeeded, so resubmit the task for execution
   * to ensure that reconstruction will happen. */
  local_scheduler_state *state = user_context;
  task_spec *spec = task_task_spec(task);
  handle_task_submitted(state, state->algorithm_state, spec);

  /* Recursively reconstruct the task's inputs, if necessary. */
  for (int64_t i = 0; i < task_num_args(spec); ++i) {
    if (task_arg_type(spec, i) == ARG_BY_REF) {
      object_id arg_id = task_arg_id(spec, i);
      reconstruct_object(state, arg_id);
    }
  }
}

void reconstruct_result_lookup_callback(object_id reconstruct_object_id,
                                        task_id task_id,
                                        void *user_context) {
  /* TODO(swang): The following check will fail if an object was created by a
   * put. */
  CHECKM(!IS_NIL_ID(task_id),
         "No task information found for object during reconstruction");
  local_scheduler_state *state = user_context;
  /* Try to claim the responsibility for reconstruction by doing a test-and-set
   * of the task's scheduling state in the global state. If the task's
   * scheduling state is pending completion, assume that reconstruction is
   * already being taken care of. NOTE: This codepath is not responsible for
   * detecting failure of the other reconstruction, or updating the
   * scheduling_state accordingly. */
  task_table_test_and_update(
      state->db, task_id, TASK_STATUS_DONE, TASK_STATUS_RECONSTRUCTING,
      (retry_info *) &photon_retry, reconstruct_task_update_callback, state);
}

void reconstruct_object_lookup_callback(object_id reconstruct_object_id,
                                        int manager_count,
                                        const char *manager_vector[],
                                        void *user_context) {
  LOG_DEBUG("Manager count was %d", manager_count);
  /* Only continue reconstruction if we find that the object doesn't exist on
   * any nodes. NOTE: This codepath is not responsible for checking if the
   * object table entry is up-to-date. */
  local_scheduler_state *state = user_context;
  if (manager_count == 0) {
    /* Look up the task that created the object in the result table. */
    result_table_lookup(state->db, reconstruct_object_id,
                        (retry_info *) &photon_retry,
                        reconstruct_result_lookup_callback, (void *) state);
  }
}

void reconstruct_object(local_scheduler_state *state,
                        object_id reconstruct_object_id) {
  LOG_DEBUG("Starting reconstruction");
  /* TODO(swang): Track task lineage for puts. */
  CHECK(state->db != NULL);
  /* Determine if reconstruction is necessary by checking if the object exists
   * on a node. */
  object_table_lookup(state->db, reconstruct_object_id,
                      (retry_info *) &photon_retry,
                      reconstruct_object_lookup_callback, (void *) state);
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  local_scheduler_client *worker = context;
  local_scheduler_state *state = worker->local_scheduler_state;

  int64_t type;
  int64_t length = read_buffer(client_sock, &type, state->input_buffer);

  LOG_DEBUG("New event of type %" PRId64, type);

  switch (type) {
  case SUBMIT_TASK: {
    task_spec *spec = (task_spec *) utarray_front(state->input_buffer);
    handle_task_submitted(state, state->algorithm_state, spec);
  } break;
  case TASK_DONE: {
  } break;
  case EVENT_LOG_MESSAGE: {
    /* Parse the message. TODO(rkn): Redo this using flatbuffers to serialize
     * the message. */
    uint8_t *message = (uint8_t *) utarray_front(state->input_buffer);
    int64_t offset = 0;
    int64_t key_length;
    memcpy(&key_length, &message[offset], sizeof(key_length));
    offset += sizeof(key_length);
    int64_t value_length;
    memcpy(&value_length, &message[offset], sizeof(value_length));
    offset += sizeof(value_length);
    uint8_t *key = malloc(key_length);
    memcpy(key, &message[offset], key_length);
    offset += key_length;
    uint8_t *value = malloc(value_length);
    memcpy(value, &message[offset], value_length);
    offset += value_length;
    CHECK(offset == length);
    if (state->db != NULL) {
      ray_log_event(state->db, key, key_length, value, value_length);
    }
    free(key);
    free(value);
  } break;
  case GET_TASK: {
    /* If this worker reports a completed task: account for resources. */
    if (worker->task_in_progress != NULL) {
      task_spec *spec = task_task_spec(worker->task_in_progress);
      /* Return dynamic resources back for the task in progress. */
      for (int i = 0; i < MAX_RESOURCE_INDEX; i++) {
        state->dynamic_resources[i] += task_spec_get_required_resource(spec, i);
        /* Sanity-check resource vector boundary conditions. */
        CHECK(state->dynamic_resources[i] <= state->static_resources[i]);
      }
      print_resource_info(state, spec);
      /* If we're connected to Redis, update tables. */
      if (state->db != NULL) {
        /* Update control state tables. */
        task_set_state(worker->task_in_progress, TASK_STATUS_DONE);
        task_table_update(state->db, worker->task_in_progress,
                          (retry_info *) &photon_retry, NULL, NULL);
        /* The call to task_table_update takes ownership of the
         * task_in_progress, so we set the pointer to NULL so it is not used. */
      } else {
        free_task(worker->task_in_progress);
      }
      worker->task_in_progress = NULL;
    }
    /* Let the scheduling algorithm process the fact that there is an available
     * worker. */
    handle_worker_available(state, state->algorithm_state, worker);
  } break;
  case RECONSTRUCT_OBJECT: {
    object_id *obj_id = (object_id *) utarray_front(state->input_buffer);
    reconstruct_object(state, *obj_id);
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
  local_scheduler_state *state = context;
  int new_socket = accept_client(listener_sock);
  /* Create a struct for this worker. This will be freed when we free the local
   * scheduler state. */
  local_scheduler_client *worker = malloc(sizeof(local_scheduler_client));
  worker->sock = new_socket;
  worker->task_in_progress = NULL;
  worker->local_scheduler_state = state;
  utarray_push_back(state->workers, &worker);
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message,
                      worker);
  LOG_DEBUG("new connection with fd %d", new_socket);
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

int heartbeat_handler(event_loop *loop, timer_id id, void *context) {
  local_scheduler_state *state = context;
  scheduling_algorithm_state *algorithm_state = state->algorithm_state;
  local_scheduler_info info;
  /* Ask the scheduling algorithm to fill out the scheduler info struct. */
  provide_scheduler_info(state, algorithm_state, &info);
  /* Publish the heartbeat to all subscribers of the local scheduler table. */
  local_scheduler_table_send_info(state->db, &info, NULL);
  /* Reset the timer. */
  return LOCAL_SCHEDULER_HEARTBEAT_TIMEOUT_MILLISECONDS;
}

void start_new_worker(local_scheduler_state *state) {
  /* We can't start a worker if we don't have the path to the worker script. */
  CHECK(state->config.start_worker_command != NULL);
  /* Launch the process to create the worker. */
  FILE *p = popen(state->config.start_worker_command, "r");
  UNUSED(p);
}

void start_server(const char *node_ip_address,
                  const char *socket_name,
                  const char *redis_addr,
                  int redis_port,
                  const char *plasma_store_socket_name,
                  const char *plasma_manager_socket_name,
                  const char *plasma_manager_address,
                  bool global_scheduler_exists,
                  const char *start_worker_command,
                  const double static_resource_conf[]) {
  /* Ignore SIGPIPE signals. If we don't do this, then when we attempt to write
   * to a client that has already died, the local scheduler could die. */
  signal(SIGPIPE, SIG_IGN);
  int fd = bind_ipc_sock(socket_name, true);
  event_loop *loop = event_loop_create();
  g_state = init_local_scheduler(
      node_ip_address, loop, redis_addr, redis_port, socket_name,
      plasma_store_socket_name, plasma_manager_socket_name,
      plasma_manager_address, global_scheduler_exists, start_worker_command,
      static_resource_conf);
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
  /* Create a timer for publishing information about the load on the local
   * scheduler to the local scheduler table. This message also serves as a
   * heartbeat. */
  if (g_state->db != NULL) {
    event_loop_add_timer(loop, LOCAL_SCHEDULER_HEARTBEAT_TIMEOUT_MILLISECONDS,
                         heartbeat_handler, g_state);
  }
  /* Create a timer for fetching queued tasks' missing object dependencies. */
  event_loop_add_timer(loop, LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS,
                       fetch_object_timeout_handler, g_state);
  /* Run event loop. */
  event_loop_run(loop);
}

/* Only declare the main function if we are not in testing mode, since the test
 * suite has its own declaration of main. */
#ifndef PHOTON_TEST
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
  /* Address for the plasma manager associated with this Photon instance. */
  char *plasma_manager_address = NULL;
  /* The IP address of the node that this local scheduler is running on. */
  char *node_ip_address = NULL;
  /* The command to run when starting new workers. */
  char *start_worker_command = NULL;
  /* Comma-separated list of configured resource capabilities for this node. */
  char *static_resource_list = NULL;
  double static_resource_conf[MAX_RESOURCE_INDEX];
  int c;
  bool global_scheduler_exists = true;
  while ((c = getopt(argc, argv, "s:r:p:m:ga:h:w:c:")) != -1) {
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
    case 'g':
      global_scheduler_exists = false;
      break;
    case 'a':
      plasma_manager_address = optarg;
      break;
    case 'h':
      node_ip_address = optarg;
      break;
    case 'w':
      start_worker_command = optarg;
      break;
    case 'c':
      static_resource_list = optarg;
      break;
    default:
      LOG_FATAL("unknown option %c", c);
    }
  }
  if (!static_resource_list) {
    /* Use defaults for this node's static resource configuration. */
    memset(&static_resource_conf[0], 0, sizeof(static_resource_conf));
    static_resource_conf[CPU_RESOURCE_INDEX] = DEFAULT_NUM_CPUS;
    static_resource_conf[GPU_RESOURCE_INDEX] = DEFAULT_NUM_GPUS;
  } else {
    /* Tokenize the string. */
    const char delim[2] = ",";
    char *token;
    int idx = 0; /* Index into the resource vector. */
    token = strtok(static_resource_list, delim);
    while (token != NULL && idx < MAX_RESOURCE_INDEX) {
      static_resource_conf[idx++] = atoi(token);
      /* Attempt to get the next token. */
      token = strtok(NULL, delim);
    }
  }
  if (!scheduler_socket_name) {
    LOG_FATAL("please specify socket for incoming connections with -s switch");
  }
  if (!plasma_store_socket_name) {
    LOG_FATAL(
        "please specify socket for connecting to Plasma store with -p switch");
  }
  if (!node_ip_address) {
    LOG_FATAL("please specify the node IP address with -h switch");
  }
  if (!redis_addr_port) {
    /* Start the local scheduler without connecting to Redis. In this case, all
     * submitted tasks will be queued and scheduled locally. */
    if (plasma_manager_socket_name) {
      LOG_FATAL(
          "if a plasma manager socket name is provided with the -m switch, "
          "then a redis address must be provided with the -r switch");
    }
    start_server(node_ip_address, scheduler_socket_name, NULL, -1,
                 plasma_store_socket_name, NULL, plasma_manager_address,
                 global_scheduler_exists, start_worker_command,
                 static_resource_conf);
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
    start_server(node_ip_address, scheduler_socket_name, &redis_addr[0],
                 atoi(redis_port), plasma_store_socket_name,
                 plasma_manager_socket_name, plasma_manager_address,
                 global_scheduler_exists, start_worker_command,
                 static_resource_conf);
  }
}
#endif
