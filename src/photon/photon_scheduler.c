#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include "common.h"
#include "event_loop.h"
#include "io.h"
#include "logging.h"
#include "object_info.h"
#include "photon.h"
#include "photon_scheduler.h"
#include "photon_algorithm.h"
#include "state/actor_notification_table.h"
#include "state/db.h"
#include "state/task_table.h"
#include "state/object_table.h"
#include "utarray.h"
#include "uthash.h"

UT_icd task_ptr_icd = {sizeof(task *), NULL, NULL, NULL};
UT_icd workers_icd = {sizeof(local_scheduler_client *), NULL, NULL, NULL};

UT_icd pid_t_icd = {sizeof(pid_t), NULL, NULL, NULL};

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

/**
 * Kill a worker, if it is a child process, and clean up all of its associated
 * state.
 *
 * @param worker A pointer to the worker we want to kill.
 * @param wait A bool representing whether we should wait for the worker's
 *        process to exit. If the worker is not a child process, this flag is
 *        ignored.
 * @return Void.
 */
void kill_worker(local_scheduler_client *worker, bool wait) {
  /* TODO(swang): This method should also propagate changes to other parts of
   * the system to reflect the killed task in progress, if there was one.  This
   * includes updating dynamic resources and updating the task table. */
  /* Erase the worker from the array of workers. */
  local_scheduler_state *state = worker->local_scheduler_state;
  int num_workers = utarray_len(state->workers);
  for (int i = 0; i < utarray_len(state->workers); ++i) {
    local_scheduler_client *active_worker =
        *(local_scheduler_client **) utarray_eltptr(state->workers, i);
    if (active_worker == worker) {
      utarray_erase(state->workers, i, 1);
    }
  }
  /* Make sure that we erased exactly 1 worker. */
  CHECKM(!(utarray_len(state->workers) < num_workers - 1),
         "Found duplicate workers");
  CHECKM(utarray_len(state->workers) != num_workers,
         "Tried to kill worker that doesn't exist");

  /* Remove the client socket from the event loop so that we don't process the
   * SIGPIPE when the worker is killed. */
  event_loop_remove_file(worker->local_scheduler_state->loop, worker->sock);

  /* If the worker has registered a process ID with us and it's a child
   * process, use it to send a kill signal. */
  if (worker->is_child && worker->pid != 0) {
    kill(worker->pid, SIGKILL);
    if (wait) {
      /* Wait for the process to exit. */
      waitpid(worker->pid, NULL, 0);
    }
    LOG_INFO("Killed worker with pid %d", worker->pid);
  }

  /* Clean up the client socket after killing the worker so that the worker
   * can't receive the SIGPIPE before exiting. */
  close(worker->sock);

  /* Clean up the task in progress. */
  if (worker->task_in_progress) {
    /* TODO(swang): Update the task table to mark the task as lost. */
    free_task(worker->task_in_progress);
  }

  LOG_DEBUG("Killed worker with pid %d", worker->pid);
  free(worker);
}

void free_local_scheduler(local_scheduler_state *state) {
  /* Free the command for starting new workers. */
  if (state->config.start_worker_command != NULL) {
    int i = 0;
    const char *arg = state->config.start_worker_command[i];
    while (arg != NULL) {
      free((void *) arg);
      ++i;
      arg = state->config.start_worker_command[i];
    }
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

  /* Kill any child processes that didn't register as a worker yet. */
  pid_t *worker_pid;
  for (worker_pid = (pid_t *) utarray_front(state->child_pids);
       worker_pid != NULL;
       worker_pid = (pid_t *) utarray_next(state->child_pids, worker_pid)) {
    kill(*worker_pid, SIGKILL);
    waitpid(*worker_pid, NULL, 0);
    LOG_DEBUG("Killed pid %d", *worker_pid);
  }
  utarray_free(state->child_pids);

  /* Free the list of workers and any tasks that are still in progress on those
   * workers. */
  for (local_scheduler_client **worker =
           (local_scheduler_client **) utarray_front(state->workers);
       worker != NULL;
       worker = (local_scheduler_client **) utarray_front(state->workers)) {
    kill_worker(*worker, true);
  }
  utarray_free(state->workers);
  state->workers = NULL;

  /* Free the mapping from the actor ID to the ID of the local scheduler
   * responsible for that actor. */
  actor_map_entry *current_actor_map_entry, *temp_actor_map_entry;
  HASH_ITER(hh, state->actor_mapping, current_actor_map_entry,
            temp_actor_map_entry) {
    HASH_DEL(state->actor_mapping, current_actor_map_entry);
    free(current_actor_map_entry);
  }

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

/**
 * Start a new worker as a child process.
 *
 * @param state The state of the local scheduler.
 * @return Void.
 */
void start_worker(local_scheduler_state *state, actor_id actor_id) {
  /* We can't start a worker if we don't have the path to the worker script. */
  if (state->config.start_worker_command == NULL) {
    LOG_WARN("No valid command to start worker provided. Cannot start worker.");
    return;
  }
  /* Launch the process to create the worker. */
  pid_t pid = fork();
  if (pid != 0) {
    utarray_push_back(state->child_pids, &pid);
    LOG_INFO("Started worker with pid %d", pid);
    return;
  }

  char id_string[ID_STRING_SIZE];
  object_id_to_string(actor_id, id_string, ID_STRING_SIZE);
  /* Figure out how many arguments there are in the start_worker_command. */
  int num_args = 0;
  for (; state->config.start_worker_command[num_args] != NULL; ++num_args) {
  }
  const char **start_actor_worker_command =
      malloc((num_args + 3) * sizeof(const char *));
  for (int i = 0; i < num_args; ++i) {
    start_actor_worker_command[i] = state->config.start_worker_command[i];
  }
  start_actor_worker_command[num_args] = "--actor-id";
  start_actor_worker_command[num_args + 1] = (const char *) id_string;
  start_actor_worker_command[num_args + 2] = NULL;
  /* Try to execute the worker command. Exit if we're not successful. */
  execvp(start_actor_worker_command[0],
         (char *const *) start_actor_worker_command);
  free(start_actor_worker_command);
  free_local_scheduler(state);
  LOG_FATAL("Failed to start worker");
}

/**
 * Parse the command to start a worker. This takes in the command string,
 * splits it into tokens on the space characters, and allocates an array of the
 * tokens, terminated by a NULL pointer.
 *
 * @param command The command string to start a worker.
 * @return A pointer to an array of strings, the tokens in the command string.
 *         The last element is a NULL pointer.
 */
const char **parse_command(const char *command) {
  /* Count the number of tokens. */
  char *command_copy = strdup(command);
  const char *delimiter = " ";
  char *token = NULL;
  int num_args = 0;
  token = strtok(command_copy, delimiter);
  while (token != NULL) {
    ++num_args;
    token = strtok(NULL, delimiter);
  }
  free(command_copy);

  /* Allocate a NULL-terminated array for the tokens. */
  const char **command_args = malloc((num_args + 1) * sizeof(const char *));
  command_args[num_args] = NULL;

  /* Fill in the token array. */
  command_copy = strdup(command);
  token = strtok(command_copy, delimiter);
  int i = 0;
  while (token != NULL) {
    command_args[i] = strdup(token);
    ++i;
    token = strtok(NULL, delimiter);
  }
  free(command_copy);

  CHECK(num_args == i);
  return command_args;
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
    const double static_resource_conf[],
    const char *start_worker_command,
    int num_workers) {
  local_scheduler_state *state = malloc(sizeof(local_scheduler_state));
  /* Set the configuration struct for the local scheduler. */
  if (start_worker_command != NULL) {
    state->config.start_worker_command = parse_command(start_worker_command);
  } else {
    state->config.start_worker_command = NULL;
  }
  if (start_worker_command == NULL) {
    LOG_WARN(
        "No valid command to start a worker provided, local scheduler will not "
        "start any workers.");
  }
  state->config.global_scheduler_exists = global_scheduler_exists;

  state->loop = loop;
  /* Initialize the list of workers. */
  utarray_new(state->workers, &workers_icd);
  /* Initialize the hash table mapping actor ID to the ID of the local scheduler
   * that is responsible for that actor. */
  state->actor_mapping = NULL;
  /* Connect to Redis if a Redis address is provided. */
  if (redis_addr != NULL) {
    int num_args;
    const char **db_connect_args = NULL;
    /* Use UT_string to convert the resource value into a string. */
    UT_string *num_cpus;
    UT_string *num_gpus;
    utstring_new(num_cpus);
    utstring_new(num_gpus);
    utstring_printf(num_cpus, "%f", static_resource_conf[0]);
    utstring_printf(num_gpus, "%f", static_resource_conf[1]);
    if (plasma_manager_address != NULL) {
      num_args = 8;
      db_connect_args = malloc(sizeof(char *) * num_args);
      db_connect_args[0] = "local_scheduler_socket_name";
      db_connect_args[1] = local_scheduler_socket_name;
      db_connect_args[2] = "num_cpus";
      db_connect_args[3] = utstring_body(num_cpus);
      db_connect_args[4] = "num_gpus";
      db_connect_args[5] = utstring_body(num_gpus);
      db_connect_args[6] = "aux_address";
      db_connect_args[7] = plasma_manager_address;
    } else {
      num_args = 6;
      db_connect_args = malloc(sizeof(char *) * num_args);
      db_connect_args[0] = "local_scheduler_socket_name";
      db_connect_args[1] = local_scheduler_socket_name;
      db_connect_args[2] = "num_cpus";
      db_connect_args[3] = utstring_body(num_cpus);
      db_connect_args[4] = "num_gpus";
      db_connect_args[5] = utstring_body(num_gpus);
    }
    state->db = db_connect(redis_addr, redis_port, "photon", node_ip_address,
                           num_args, db_connect_args);
    utstring_free(num_cpus);
    utstring_free(num_gpus);
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

  /* Start the initial set of workers. */
  utarray_new(state->child_pids, &pid_t_icd);
  for (int i = 0; i < num_workers; ++i) {
    start_worker(state, NIL_ACTOR_ID);
  }

  return state;
}

void update_dynamic_resources(local_scheduler_state *state,
                              task_spec *spec,
                              bool return_resources) {
  for (int i = 0; i < MAX_RESOURCE_INDEX; ++i) {
    double resource = task_spec_get_required_resource(spec, i);
    if (!return_resources) {
      /* If we are not returning resources, we are leasing them, so we want to
       * subtract the resource quantities from our accounting. */
      resource *= -1;
    }
    /* Add or subtract the task's resources from our count. */
    state->dynamic_resources[i] += resource;

    if (!return_resources && state->dynamic_resources[i] < 0) {
      /* We are using more resources than we have been allocated. */
      LOG_WARN("photon dynamic resources dropped to %8.4f\t%8.4f\n",
               state->dynamic_resources[0], state->dynamic_resources[1]);
    }
    CHECK(state->dynamic_resources[i] <= state->static_resources[i]);
  }
  print_resource_info(state, spec);
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
  update_dynamic_resources(state, spec, false);
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
  /* If the task is an actor task, then we currently do not reconstruct it.
   * TODO(rkn): Handle this better. */
  CHECK(actor_ids_equal(task_spec_actor_id(spec), NIL_ACTOR_ID));
  /* Resubmit the task. */
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
    if (actor_ids_equal(task_spec_actor_id(spec), NIL_ACTOR_ID)) {
      handle_task_submitted(state, state->algorithm_state, spec);
    } else {
      handle_actor_task_submitted(state, state->algorithm_state, spec);
    }

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
  case REGISTER_WORKER_INFO: {
    /* Update the actor mapping with the actor ID of the worker (if an actor is
     * running on the worker). */
    register_worker_info *info =
        (register_worker_info *) utarray_front(state->input_buffer);
    if (!actor_ids_equal(info->actor_id, NIL_ACTOR_ID)) {
      /* Make sure that the local scheduler is aware that it is responsible for
       * this actor. */
      actor_map_entry *entry;
      HASH_FIND(hh, state->actor_mapping, &info->actor_id,
                sizeof(info->actor_id), entry);
      CHECK(entry != NULL);
      CHECK(db_client_ids_equal(entry->local_scheduler_id,
                                get_db_client_id(state->db)));
      /* Update the worker struct with this actor ID. */
      CHECK(actor_ids_equal(worker->actor_id, NIL_ACTOR_ID));
      worker->actor_id = info->actor_id;
      /* Let the scheduling algorithm process the presence of this new
       * worker. */
      handle_actor_worker_connect(state, state->algorithm_state, info->actor_id,
                                  worker);
    }

    /* Register worker process id with the scheduler. */
    worker->pid = info->worker_pid;
    /* Determine if this worker is one of our child processes. */
    LOG_DEBUG("PID is %d", info->worker_pid);
    pid_t *child_pid;
    int index = 0;
    for (child_pid = (pid_t *) utarray_front(state->child_pids);
         child_pid != NULL;
         child_pid = (pid_t *) utarray_next(state->child_pids, child_pid)) {
      if (*child_pid == info->worker_pid) {
        /* If this worker is one of our child processes, mark it as a child so
         * that we know that we can wait for the process to exit during
         * cleanup. */
        worker->is_child = true;
        utarray_erase(state->child_pids, index, 1);
        LOG_DEBUG("Found matching child pid %d", info->worker_pid);
        break;
      }
      ++index;
    }
  } break;
  case GET_TASK: {
    /* If this worker reports a completed task: account for resources. */
    if (worker->task_in_progress != NULL) {
      task_spec *spec = task_task_spec(worker->task_in_progress);
      /* Return dynamic resources back for the task in progress. */
      update_dynamic_resources(state, spec, true);
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
    if (actor_ids_equal(worker->actor_id, NIL_ACTOR_ID)) {
      handle_worker_available(state, state->algorithm_state, worker);
    } else {
      handle_actor_worker_available(state, state->algorithm_state, worker);
    }
  } break;
  case RECONSTRUCT_OBJECT: {
    if (worker->task_in_progress != NULL && !worker->is_blocked) {
      /* TODO(swang): For now, we don't handle blocked actors. */
      if (actor_ids_equal(worker->actor_id, NIL_ACTOR_ID)) {
        /* If the worker was executing a task (i.e. non-driver) and it wasn't
         * already blocked on an object that's not locally available, update its
         * state to blocked. */
        handle_worker_blocked(state, state->algorithm_state, worker);
        print_worker_info("Reconstructing", state->algorithm_state);
      }
    }
    object_id *obj_id = (object_id *) utarray_front(state->input_buffer);
    reconstruct_object(state, *obj_id);
  } break;
  case DISCONNECT_CLIENT: {
    LOG_INFO("Disconnecting client on fd %d", client_sock);
    kill_worker(worker, false);
    if (!actor_ids_equal(worker->actor_id, NIL_ACTOR_ID)) {
      /* Let the scheduling algorithm process the absence of this worker. */
      handle_actor_worker_disconnect(state, state->algorithm_state,
                                     worker->actor_id);
    }
  } break;
  case LOG_MESSAGE: {
  } break;
  case NOTIFY_UNBLOCKED: {
    if (worker->task_in_progress != NULL) {
      /* TODO(swang): For now, we don't handle blocked actors. */
      if (actor_ids_equal(worker->actor_id, NIL_ACTOR_ID)) {
        /* If the worker was executing a task (i.e. non-driver), update its
         * state to not blocked. */
        CHECK(worker->is_blocked);
        handle_worker_unblocked(state, state->algorithm_state, worker);
      }
    }
    print_worker_info("Worker unblocked", state->algorithm_state);
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
  worker->is_blocked = false;
  worker->pid = 0;
  worker->is_child = false;
  worker->actor_id = NIL_ACTOR_ID;
  worker->local_scheduler_state = state;
  utarray_push_back(state->workers, &worker);
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message,
                      worker);
  LOG_DEBUG("new connection with fd %d", new_socket);
}

/* We need this code so we can clean up when we get a SIGTERM signal. */

local_scheduler_state *g_state;

void signal_handler(int signal) {
  LOG_DEBUG("Signal was %d", signal);
  if (signal == SIGTERM) {
    free_local_scheduler(g_state);
    exit(0);
  }
}

/* End of the cleanup code. */

void handle_task_scheduled_callback(task *original_task, void *user_context) {
  task_spec *spec = task_task_spec(original_task);
  if (actor_ids_equal(task_spec_actor_id(spec), NIL_ACTOR_ID)) {
    /* This task does not involve an actor. Handle it normally. */
    handle_task_scheduled(g_state, g_state->algorithm_state, spec);
  } else {
    /* This task involves an actor. Call the scheduling algorithm's actor
     * handler. */
    handle_actor_task_scheduled(g_state, g_state->algorithm_state, spec);
  }
}

/**
 * Process a notification about the creation of a new actor. Use this to update
 * the mapping from actor ID to the local scheduler ID of the local scheduler
 * that is responsible for the actor. If this local scheduler is responsible for
 * the actor, then launch a new worker process to create that actor.
 *
 * @param actor_id The ID of the actor being created.
 * @param local_scheduler_id The ID of the local scheduler that is responsible
 *        for creating the actor.
 * @return Void.
 */
void handle_actor_creation_callback(actor_info info, void *context) {
  actor_id actor_id = info.actor_id;
  db_client_id local_scheduler_id = info.local_scheduler_id;
  local_scheduler_state *state = context;
  /* Make sure the actor entry is not already present in the actor map table.
   * TODO(rkn): We will need to remove this check to handle the case where the
   * corresponding publish is retried and the case in which a task that creates
   * an actor is resubmitted due to fault tolerance. */
  actor_map_entry *entry;
  HASH_FIND(hh, state->actor_mapping, &actor_id, sizeof(actor_id), entry);
  CHECK(entry == NULL);
  /* Create a new entry and add it to the actor mapping table. TODO(rkn):
   * Currently this is never removed (except when the local scheduler state is
   * deleted). */
  entry = malloc(sizeof(actor_map_entry));
  entry->actor_id = actor_id;
  entry->local_scheduler_id = local_scheduler_id;
  HASH_ADD(hh, state->actor_mapping, actor_id, sizeof(entry->actor_id), entry);
  /* If this local scheduler is responsible for the actor, then start a new
   * worker for the actor. */
  if (db_client_ids_equal(local_scheduler_id, get_db_client_id(state->db))) {
    start_worker(state, actor_id);
  }
  /* Let the scheduling algorithm process the fact that a new actor has been
   * created. */
  handle_actor_creation_notification(state, state->algorithm_state, actor_id);
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

void start_server(const char *node_ip_address,
                  const char *socket_name,
                  const char *redis_addr,
                  int redis_port,
                  const char *plasma_store_socket_name,
                  const char *plasma_manager_socket_name,
                  const char *plasma_manager_address,
                  bool global_scheduler_exists,
                  const double static_resource_conf[],
                  const char *start_worker_command,
                  int num_workers) {
  /* Ignore SIGPIPE signals. If we don't do this, then when we attempt to write
   * to a client that has already died, the local scheduler could die. */
  signal(SIGPIPE, SIG_IGN);
  int fd = bind_ipc_sock(socket_name, true);
  event_loop *loop = event_loop_create();
  g_state = init_local_scheduler(
      node_ip_address, loop, redis_addr, redis_port, socket_name,
      plasma_store_socket_name, plasma_manager_socket_name,
      plasma_manager_address, global_scheduler_exists, static_resource_conf,
      start_worker_command, num_workers);
  /* Register a callback for registering new clients. */
  event_loop_add_file(loop, fd, EVENT_LOOP_READ, new_client_connection,
                      g_state);
  /* Subscribe to receive notifications about tasks that are assigned to this
   * local scheduler by the global scheduler or by other local schedulers.
   * TODO(rkn): we also need to get any tasks that were assigned to this local
   * scheduler before the call to subscribe. */
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
  /* Subscribe to notifications about newly created actors. */
  if (g_state->db != NULL) {
    actor_notification_table_subscribe(
        g_state->db, handle_actor_creation_callback, g_state, &retry);
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
  /* Comma-separated list of configured resource capabilities for this node. */
  char *static_resource_list = NULL;
  double static_resource_conf[MAX_RESOURCE_INDEX];
  /* The command to run when starting new workers. */
  char *start_worker_command = NULL;
  /* The number of workers to start. */
  char *num_workers_str = NULL;
  int c;
  bool global_scheduler_exists = true;
  while ((c = getopt(argc, argv, "s:r:p:m:ga:h:c:w:n:")) != -1) {
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
    case 'c':
      static_resource_list = optarg;
      break;
    case 'w':
      start_worker_command = optarg;
      break;
    case 'n':
      num_workers_str = optarg;
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
  int num_workers = 0;
  if (num_workers_str) {
    num_workers = strtol(num_workers_str, NULL, 10);
    if (num_workers < 0) {
      LOG_FATAL("Number of workers must be nonnegative");
    }
  }

  char *redis_addr = NULL;
  int redis_port = -1;
  if (!redis_addr_port) {
    /* Start the local scheduler without connecting to Redis. In this case, all
     * submitted tasks will be queued and scheduled locally. */
    if (plasma_manager_socket_name) {
      LOG_FATAL(
          "if a plasma manager socket name is provided with the -m switch, "
          "then a redis address must be provided with the -r switch");
    }
  } else {
    char redis_addr_buffer[16] = {0};
    char redis_port_str[6] = {0};
    /* Parse the Redis address into an IP address and a port. */
    int num_assigned = sscanf(redis_addr_port, "%15[0-9.]:%5[0-9]",
                              redis_addr_buffer, redis_port_str);
    if (num_assigned != 2) {
      LOG_FATAL(
          "if a redis address is provided with the -r switch, it should be "
          "formatted like 127.0.0.1:6379");
    }
    redis_addr = redis_addr_buffer;
    redis_port = strtol(redis_port_str, NULL, 10);
    if (redis_port == 0) {
      LOG_FATAL("Unable to parse port number from redis address %s",
                redis_addr_port);
    }
    if (!plasma_manager_socket_name) {
      LOG_FATAL(
          "please specify socket for connecting to Plasma manager with -m "
          "switch");
    }
  }

  start_server(node_ip_address, scheduler_socket_name, redis_addr, redis_port,
               plasma_store_socket_name, plasma_manager_socket_name,
               plasma_manager_address, global_scheduler_exists,
               static_resource_conf, start_worker_command, num_workers);
}
#endif
