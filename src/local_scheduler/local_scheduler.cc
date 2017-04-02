#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include "common.h"
#include "common_protocol.h"
#include "event_loop.h"
#include "format/local_scheduler_generated.h"
#include "io.h"
#include "logging.h"
#include "local_scheduler_shared.h"
#include "local_scheduler.h"
#include "local_scheduler_algorithm.h"
#include "state/actor_notification_table.h"
#include "state/db.h"
#include "state/task_table.h"
#include "state/object_table.h"
#include "state/error_table.h"
#include "utarray.h"
#include "uthash.h"

UT_icd task_ptr_icd = {sizeof(Task *), NULL, NULL, NULL};
UT_icd workers_icd = {sizeof(LocalSchedulerClient *), NULL, NULL, NULL};

UT_icd pid_t_icd = {sizeof(pid_t), NULL, NULL, NULL};

UT_icd byte_icd = {sizeof(uint8_t), NULL, NULL, NULL};

/**
 * A helper function for printing available and requested resource information.
 *
 * @param state Local scheduler state.
 * @param spec Task specification object.
 * @return Void.
 */
void print_resource_info(const LocalSchedulerState *state,
                         const TaskSpec *spec) {
#if RAY_COMMON_LOG_LEVEL <= RAY_COMMON_DEBUG
  /* Print information about available and requested resources. */
  char buftotal[256], bufavail[256], bufresreq[256];
  snprintf(bufavail, sizeof(bufavail), "%8.4f %8.4f",
           state->dynamic_resources[ResourceIndex_CPU],
           state->dynamic_resources[ResourceIndex_GPU]);
  snprintf(buftotal, sizeof(buftotal), "%8.4f %8.4f",
           state->static_resources[ResourceIndex_CPU],
           state->static_resources[ResourceIndex_GPU]);
  if (spec) {
    snprintf(bufresreq, sizeof(bufresreq), "%8.4f %8.4f",
             task_spec_get_required_resource(spec, ResourceIndex_CPU),
             task_spec_get_required_resource(spec, ResourceIndex_GPU));
  }
  LOG_DEBUG("Resources: [total=%s][available=%s][requested=%s]", buftotal,
            bufavail, spec ? bufresreq : "n/a");
#endif
}

bool check_dynamic_resources(LocalSchedulerState *state,
                             double num_cpus,
                             double num_gpus) {
  if (num_cpus > 0 && state->dynamic_resources[ResourceIndex_CPU] < num_cpus) {
    /* We only use this check when num_cpus is positive so that we can still
     * create actors even when the CPUs are oversubscribed. */
    return false;
  }
  if (state->dynamic_resources[ResourceIndex_GPU] < num_gpus) {
    return false;
  }
  return true;
}

void acquire_resources(LocalSchedulerState *state,
                       LocalSchedulerClient *worker,
                       double num_cpus,
                       double num_gpus) {
  /* Acquire the CPU resources. */
  bool oversubscribed = (state->dynamic_resources[ResourceIndex_CPU] < 0);
  state->dynamic_resources[ResourceIndex_CPU] -= num_cpus;
  CHECK(worker->cpus_in_use == 0);
  worker->cpus_in_use += num_cpus;
  /* Log a warning if we are using more resources than we have been allocated,
   * and we weren't already oversubscribed. */
  if (!oversubscribed && state->dynamic_resources[ResourceIndex_CPU] < 0) {
    LOG_WARN("local_scheduler dynamic resources dropped to %8.4f\t%8.4f\n",
             state->dynamic_resources[0], state->dynamic_resources[1]);
  }

  /* Acquire the GPU resources. */
  if (num_gpus != 0) {
    /* Make sure that the worker isn't using any GPUs already. */
    CHECK(worker->gpus_in_use.size() == 0);
    /* Update the total quantity of GPU resources available. */
    CHECK(state->dynamic_resources[ResourceIndex_GPU] >= num_gpus);
    state->dynamic_resources[ResourceIndex_GPU] -= num_gpus;
    /* Make sure the number of GPUs available is sufficiently large. */
    CHECK(state->available_gpus.size() >= num_gpus);
    /* Reserve GPUs for the worker. */
    for (int i = 0; i < num_gpus; ++i) {
      worker->gpus_in_use.push_back(state->available_gpus.back());
      state->available_gpus.pop_back();
    }
  }
}

void release_resources(LocalSchedulerState *state,
                       LocalSchedulerClient *worker,
                       double num_cpus,
                       double num_gpus) {
  /* Release the CPU resources. */
  CHECK(num_cpus == worker->cpus_in_use);
  state->dynamic_resources[ResourceIndex_CPU] += num_cpus;
  worker->cpus_in_use = 0;

  /* Release the GPU resources. */
  if (num_gpus != 0) {
    CHECK(num_gpus == worker->gpus_in_use.size());
    state->dynamic_resources[ResourceIndex_GPU] += num_gpus;
    /* Move the GPU IDs the worker was using back to the local scheduler. */
    for (auto const &gpu_id : worker->gpus_in_use) {
      state->available_gpus.push_back(gpu_id);
    }
    worker->gpus_in_use.clear();
  }
}

int force_kill_worker(event_loop *loop, timer_id id, void *context) {
  LocalSchedulerClient *worker = (LocalSchedulerClient *) context;
  kill(worker->pid, SIGKILL);
  close(worker->sock);
  delete worker;
  return EVENT_LOOP_TIMER_DONE;
}

/**
 * Kill a worker, if it is a child process, and clean up all of its associated
 * state.
 *
 * @param worker A pointer to the worker we want to kill.
 * @param cleanup A bool representing whether we're cleaning up the entire local
 *        scheduler's state, or just this worker. If true, then the worker will
 *        be force-killed immediately. Else, the worker will be given a chance
 *        to clean up its own state.
 * @return Void.
 */
void kill_worker(LocalSchedulerClient *worker, bool cleanup) {
  /* Erase the local scheduler's reference to the worker. */
  LocalSchedulerState *state = worker->local_scheduler_state;
  int num_workers = utarray_len(state->workers);
  for (int i = 0; i < utarray_len(state->workers); ++i) {
    LocalSchedulerClient *active_worker =
        *(LocalSchedulerClient **) utarray_eltptr(state->workers, i);
    if (active_worker == worker) {
      utarray_erase(state->workers, i, 1);
    }
  }
  /* Make sure that we erased exactly 1 worker. */
  CHECKM(!(utarray_len(state->workers) < num_workers - 1),
         "Found duplicate workers");
  CHECKM(utarray_len(state->workers) != num_workers,
         "Tried to kill worker that doesn't exist");
  /* Erase the algorithm state's reference to the worker. */
  handle_worker_removed(state, state->algorithm_state, worker);

  /* Remove the client socket from the event loop so that we don't process the
   * SIGPIPE when the worker is killed. */
  event_loop_remove_file(worker->local_scheduler_state->loop, worker->sock);

  /* If the worker has registered a process ID with us and it's a child
   * process, use it to send a kill signal. */
  bool free_worker = true;
  if (worker->is_child && worker->pid != 0) {
    if (cleanup) {
      /* If we're exiting the local scheduler anyway, it's okay to force kill
       * the worker immediately. Wait for the process to exit. */
      kill(worker->pid, SIGKILL);
      waitpid(worker->pid, NULL, 0);
      close(worker->sock);
    } else {
      /* If we're just cleaning up a single worker, allow it some time to clean
       * up its state before force killing. The client socket will be closed
       * and the worker struct will be freed after the timeout. */
      kill(worker->pid, SIGTERM);
      event_loop_add_timer(state->loop, KILL_WORKER_TIMEOUT_MILLISECONDS,
                           force_kill_worker, (void *) worker);
      free_worker = false;
    }
    LOG_INFO("Killed worker with pid %d", worker->pid);
  }

  /* Return the resources that the worker was using, if any. */
  release_resources(state, worker, (double) worker->cpus_in_use,
                    (double) worker->gpus_in_use.size());

  /* Clean up the task in progress. */
  if (worker->task_in_progress) {
    /* Update the task table to reflect that the task failed to complete. */
    if (state->db != NULL) {
      Task_set_state(worker->task_in_progress, TASK_STATUS_LOST);
      task_table_update(state->db, worker->task_in_progress, NULL, NULL, NULL);
    } else {
      Task_free(worker->task_in_progress);
    }
  }

  LOG_DEBUG("Killed worker with pid %d", worker->pid);
  if (free_worker) {
    /* Clean up the client socket after killing the worker so that the worker
     * can't receive the SIGPIPE before exiting. */
    close(worker->sock);
    delete worker;
  }
}

void LocalSchedulerState_free(LocalSchedulerState *state) {
  /* Reset the SIGTERM handler to default behavior, so we try to clean up the
   * local scheduler at most once. If a SIGTERM is caught afterwards, there is
   * the possibility of orphan worker processes. */
  signal(SIGTERM, SIG_DFL);

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
  /* TODO(swang): It's possible that the local scheduler will exit before all
   * of its task table updates make it to redis. */
  for (LocalSchedulerClient **worker =
           (LocalSchedulerClient **) utarray_front(state->workers);
       worker != NULL;
       worker = (LocalSchedulerClient **) utarray_front(state->workers)) {
    kill_worker(*worker, true);
  }
  utarray_free(state->workers);
  state->workers = NULL;

  /* Disconnect from the database. */
  if (state->db != NULL) {
    db_disconnect(state->db);
    state->db = NULL;
  }
  /* Disconnect from plasma. */
  plasma_disconnect(state->plasma_conn);
  state->plasma_conn = NULL;

  /* Free the mapping from the actor ID to the ID of the local scheduler
   * responsible for that actor. */
  actor_map_entry *current_actor_map_entry, *temp_actor_map_entry;
  HASH_ITER(hh, state->actor_mapping, current_actor_map_entry,
            temp_actor_map_entry) {
    HASH_DEL(state->actor_mapping, current_actor_map_entry);
    free(current_actor_map_entry);
  }

  /* Free the algorithm state. */
  SchedulingAlgorithmState_free(state->algorithm_state);
  state->algorithm_state = NULL;
  /* Free the input buffer. */
  utarray_free(state->input_buffer);
  state->input_buffer = NULL;
  /* Destroy the event loop. */
  event_loop_destroy(state->loop);
  state->loop = NULL;
  /* Free the scheduler state. */
  delete state;
}

/**
 * Start a new worker as a child process.
 *
 * @param state The state of the local scheduler.
 * @return Void.
 */
void start_worker(LocalSchedulerState *state, ActorID actor_id) {
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
  ObjectID_to_string(actor_id, id_string, ID_STRING_SIZE);
  /* Figure out how many arguments there are in the start_worker_command. */
  int num_args = 0;
  for (; state->config.start_worker_command[num_args] != NULL; ++num_args) {
  }
  const char **start_actor_worker_command =
      (const char **) malloc((num_args + 3) * sizeof(const char *));
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
  LocalSchedulerState_free(state);
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
  const char **command_args =
      (const char **) malloc((num_args + 1) * sizeof(const char *));
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

LocalSchedulerState *LocalSchedulerState_init(
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
  LocalSchedulerState *state = new LocalSchedulerState();
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
      db_connect_args = (const char **) malloc(sizeof(char *) * num_args);
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
      db_connect_args = (const char **) malloc(sizeof(char *) * num_args);
      db_connect_args[0] = "local_scheduler_socket_name";
      db_connect_args[1] = local_scheduler_socket_name;
      db_connect_args[2] = "num_cpus";
      db_connect_args[3] = utstring_body(num_cpus);
      db_connect_args[4] = "num_gpus";
      db_connect_args[5] = utstring_body(num_gpus);
    }
    state->db = db_connect(redis_addr, redis_port, "local_scheduler",
                           node_ip_address, num_args, db_connect_args);
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
  state->algorithm_state = SchedulingAlgorithmState_init();
  /* Add the input buffer. This is used to read in messages from clients without
   * having to reallocate a new buffer every time. */
  utarray_new(state->input_buffer, &byte_icd);

  /* Initialize resource vectors. */
  for (int i = 0; i < ResourceIndex_MAX; i++) {
    state->static_resources[i] = state->dynamic_resources[i] =
        static_resource_conf[i];
  }
  /* Initialize available GPUs. */
  for (int i = 0; i < state->static_resources[ResourceIndex_GPU]; ++i) {
    state->available_gpus.push_back(i);
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

void assign_task_to_worker(LocalSchedulerState *state,
                           TaskSpec *spec,
                           int64_t task_spec_size,
                           LocalSchedulerClient *worker) {
  CHECK(worker != NULL);
  CHECK(ActorID_equal(worker->actor_id, TaskSpec_actor_id(spec)));
  Task *task = Task_alloc(spec, task_spec_size, TASK_STATUS_RUNNING,
                          state->db ? get_db_client_id(state->db) : NIL_ID);
  /* Record which task this worker is executing. This will be freed in
   * process_message when the worker sends a GetTask message to the local
   * scheduler. */
  worker->task_in_progress = Task_copy(task);
  /* Update the global task table. */
  if (state->db != NULL) {
    task_table_update(state->db, task, NULL, NULL, NULL);
  } else {
    Task_free(task);
  }

  /* Acquire the resources necessary to run the task. */
  if (ActorID_equal(worker->actor_id, NIL_ACTOR_ID)) {
    acquire_resources(state, worker,
                      TaskSpec_get_required_resource(spec, ResourceIndex_CPU),
                      TaskSpec_get_required_resource(spec, ResourceIndex_GPU));
  }

  /* Construct the flatbuffer message to send to the worker. */
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      CreateGetTaskReply(fbb, fbb.CreateString((char *) spec, task_spec_size),
                         fbb.CreateVector(worker->gpus_in_use));
  fbb.Finish(message);

  /* Send the message to the local scheduler. */
  if (write_message(worker->sock, MessageType_ExecuteTask, fbb.GetSize(),
                    fbb.GetBufferPointer()) < 0) {
    if (errno == EPIPE || errno == EBADF || errno == ECONNRESET) {
      /* Something went wrong, so kill the worker. */
      kill_worker(worker, false);
      LOG_WARN(
          "Failed to give task to worker on fd %d. The client may have hung "
          "up.",
          worker->sock);
    } else {
      LOG_FATAL("Failed to give task to client on fd %d.", worker->sock);
    }
  }
}

/**
 * Reply to a worker that just registered with the local scheduler. Sending this
 * message confirms the worker's registration with the local scheduler.
 *
 * @param state The state of the local scheduler.
 * @param worker The worker to send message to.
 * @return Void.
 */
void send_register_worker_reply(LocalSchedulerState *state,
                                LocalSchedulerClient *worker) {
  /* Construct the flatbuffer object to send to the worker. */
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      CreateRegisterWorkerReply(fbb, fbb.CreateVector(worker->gpus_in_use));
  fbb.Finish(message);

  /* Send the message to the worker. */
  if (write_message(worker->sock, MessageType_RegisterWorkerReply,
                    fbb.GetSize(), fbb.GetBufferPointer()) < 0) {
    if (errno == EPIPE || errno == EBADF || errno == ECONNRESET) {
      /* Something went wrong, so kill the worker. */
      kill_worker(worker, false);
      LOG_WARN(
          "Failed to give send register worker reply to worker on fd %d. The "
          "client may have hung up.",
          worker->sock);
    } else {
      LOG_FATAL("Failed to send register worker reply to client on fd %d.",
                worker->sock);
    }
  }
}

void process_plasma_notification(event_loop *loop,
                                 int client_sock,
                                 void *context,
                                 int events) {
  LocalSchedulerState *state = (LocalSchedulerState *) context;
  /* Read the notification from Plasma. */
  uint8_t *notification = read_message_async(loop, client_sock);
  if (!notification) {
    /* The store has closed the socket. */
    LocalSchedulerState_free(state);
    LOG_FATAL(
        "Lost connection to the plasma store, local scheduler is exiting!");
  }
  auto object_info = flatbuffers::GetRoot<ObjectInfo>(notification);
  ObjectID object_id = from_flatbuf(object_info->object_id());
  if (object_info->is_deletion()) {
    handle_object_removed(state, object_id);
  } else {
    handle_object_available(state, state->algorithm_state, object_id);
  }
  free(notification);
}

void reconstruct_task_update_callback(Task *task,
                                      void *user_context,
                                      bool updated) {
  /* The task ID should be in the task table. */
  CHECK(task != NULL);
  if (!updated) {
    /* The test-and-set of the task's scheduling state failed, so the task was
     * either not finished yet, or it was already being reconstructed.
     * Suppress the reconstruction request. */
    return;
  }
  /* Otherwise, the test-and-set succeeded, so resubmit the task for execution
   * to ensure that reconstruction will happen. */
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;
  TaskSpec *spec = Task_task_spec(task);
  /* If the task is an actor task, then we currently do not reconstruct it.
   * TODO(rkn): Handle this better. */
  CHECK(ActorID_equal(TaskSpec_actor_id(spec), NIL_ACTOR_ID));
  /* Resubmit the task. */
  handle_task_submitted(state, state->algorithm_state, spec,
                        Task_task_spec_size(task));
  /* Recursively reconstruct the task's inputs, if necessary. */
  for (int64_t i = 0; i < TaskSpec_num_args(spec); ++i) {
    if (TaskSpec_arg_by_ref(spec, i)) {
      ObjectID arg_id = TaskSpec_arg_id(spec, i);
      reconstruct_object(state, arg_id);
    }
  }
}

void reconstruct_put_task_update_callback(Task *task,
                                          void *user_context,
                                          bool updated) {
  CHECK(task != NULL);
  if (updated) {
    /* The update to TASK_STATUS_RECONSTRUCTING succeeded, so continue with
     * reconstruction as usual. */
    reconstruct_task_update_callback(task, user_context, updated);
    return;
  }

  /* An object created by `ray.put` was not able to be reconstructed, and the
   * workload will likely hang. Push an error to the appropriate driver. */
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;
  TaskSpec *spec = Task_task_spec(task);
  FunctionID function = TaskSpec_function(spec);
  push_error(state->db, TaskSpec_driver_id(spec),
             PUT_RECONSTRUCTION_ERROR_INDEX, sizeof(function), function.id);
}

void reconstruct_evicted_result_lookup_callback(ObjectID reconstruct_object_id,
                                                TaskID task_id,
                                                bool is_put,
                                                void *user_context) {
  CHECKM(!IS_NIL_ID(task_id),
         "No task information found for object during reconstruction");
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;

  task_table_test_and_update_callback done_callback;
  if (is_put) {
    /* If the evicted object was created through ray.put and the originating
     * task
     * is still executing, it's very likely that the workload will hang and the
     * worker needs to be restarted. Else, the reconstruction behavior is the
     * same as for other evicted objects */
    done_callback = reconstruct_put_task_update_callback;
  } else {
    done_callback = reconstruct_task_update_callback;
  }
  /* If there are no other instances of the task running, it's safe for us to
   * claim responsibility for reconstruction. */
  task_table_test_and_update(
      state->db, task_id, (TASK_STATUS_DONE | TASK_STATUS_LOST),
      TASK_STATUS_RECONSTRUCTING, NULL, done_callback, state);
}

void reconstruct_failed_result_lookup_callback(ObjectID reconstruct_object_id,
                                               TaskID task_id,
                                               bool is_put,
                                               void *user_context) {
  if (IS_NIL_ID(task_id)) {
    /* NOTE(swang): For some reason, the result table update sometimes happens
     * after this lookup returns, possibly due to concurrent clients. In most
     * cases, this is okay because the initial execution is probably still
     * pending, so for now, we log a warning and suppress reconstruction. */
    LOG_WARN(
        "No task information found for object during reconstruction (no object "
        "entry yet)");
    return;
  }
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;
  /* If the task failed to finish, it's safe for us to claim responsibility for
   * reconstruction. */
  task_table_test_and_update(state->db, task_id, TASK_STATUS_LOST,
                             TASK_STATUS_RECONSTRUCTING, NULL,
                             reconstruct_task_update_callback, state);
}

void reconstruct_object_lookup_callback(ObjectID reconstruct_object_id,
                                        int manager_count,
                                        const char *manager_vector[],
                                        void *user_context) {
  LOG_DEBUG("Manager count was %d", manager_count);
  /* Only continue reconstruction if we find that the object doesn't exist on
   * any nodes. NOTE: This codepath is not responsible for checking if the
   * object table entry is up-to-date. */
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;
  /* Look up the task that created the object in the result table. */
  if (manager_count == 0) {
    /* If the object was created and later evicted, we reconstruct the object
     * if and only if there are no other instances of the task running. */
    result_table_lookup(state->db, reconstruct_object_id, NULL,
                        reconstruct_evicted_result_lookup_callback,
                        (void *) state);
  } else if (manager_count == -1) {
    /* If the object has not been created yet, we reconstruct the object if and
     * only if the task that created the object failed to complete. */
    result_table_lookup(state->db, reconstruct_object_id, NULL,
                        reconstruct_failed_result_lookup_callback,
                        (void *) state);
  }
}

void reconstruct_object(LocalSchedulerState *state,
                        ObjectID reconstruct_object_id) {
  LOG_DEBUG("Starting reconstruction");
  /* TODO(swang): Track task lineage for puts. */
  CHECK(state->db != NULL);
  /* Determine if reconstruction is necessary by checking if the object exists
   * on a node. */
  object_table_lookup(state->db, reconstruct_object_id, NULL,
                      reconstruct_object_lookup_callback, (void *) state);
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  LocalSchedulerClient *worker = (LocalSchedulerClient *) context;
  LocalSchedulerState *state = worker->local_scheduler_state;

  int64_t type;
  int64_t length = read_buffer(client_sock, &type, state->input_buffer);

  LOG_DEBUG("New event of type %" PRId64, type);

  switch (type) {
  case MessageType_SubmitTask: {
    TaskSpec *spec = (TaskSpec *) utarray_front(state->input_buffer);
    /* Update the result table, which holds mappings of object ID -> ID of the
     * task that created it. */
    if (state->db != NULL) {
      TaskID task_id = TaskSpec_task_id(spec);
      for (int64_t i = 0; i < TaskSpec_num_returns(spec); ++i) {
        ObjectID return_id = TaskSpec_return(spec, i);
        result_table_add(state->db, return_id, task_id, false, NULL, NULL,
                         NULL);
      }
    }

    /* Handle the task submission. */
    if (ActorID_equal(TaskSpec_actor_id(spec), NIL_ACTOR_ID)) {
      handle_task_submitted(state, state->algorithm_state, spec, length);
    } else {
      handle_actor_task_submitted(state, state->algorithm_state, spec, length);
    }

  } break;
  case MessageType_TaskDone: {
  } break;
  case MessageType_EventLogMessage: {
    /* Parse the message. */
    auto message = flatbuffers::GetRoot<EventLogMessage>(
        utarray_front(state->input_buffer));
    if (state->db != NULL) {
      RayLogger_log_event(
          state->db, (uint8_t *) message->key()->data(), message->key()->size(),
          (uint8_t *) message->value()->data(), message->value()->size());
    }
  } break;
  case MessageType_RegisterWorkerInfo: {
    /* Update the actor mapping with the actor ID of the worker (if an actor is
     * running on the worker). */
    auto message = flatbuffers::GetRoot<RegisterWorkerInfo>(
        utarray_front(state->input_buffer));
    int64_t worker_pid = message->worker_pid();
    ActorID actor_id = from_flatbuf(message->actor_id());
    if (!ActorID_equal(actor_id, NIL_ACTOR_ID)) {
      /* Make sure that the local scheduler is aware that it is responsible for
       * this actor. */
      actor_map_entry *entry;
      HASH_FIND(hh, state->actor_mapping, &actor_id, sizeof(actor_id), entry);
      CHECK(entry != NULL);
      CHECK(DBClientID_equal(entry->local_scheduler_id,
                             get_db_client_id(state->db)));
      /* Update the worker struct with this actor ID. */
      CHECK(ActorID_equal(worker->actor_id, NIL_ACTOR_ID));
      worker->actor_id = actor_id;
      /* Let the scheduling algorithm process the presence of this new
       * worker. */
      handle_actor_worker_connect(state, state->algorithm_state, actor_id,
                                  worker);

      /* If there are enough GPUs available, allocate them and reply to the
       * actor. */
      double num_gpus_required = (double) message->num_gpus();
      if (check_dynamic_resources(state, 0, num_gpus_required)) {
        acquire_resources(state, worker, 0, num_gpus_required);
      } else {
        /* TODO(rkn): This means that an actor wants to register but that there
         * aren't enough GPUs for it. We should queue this request, and reply to
         * the actor when GPUs become available. */
        LOG_WARN(
            "Attempting to create an actor but there aren't enough available "
            "GPUs. We'll start the worker anyway without any GPUs, but this is "
            "incorrect behavior.");
      }
    }

    /* Send a message confirming the actor's registration. */
    send_register_worker_reply(state, worker);

    /* Register worker process id with the scheduler. */
    worker->pid = worker_pid;
    /* Determine if this worker is one of our child processes. */
    LOG_DEBUG("PID is %d", worker_pid);
    pid_t *child_pid;
    int index = 0;
    for (child_pid = (pid_t *) utarray_front(state->child_pids);
         child_pid != NULL;
         child_pid = (pid_t *) utarray_next(state->child_pids, child_pid)) {
      if (*child_pid == worker_pid) {
        /* If this worker is one of our child processes, mark it as a child so
         * that we know that we can wait for the process to exit during
         * cleanup. */
        worker->is_child = true;
        utarray_erase(state->child_pids, index, 1);
        LOG_DEBUG("Found matching child pid %d", worker_pid);
        break;
      }
      ++index;
    }
  } break;
  case MessageType_GetTask: {
    /* If this worker reports a completed task: account for resources. */
    if (worker->task_in_progress != NULL) {
      /* Return dynamic resources back for the task in progress. If the worker
       * is an actor, then we do not return GPU resources. */
      if (ActorID_equal(worker->actor_id, NIL_ACTOR_ID)) {
        release_resources(state, worker, (double) worker->cpus_in_use,
                          (double) worker->gpus_in_use.size());
      } else {
        /* release_resources(state, worker, (double) worker->cpus_in_use, 0); */
      }

      /* If we're connected to Redis, update tables. */
      if (state->db != NULL) {
        /* Update control state tables. */
        Task_set_state(worker->task_in_progress, TASK_STATUS_DONE);
        task_table_update(state->db, worker->task_in_progress, NULL, NULL,
                          NULL);
        /* The call to task_table_update takes ownership of the
         * task_in_progress, so we set the pointer to NULL so it is not used. */
      } else {
        Task_free(worker->task_in_progress);
      }
      worker->task_in_progress = NULL;
    }
    /* Let the scheduling algorithm process the fact that there is an available
     * worker. */
    if (ActorID_equal(worker->actor_id, NIL_ACTOR_ID)) {
      handle_worker_available(state, state->algorithm_state, worker);
    } else {
      handle_actor_worker_available(state, state->algorithm_state, worker);
    }
  } break;
  case MessageType_ReconstructObject: {
    auto message = flatbuffers::GetRoot<ReconstructObject>(
        utarray_front(state->input_buffer));
    if (worker->task_in_progress != NULL && !worker->is_blocked) {
      /* TODO(swang): For now, we don't handle blocked actors. */
      if (ActorID_equal(worker->actor_id, NIL_ACTOR_ID)) {
        /* If the worker was executing a task (i.e. non-driver) and it wasn't
         * already blocked on an object that's not locally available, release
         * it's resources and update its state to blocked. Note, we do not
         * return GPU resources because the worker will still be using memory on
         * the GPUs. */
        TaskSpec *spec = Task_task_spec(worker->task_in_progress);
        CHECK((double) worker->cpus_in_use ==
              TaskSpec_get_required_resource(spec, ResourceIndex_CPU));
        release_resources(
            state, worker,
            TaskSpec_get_required_resource(spec, ResourceIndex_CPU), 0);

        handle_worker_blocked(state, state->algorithm_state, worker);
        print_worker_info("Reconstructing", state->algorithm_state);
      }
    }
    reconstruct_object(state, from_flatbuf(message->object_id()));
  } break;
  case DISCONNECT_CLIENT: {
    LOG_INFO("Disconnecting client on fd %d", client_sock);
    kill_worker(worker, false);
    if (!ActorID_equal(worker->actor_id, NIL_ACTOR_ID)) {
      /* Let the scheduling algorithm process the absence of this worker. */
      handle_actor_worker_disconnect(state, state->algorithm_state,
                                     worker->actor_id);
    }
  } break;
  case MessageType_NotifyUnblocked: {
    if (worker->task_in_progress != NULL) {
      /* TODO(swang): For now, we don't handle blocked actors. */
      if (ActorID_equal(worker->actor_id, NIL_ACTOR_ID)) {
        /* If the worker was executing a task (i.e. non-driver), update its
         * state to not blocked. */
        CHECK(worker->is_blocked);

        /* Lease back the resources that the blocked worker will need. However,
         * do not return GPU resources because the blocked worker will still be
         * using memory on the GPUs. */
        /* TODO(swang): Leasing back the resources to blocked workers can cause
         * us to transiently exceed the maximum number of resources. This can be
         * fixed by having blocked workers explicitly yield and wait to be given
         * back resources before continuing execution. */
        TaskSpec *spec = Task_task_spec(worker->task_in_progress);
        acquire_resources(
            state, worker,
            TaskSpec_get_required_resource(spec, ResourceIndex_CPU), 0);

        handle_worker_unblocked(state, state->algorithm_state, worker);
      }
    }
    print_worker_info("Worker unblocked", state->algorithm_state);
  } break;
  case MessageType_PutObject: {
    auto message =
        flatbuffers::GetRoot<PutObject>(utarray_front(state->input_buffer));
    result_table_add(state->db, from_flatbuf(message->object_id()),
                     from_flatbuf(message->task_id()), true, NULL, NULL, NULL);
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
  LocalSchedulerState *state = (LocalSchedulerState *) context;
  int new_socket = accept_client(listener_sock);
  /* Create a struct for this worker. This will be freed when we free the local
   * scheduler state. */
  LocalSchedulerClient *worker = new LocalSchedulerClient();
  worker->sock = new_socket;
  worker->task_in_progress = NULL;
  worker->cpus_in_use = 0;
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

LocalSchedulerState *g_state;

void signal_handler(int signal) {
  LOG_DEBUG("Signal was %d", signal);
  if (signal == SIGTERM) {
    /* NOTE(swang): This call removes the SIGTERM handler to ensure that we
     * free the local scheduler state at most once. If another SIGTERM is
     * caught during this call, there is the possibility of orphan worker
     * processes. */
    LocalSchedulerState_free(g_state);
    exit(0);
  }
}

/* End of the cleanup code. */

void handle_task_scheduled_callback(Task *original_task, void *user_context) {
  TaskSpec *spec = Task_task_spec(original_task);
  if (ActorID_equal(TaskSpec_actor_id(spec), NIL_ACTOR_ID)) {
    /* This task does not involve an actor. Handle it normally. */
    handle_task_scheduled(g_state, g_state->algorithm_state, spec,
                          Task_task_spec_size(original_task));
  } else {
    /* This task involves an actor. Call the scheduling algorithm's actor
     * handler. */
    handle_actor_task_scheduled(g_state, g_state->algorithm_state, spec,
                                Task_task_spec_size(original_task));
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
void handle_actor_creation_callback(ActorInfo info, void *context) {
  ActorID actor_id = info.actor_id;
  DBClientID local_scheduler_id = info.local_scheduler_id;
  LocalSchedulerState *state = (LocalSchedulerState *) context;
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
  entry = (actor_map_entry *) malloc(sizeof(actor_map_entry));
  entry->actor_id = actor_id;
  entry->local_scheduler_id = local_scheduler_id;
  HASH_ADD(hh, state->actor_mapping, actor_id, sizeof(entry->actor_id), entry);
  /* If this local scheduler is responsible for the actor, then start a new
   * worker for the actor. */
  if (DBClientID_equal(local_scheduler_id, get_db_client_id(state->db))) {
    start_worker(state, actor_id);
  }
  /* Let the scheduling algorithm process the fact that a new actor has been
   * created. */
  handle_actor_creation_notification(state, state->algorithm_state, actor_id);
}

int heartbeat_handler(event_loop *loop, timer_id id, void *context) {
  LocalSchedulerState *state = (LocalSchedulerState *) context;
  SchedulingAlgorithmState *algorithm_state = state->algorithm_state;
  LocalSchedulerInfo info;
  /* Ask the scheduling algorithm to fill out the scheduler info struct. */
  provide_scheduler_info(state, algorithm_state, &info);
  /* Publish the heartbeat to all subscribers of the local scheduler table. */
  local_scheduler_table_send_info(state->db, &info, NULL);
  /* Reset the timer. */
  return HEARTBEAT_TIMEOUT_MILLISECONDS;
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
  g_state = LocalSchedulerState_init(
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
  if (g_state->db != NULL) {
    task_table_subscribe(g_state->db, get_db_client_id(g_state->db),
                         TASK_STATUS_SCHEDULED, handle_task_scheduled_callback,
                         NULL, NULL, NULL, NULL);
  }
  /* Subscribe to notifications about newly created actors. */
  if (g_state->db != NULL) {
    actor_notification_table_subscribe(
        g_state->db, handle_actor_creation_callback, g_state, NULL);
  }
  /* Create a timer for publishing information about the load on the local
   * scheduler to the local scheduler table. This message also serves as a
   * heartbeat. */
  if (g_state->db != NULL) {
    event_loop_add_timer(loop, HEARTBEAT_TIMEOUT_MILLISECONDS,
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
#ifndef LOCAL_SCHEDULER_TEST
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
  /* Address for the plasma manager associated with this local scheduler
   * instance. */
  char *plasma_manager_address = NULL;
  /* The IP address of the node that this local scheduler is running on. */
  char *node_ip_address = NULL;
  /* Comma-separated list of configured resource capabilities for this node. */
  char *static_resource_list = NULL;
  double static_resource_conf[ResourceIndex_MAX];
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
    static_resource_conf[ResourceIndex_CPU] = DEFAULT_NUM_CPUS;
    static_resource_conf[ResourceIndex_GPU] = DEFAULT_NUM_GPUS;
  } else {
    /* Tokenize the string. */
    const char delim[2] = ",";
    char *token;
    int idx = 0; /* Index into the resource vector. */
    token = strtok(static_resource_list, delim);
    while (token != NULL && idx < ResourceIndex_MAX) {
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
