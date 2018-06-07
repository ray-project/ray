#include <sstream>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>

#include "common.h"
#include "common_protocol.h"
#include "event_loop.h"
#include "format/local_scheduler_generated.h"
#include "io.h"
#include "logging.h"
#include "local_scheduler_shared.h"
#include "local_scheduler.h"
#include "local_scheduler_algorithm.h"
#include "net.h"
#include "state/actor_notification_table.h"
#include "state/db.h"
#include "state/db_client_table.h"
#include "state/driver_table.h"
#include "state/task_table.h"
#include "state/object_table.h"
#include "state/error_table.h"

using MessageType = ray::local_scheduler::protocol::MessageType;

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
  // Print information about available and requested resources.
  std::cout << "Static Resources: " << std::endl;
  for (auto const &resource_pair : state->static_resources) {
    std::cout << "    " << resource_pair.first << ": " << resource_pair.second
              << std::endl;
  }
  std::cout << "Dynamic Resources: " << std::endl;
  for (auto const &resource_pair : state->dynamic_resources) {
    std::cout << "    " << resource_pair.first << ": " << resource_pair.second
              << std::endl;
  }
  if (spec) {
    std::cout << "Task Required Resources: " << std::endl;
    for (auto const &resource_pair : TaskSpec_get_required_resources(spec)) {
      std::cout << "    " << resource_pair.first << ": " << resource_pair.second
                << std::endl;
    }
  }
#endif
}

int force_kill_worker(event_loop *loop, timer_id id, void *context) {
  LocalSchedulerClient *worker = (LocalSchedulerClient *) context;
  kill(worker->pid, SIGKILL);
  close(worker->sock);
  delete worker;
  return EVENT_LOOP_TIMER_DONE;
}

void kill_worker(LocalSchedulerState *state,
                 LocalSchedulerClient *worker,
                 bool cleanup,
                 bool suppress_warning) {
  /* Erase the local scheduler's reference to the worker. */
  auto it = std::find(state->workers.begin(), state->workers.end(), worker);
  RAY_CHECK(it != state->workers.end());
  state->workers.erase(it);

  /* Make sure that we removed the worker. */
  it = std::find(state->workers.begin(), state->workers.end(), worker);
  RAY_CHECK(it == state->workers.end());

  /* Release any resources held by the worker. It's important to do this before
   * calling handle_worker_removed and handle_actor_worker_disconnect because
   * freeing up resources here will allow the scheduling algorithm to dispatch
   * more tasks. */
  release_resources(state, worker, worker->resources_in_use);

  /* Erase the algorithm state's reference to the worker. */
  if (worker->actor_id.is_nil()) {
    handle_worker_removed(state, state->algorithm_state, worker);
  } else {
    /* Let the scheduling algorithm process the absence of this worker. */
    handle_actor_worker_disconnect(state, state->algorithm_state, worker,
                                   cleanup);
  }

  /* Remove the client socket from the event loop so that we don't process the
   * SIGPIPE when the worker is killed. */
  event_loop_remove_file(state->loop, worker->sock);

  /* If the worker has registered a process ID with us and it's a child
   * process, use it to send a kill signal. */
  bool free_worker = true;
  if (worker->is_child && worker->pid != 0) {
    /* If worker is a driver, we should not enter this condition because
     * worker->pid should be 0. */
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
      event_loop_add_timer(
          state->loop, RayConfig::instance().kill_worker_timeout_milliseconds(),
          force_kill_worker, (void *) worker);
      free_worker = false;
    }
    RAY_LOG(DEBUG) << "Killed worker with pid " << worker->pid;
  }

  /* If this worker is still running a task and we aren't cleaning up, push an
   * error message to the driver responsible for the task. */
  if (worker->task_in_progress != NULL && !cleanup && !suppress_warning) {
    TaskSpec *spec = Task_task_execution_spec(worker->task_in_progress)->Spec();

    std::ostringstream error_message;
    error_message << "The worker with ID " << worker->client_id << " died or "
                  << "was killed while executing the task with ID "
                  << TaskSpec_task_id(spec);
    push_error(state->db, TaskSpec_driver_id(spec), ErrorIndex::WORKER_DIED,
               error_message.str());
  }

  /* Clean up the task in progress. */
  if (worker->task_in_progress) {
    /* Update the task table to reflect that the task failed to complete. */
    if (state->db != NULL) {
      Task_set_state(worker->task_in_progress, TaskStatus::LOST);
#if !RAY_USE_NEW_GCS
      task_table_update(state->db, worker->task_in_progress, NULL, NULL, NULL);
#else
      RAY_CHECK_OK(TaskTableAdd(&state->gcs_client, worker->task_in_progress));
      Task_free(worker->task_in_progress);
#endif
    } else {
      Task_free(worker->task_in_progress);
    }
  }

  RAY_LOG(DEBUG) << "Killed worker with pid " << worker->pid;
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
  /* Send a null heartbeat that tells the global scheduler that we are dead to
   * avoid waiting for the heartbeat timeout. */
  if (state->db != NULL) {
    local_scheduler_table_disconnect(state->db);
  }

  /* Kill any child processes that didn't register as a worker yet. */
  for (auto const &worker_pid : state->child_pids) {
    kill(worker_pid, SIGKILL);
    waitpid(worker_pid, NULL, 0);
    RAY_LOG(INFO) << "Killed worker pid " << worker_pid
                  << " which hadn't started yet.";
  }

  /* Kill any registered workers. */
  /* TODO(swang): It's possible that the local scheduler will exit before all
   * of its task table updates make it to redis. */
  while (state->workers.size() > 0) {
    /* Note that kill_worker modifies the container state->workers, so it is
     * important to do this loop in a way that does not use invalidated
     * iterators. */
    kill_worker(state, state->workers.back(), true, false);
  }

  /* Disconnect from plasma. */
  ARROW_CHECK_OK(state->plasma_conn->Disconnect());
  delete state->plasma_conn;
  state->plasma_conn = NULL;

  /* Clean up the database connection. NOTE(swang): The global scheduler is
   * responsible for deleting our entry from the db_client table, so do not
   * delete it here. */
  if (state->db != NULL) {
    DBHandle_free(state->db);
  }

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

  /* Free the algorithm state. */
  SchedulingAlgorithmState_free(state->algorithm_state);
  state->algorithm_state = NULL;

  event_loop *loop = state->loop;

  /* Free the scheduler state. */
  delete state;

  /* Destroy the event loop. */
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
}

void start_worker(LocalSchedulerState *state) {
  /* We can't start a worker if we don't have the path to the worker script. */
  if (state->config.start_worker_command == NULL) {
    RAY_LOG(DEBUG) << "No valid command to start worker provided. Cannot start "
                   << "worker.";
    return;
  }
  /* Launch the process to create the worker. */
  pid_t pid = fork();
  if (pid != 0) {
    state->child_pids.push_back(pid);
    RAY_LOG(DEBUG) << "Started worker with pid " << pid;
    return;
  }

  /* Reset the SIGCHLD handler so that it doesn't influence the worker. */
  signal(SIGCHLD, SIG_DFL);

  std::vector<const char *> command_vector;
  for (int i = 0; state->config.start_worker_command[i] != NULL; i++) {
    command_vector.push_back(state->config.start_worker_command[i]);
  }

  /* Add a NULL pointer to the end. */
  command_vector.push_back(NULL);

  /* Try to execute the worker command. Exit if we're not successful. */
  execvp(command_vector[0], (char *const *) command_vector.data());

  LocalSchedulerState_free(state);
  RAY_LOG(FATAL) << "Failed to start worker";
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

  RAY_CHECK(num_args == i);
  return command_args;
}

LocalSchedulerState *LocalSchedulerState_init(
    const char *node_ip_address,
    event_loop *loop,
    const char *redis_primary_addr,
    int redis_primary_port,
    const char *local_scheduler_socket_name,
    const char *plasma_store_socket_name,
    const char *plasma_manager_socket_name,
    const char *plasma_manager_address,
    bool global_scheduler_exists,
    const std::unordered_map<std::string, double> &static_resource_conf,
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
    RAY_LOG(WARNING) << "No valid command to start a worker provided, local "
                     << "scheduler will not start any workers.";
  }
  state->config.global_scheduler_exists = global_scheduler_exists;

  state->loop = loop;

  /* Connect to Redis if a Redis address is provided. */
  if (redis_primary_addr != NULL) {
    /* Construct db_connect_args */
    std::vector<std::string> db_connect_args;
    db_connect_args.push_back("local_scheduler_socket_name");
    db_connect_args.push_back(local_scheduler_socket_name);
    for (auto const &resource_pair : static_resource_conf) {
      // TODO(rkn): This could cause issues if a resource name collides with
      // another field name "manager_address".
      db_connect_args.push_back(resource_pair.first);
      db_connect_args.push_back(std::to_string(resource_pair.second));
    }

    if (plasma_manager_address != NULL) {
      db_connect_args.push_back("manager_address");
      db_connect_args.push_back(plasma_manager_address);
    }

    state->db = db_connect(std::string(redis_primary_addr), redis_primary_port,
                           "local_scheduler", node_ip_address, db_connect_args);
    db_attach(state->db, loop, false);

    RAY_CHECK_OK(state->gcs_client.Connect(std::string(redis_primary_addr),
                                           redis_primary_port));
    RAY_CHECK_OK(state->gcs_client.context()->AttachToEventLoop(loop));
  } else {
    state->db = NULL;
  }
  /* Connect to Plasma. This method will retry if Plasma hasn't started yet. */
  state->plasma_conn = new plasma::PlasmaClient();
  if (plasma_manager_socket_name != NULL) {
    ARROW_CHECK_OK(state->plasma_conn->Connect(
        plasma_store_socket_name, plasma_manager_socket_name,
        plasma::kPlasmaDefaultReleaseDelay));
  } else {
    ARROW_CHECK_OK(state->plasma_conn->Connect(
        plasma_store_socket_name, "", plasma::kPlasmaDefaultReleaseDelay));
  }
  /* Subscribe to notifications about sealed objects. */
  int plasma_fd;
  ARROW_CHECK_OK(state->plasma_conn->Subscribe(&plasma_fd));
  /* Add the callback that processes the notification to the event loop. */
  event_loop_add_file(loop, plasma_fd, EVENT_LOOP_READ,
                      process_plasma_notification, state);
  /* Add scheduler state. */
  state->algorithm_state = SchedulingAlgorithmState_init();

  /* Initialize resource vectors. */
  state->static_resources = static_resource_conf;
  state->dynamic_resources = static_resource_conf;
  /* Initialize available GPUs. */
  if (state->static_resources.count("GPU") == 1) {
    for (int i = 0; i < state->static_resources["GPU"]; ++i) {
      state->available_gpus.push_back(i);
    }
  }
  /* Print some debug information about resource configuration. */
  print_resource_info(state, NULL);

  /* Start the initial set of workers. */
  for (int i = 0; i < num_workers; ++i) {
    start_worker(state);
  }

  /* Initialize the time at which the previous heartbeat was sent. */
  state->previous_heartbeat_time = current_time_ms();

  return state;
}

/* TODO(atumanov): vectorize resource counts on input. */
bool check_dynamic_resources(
    LocalSchedulerState *state,
    const std::unordered_map<std::string, double> &resources) {
  for (auto const &resource_pair : resources) {
    std::string resource_name = resource_pair.first;
    double resource_quantity = resource_pair.second;
    if (state->dynamic_resources[resource_name] < resource_quantity) {
      return false;
    }
  }
  return true;
}

void resource_sanity_checks(LocalSchedulerState *state,
                            LocalSchedulerClient *worker) {
  // Check the resources in use by the worker.
  for (auto const &resource_pair : worker->resources_in_use) {
    const std::string resource_name = resource_pair.first;
    double resource_quantity = resource_pair.second;

    RAY_CHECK(state->dynamic_resources[resource_name] <=
              state->static_resources[resource_name]);
    if (resource_name != std::string("CPU")) {
      RAY_CHECK(state->dynamic_resources[resource_name] >= 0);
    }

    RAY_CHECK(resource_quantity >= 0);
    RAY_CHECK(resource_quantity <= state->static_resources[resource_name]);
  }
}

/* TODO(atumanov): just pass the required resource vector of doubles. */
void acquire_resources(
    LocalSchedulerState *state,
    LocalSchedulerClient *worker,
    const std::unordered_map<std::string, double> &resources) {
  // Loop over each required resource type and acquire the appropriate quantity.
  for (auto const &resource_pair : resources) {
    const std::string resource_name = resource_pair.first;
    double resource_quantity = resource_pair.second;

    // Do some special handling for GPU resources.
    if (resource_name == std::string("GPU")) {
      if (resource_quantity != 0) {
        // Make sure that the worker isn't using any GPUs already.
        RAY_CHECK(worker->gpus_in_use.size() == 0);
        RAY_CHECK(state->available_gpus.size() >= resource_quantity);
        // Reserve GPUs for the worker.
        for (int i = 0; i < resource_quantity; i++) {
          worker->gpus_in_use.push_back(state->available_gpus.back());
          state->available_gpus.pop_back();
        }
      }
    }

    // Do bookkeeping for general resource types.
    if (resource_name != std::string("CPU")) {
      RAY_CHECK(state->dynamic_resources[resource_name] >= resource_quantity);
    }
    state->dynamic_resources[resource_name] -= resource_quantity;
    worker->resources_in_use[resource_name] += resource_quantity;
  }

  // Do some sanity checks.
  resource_sanity_checks(state, worker);
}

void release_resources(
    LocalSchedulerState *state,
    LocalSchedulerClient *worker,
    const std::unordered_map<std::string, double> &resources) {
  for (auto const &resource_pair : resources) {
    const std::string resource_name = resource_pair.first;
    double resource_quantity = resource_pair.second;

    // Do some special handling for GPU resources.
    if (resource_name == std::string("GPU")) {
      if (resource_quantity != 0) {
        RAY_CHECK(resource_quantity == worker->gpus_in_use.size());
        // Move the GPU IDs the worker was using back to the local scheduler.
        for (auto const &gpu_id : worker->gpus_in_use) {
          state->available_gpus.push_back(gpu_id);
        }
        worker->gpus_in_use.clear();
      }
    }

    // Do bookkeeping for general resources types.
    state->dynamic_resources[resource_name] += resource_quantity;
    worker->resources_in_use[resource_name] -= resource_quantity;
  }

  // Do some sanity checks.
  resource_sanity_checks(state, worker);
}

bool is_driver_alive(LocalSchedulerState *state, WorkerID driver_id) {
  return state->removed_drivers.count(driver_id) == 0;
}

void assign_task_to_worker(LocalSchedulerState *state,
                           TaskExecutionSpec &execution_spec,
                           LocalSchedulerClient *worker) {
  int64_t task_spec_size = execution_spec.SpecSize();
  TaskSpec *spec = execution_spec.Spec();
  // Acquire the necessary resources for running this task.
  const std::unordered_map<std::string, double> required_resources =
      TaskSpec_get_required_resources(spec);
  acquire_resources(state, worker, required_resources);
  // Check that actor tasks don't have non-CPU requirements. Any necessary
  // non-CPU resources (in particular, GPUs) should already have been acquired
  // by the actor worker.
  if (!worker->actor_id.is_nil()) {
    RAY_CHECK(required_resources.size() == 1);
    RAY_CHECK(required_resources.count("CPU") == 1);
  }

  RAY_CHECK(worker->actor_id == TaskSpec_actor_id(spec));
  /* Make sure the driver for this task is still alive. */
  WorkerID driver_id = TaskSpec_driver_id(spec);
  RAY_CHECK(is_driver_alive(state, driver_id));

  /* Construct a flatbuffer object to send to the worker. */
  flatbuffers::FlatBufferBuilder fbb;
  auto message = ray::local_scheduler::protocol::CreateGetTaskReply(
      fbb, fbb.CreateString((char *) spec, task_spec_size),
      fbb.CreateVector(worker->gpus_in_use));
  fbb.Finish(message);

  if (write_message(worker->sock,
                    static_cast<int64_t>(MessageType::ExecuteTask),
                    fbb.GetSize(), (uint8_t *) fbb.GetBufferPointer()) < 0) {
    if (errno == EPIPE || errno == EBADF) {
      /* Something went wrong, so kill the worker. */
      kill_worker(state, worker, false, false);
      RAY_LOG(WARNING) << "Failed to give task to worker on fd " << worker->sock
                       << ". The client may have hung up.";
    } else {
      RAY_LOG(FATAL) << "Failed to give task to client on fd " << worker->sock;
    }
  }

  Task *task =
      Task_alloc(execution_spec, TaskStatus::RUNNING,
                 state->db ? get_db_client_id(state->db) : DBClientID::nil());
  /* Record which task this worker is executing. This will be freed in
   * process_message when the worker sends a GetTask message to the local
   * scheduler. */
  worker->task_in_progress = Task_copy(task);
  /* Update the global task table. */
  if (state->db != NULL) {
#if !RAY_USE_NEW_GCS
    task_table_update(state->db, task, NULL, NULL, NULL);
#else
    RAY_CHECK_OK(TaskTableAdd(&state->gcs_client, task));
    Task_free(task);
#endif
  } else {
    Task_free(task);
  }
}

// This is used to allow task_table_update to fail.
void allow_task_table_update_failure(UniqueID id,
                                     void *user_context,
                                     void *user_data) {}

void finish_task(LocalSchedulerState *state, LocalSchedulerClient *worker) {
  if (worker->task_in_progress != NULL) {
    TaskSpec *spec = Task_task_execution_spec(worker->task_in_progress)->Spec();
    // Return dynamic resources back for the task in progress.
    if (TaskSpec_is_actor_creation_task(spec)) {
      // Resources required by the actor creation task are acquired for the
      // actor's lifetime, so don't return anything here. TODO(rkn): Should the
      // actor creation task require 1 CPU in addition to any resources acquired
      // for the lifetime of the actor? If not, then the local scheduler may
      // schedule an arbitrary number of actor creation tasks concurrently (if
      // they don't acquire any resources for their entire lifetime). In
      // practice this will usually be rate-limited by the rate at which we can
      // create new workers.

      ActorID actor_creation_id = TaskSpec_actor_creation_id(spec);
      WorkerID driver_id = TaskSpec_driver_id(spec);

      // The driver must be alive because if the driver had been removed, then
      // this worker would have been killed (because it was executing a task for
      // the driver).
      RAY_CHECK(is_driver_alive(state, driver_id));

      // Update the worker struct with this actor ID.
      RAY_CHECK(worker->actor_id.is_nil());
      worker->actor_id = actor_creation_id;
      // Extract the initial execution dependency from the actor creation task.
      RAY_CHECK(TaskSpec_num_returns(spec) == 1);
      ObjectID initial_execution_dependency = TaskSpec_return(spec, 0);
      // Let the scheduling algorithm process the presence of this new worker.
      handle_convert_worker_to_actor(state, state->algorithm_state,
                                     actor_creation_id,
                                     initial_execution_dependency, worker);
      // Publish the actor creation notification. The corresponding callback
      // handle_actor_creation_callback will update state->actor_mapping.
      publish_actor_creation_notification(
          state->db, actor_creation_id, driver_id, get_db_client_id(state->db));
    } else if (worker->actor_id.is_nil()) {
      // Return dynamic resources back for the task in progress.
      RAY_CHECK(worker->resources_in_use["CPU"] ==
                TaskSpec_get_required_resource(spec, "CPU"));
      // Return GPU resources.
      RAY_CHECK(worker->gpus_in_use.size() ==
                TaskSpec_get_required_resource(spec, "GPU"));
      release_resources(state, worker, worker->resources_in_use);
    } else {
      // Actor tasks should only specify CPU requirements.
      RAY_CHECK(0 == TaskSpec_get_required_resource(spec, "GPU"));
      std::unordered_map<std::string, double> cpu_resources;
      cpu_resources["CPU"] = TaskSpec_get_required_resource(spec, "CPU");
      release_resources(state, worker, cpu_resources);
    }
    /* If we're connected to Redis, update tables. */
    if (state->db != NULL) {
      /* Update control state tables. */
      TaskStatus task_state = TaskStatus::DONE;
      Task_set_state(worker->task_in_progress, task_state);
#if !RAY_USE_NEW_GCS
      auto retryInfo = RetryInfo{
          .num_retries = 0,  // This value is unused.
          .timeout = 0,      // This value is unused.
          .fail_callback = allow_task_table_update_failure,
      };

      // We allow this call to fail in case the driver has been removed and the
      // task table entries have already been cleaned up by the monitor.
      task_table_update(state->db, worker->task_in_progress, &retryInfo, NULL,
                        NULL);
#else
      RAY_CHECK_OK(TaskTableAdd(&state->gcs_client, worker->task_in_progress));
      Task_free(worker->task_in_progress);
#endif
    } else {
      Task_free(worker->task_in_progress);
    }
    /* The call to task_table_update takes ownership of the
     * task_in_progress, so we set the pointer to NULL so it is not used. */
    worker->task_in_progress = NULL;
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
    RAY_LOG(FATAL) << "Lost connection to the plasma store, local scheduler is "
                   << "exiting!";
  }
  auto object_info = flatbuffers::GetRoot<ObjectInfo>(notification);
  ObjectID object_id = from_flatbuf(*object_info->object_id());
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
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;
  if (!updated) {
    /* The test-and-set failed. The task is either: (1) not finished yet, (2)
     * lost, but not yet updated, or (3) already being reconstructed. */
    DBClientID current_local_scheduler_id = Task_local_scheduler(task);
    if (!current_local_scheduler_id.is_nil()) {
      DBClient current_local_scheduler =
          db_client_table_cache_get(state->db, current_local_scheduler_id);
      if (!current_local_scheduler.is_alive) {
        /* (2) The current local scheduler for the task is dead. The task is
         * lost, but the task table hasn't received the update yet. Retry the
         * test-and-set. */
#if !RAY_USE_NEW_GCS
        task_table_test_and_update(state->db, Task_task_id(task),
                                   current_local_scheduler_id, Task_state(task),
                                   TaskStatus::RECONSTRUCTING, NULL,
                                   reconstruct_task_update_callback, state);
#else
        RAY_CHECK_OK(gcs::TaskTableTestAndUpdate(
            &state->gcs_client, Task_task_id(task), current_local_scheduler_id,
            static_cast<SchedulingState>(Task_state(task)),
            SchedulingState::RECONSTRUCTING,
            [task, user_context](gcs::AsyncGcsClient *, const ray::TaskID &,
                                 const TaskTableDataT &t, bool updated) {
              reconstruct_task_update_callback(task, user_context, updated);
            }));
        Task_free(task);
#endif
      }
    }
    /* The test-and-set failed, so it is not safe to resubmit the task for
     * execution. Suppress the request. */
    return;
  }

  /* Otherwise, the test-and-set succeeded, so resubmit the task for execution
   * to ensure that reconstruction will happen. */
  TaskExecutionSpec *execution_spec = Task_task_execution_spec(task);
  TaskSpec *spec = execution_spec->Spec();
  if (TaskSpec_actor_id(spec).is_nil()) {
    handle_task_submitted(state, state->algorithm_state, *execution_spec);
  } else {
    handle_actor_task_submitted(state, state->algorithm_state, *execution_spec);
  }

  /* Recursively reconstruct the task's inputs, if necessary. */
  int64_t num_dependencies = execution_spec->NumDependencies();
  for (int64_t i = 0; i < num_dependencies; ++i) {
    int count = execution_spec->DependencyIdCount(i);
    for (int64_t j = 0; j < count; ++j) {
      ObjectID dependency_id = execution_spec->DependencyId(i, j);
      reconstruct_object(state, dependency_id);
    }
  }
}

void reconstruct_put_task_update_callback(Task *task,
                                          void *user_context,
                                          bool updated) {
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;
  if (!updated) {
    /* The test-and-set failed. The task is either: (1) not finished yet, (2)
     * lost, but not yet updated, or (3) already being reconstructed. */
    DBClientID current_local_scheduler_id = Task_local_scheduler(task);
    if (!current_local_scheduler_id.is_nil()) {
      DBClient current_local_scheduler =
          db_client_table_cache_get(state->db, current_local_scheduler_id);
      if (!current_local_scheduler.is_alive) {
        /* (2) The current local scheduler for the task is dead. The task is
         * lost, but the task table hasn't received the update yet. Retry the
         * test-and-set. */
#if !RAY_USE_NEW_GCS
        task_table_test_and_update(state->db, Task_task_id(task),
                                   current_local_scheduler_id, Task_state(task),
                                   TaskStatus::RECONSTRUCTING, NULL,
                                   reconstruct_put_task_update_callback, state);
#else
        RAY_CHECK_OK(gcs::TaskTableTestAndUpdate(
            &state->gcs_client, Task_task_id(task), current_local_scheduler_id,
            static_cast<SchedulingState>(Task_state(task)),
            SchedulingState::RECONSTRUCTING,
            [task, user_context](gcs::AsyncGcsClient *, const ray::TaskID &,
                                 const TaskTableDataT &, bool updated) {
              reconstruct_put_task_update_callback(task, user_context, updated);
            }));
        Task_free(task);
#endif
      } else if (Task_state(task) == TaskStatus::RUNNING) {
        /* (1) The task is still executing on a live node. The object created
         * by `ray.put` was not able to be reconstructed, and the workload will
         * likely hang. Push an error to the appropriate driver. */
        TaskSpec *spec = Task_task_execution_spec(task)->Spec();

        std::ostringstream error_message;
        error_message << "The task with ID " << TaskSpec_task_id(spec)
                      << " is still executing and so the object created by "
                      << "ray.put could not be reconstructed.";
        push_error(state->db, TaskSpec_driver_id(spec),
                   ErrorIndex::PUT_RECONSTRUCTION, error_message.str());
      }
    } else {
      /* (1) The task is still executing and it is the driver task. We cannot
       * restart the driver task, so the workload will hang. Push an error to
       * the appropriate driver. */
      TaskSpec *spec = Task_task_execution_spec(task)->Spec();

      std::ostringstream error_message;
      error_message << "The task with ID " << TaskSpec_task_id(spec)
                    << " is a driver task and so the object created by ray.put "
                    << "could not be reconstructed.";
      push_error(state->db, TaskSpec_driver_id(spec),
                 ErrorIndex::PUT_RECONSTRUCTION, error_message.str());
    }
  } else {
    /* The update to TaskStatus::RECONSTRUCTING succeeded, so continue with
     * reconstruction as usual. */
    reconstruct_task_update_callback(task, user_context, updated);
  }
}

void reconstruct_evicted_result_lookup_callback(ObjectID reconstruct_object_id,
                                                TaskID task_id,
                                                bool is_put,
                                                void *user_context) {
  RAY_CHECK(!task_id.is_nil())
      << "No task information found for object during reconstruction";
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
#if !RAY_USE_NEW_GCS
  task_table_test_and_update(state->db, task_id, DBClientID::nil(),
                             (TaskStatus::DONE | TaskStatus::LOST),
                             TaskStatus::RECONSTRUCTING, NULL, done_callback,
                             state);
#else
  RAY_CHECK_OK(gcs::TaskTableTestAndUpdate(
      &state->gcs_client, task_id, DBClientID::nil(),
      static_cast<SchedulingState>(static_cast<uint>(SchedulingState::DONE) |
                                   static_cast<uint>(SchedulingState::LOST)),
      SchedulingState::RECONSTRUCTING,
      [done_callback, state](gcs::AsyncGcsClient *, const ray::TaskID &,
                             const TaskTableDataT &t, bool updated) {
        Task *task = Task_alloc(t.task_info.data(), t.task_info.size(),
                                static_cast<TaskStatus>(t.scheduling_state),
                                DBClientID::from_binary(t.scheduler_id),
                                std::vector<ObjectID>());
        done_callback(task, state, updated);
        Task_free(task);
      }));
#endif
}

void reconstruct_failed_result_lookup_callback(ObjectID reconstruct_object_id,
                                               TaskID task_id,
                                               bool is_put,
                                               void *user_context) {
  if (task_id.is_nil()) {
    /* NOTE(swang): For some reason, the result table update sometimes happens
     * after this lookup returns, possibly due to concurrent clients. In most
     * cases, this is okay because the initial execution is probably still
     * pending, so for now, we log a warning and suppress reconstruction. */
    RAY_LOG(WARNING) << "No task information found for object during "
                     << "reconstruction (no object entry yet)";
    return;
  }
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;
  /* If the task failed to finish, it's safe for us to claim responsibility for
   * reconstruction. */
#if !RAY_USE_NEW_GCS
  task_table_test_and_update(state->db, task_id, DBClientID::nil(),
                             TaskStatus::LOST, TaskStatus::RECONSTRUCTING, NULL,
                             reconstruct_task_update_callback, state);
#else
  RAY_CHECK_OK(gcs::TaskTableTestAndUpdate(
      &state->gcs_client, task_id, DBClientID::nil(), SchedulingState::LOST,
      SchedulingState::RECONSTRUCTING,
      [state](gcs::AsyncGcsClient *, const ray::TaskID &,
              const TaskTableDataT &t, bool updated) {
        Task *task = Task_alloc(t.task_info.data(), t.task_info.size(),
                                static_cast<TaskStatus>(t.scheduling_state),
                                DBClientID::from_binary(t.scheduler_id),
                                std::vector<ObjectID>());
        reconstruct_task_update_callback(task, state, updated);
        Task_free(task);
      }));
#endif
}

void reconstruct_object_lookup_callback(
    ObjectID reconstruct_object_id,
    bool never_created,
    const std::vector<DBClientID> &manager_ids,
    void *user_context) {
  RAY_LOG(DEBUG) << "Manager count was " << manager_ids.size();
  /* Only continue reconstruction if we find that the object doesn't exist on
   * any nodes. NOTE: This codepath is not responsible for checking if the
   * object table entry is up-to-date. */
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;
  /* Look up the task that created the object in the result table. */
  if (never_created) {
    /* If the object has not been created yet, we reconstruct the object if and
     * only if the task that created the object failed to complete. */
    result_table_lookup(state->db, reconstruct_object_id, NULL,
                        reconstruct_failed_result_lookup_callback,
                        (void *) state);
  } else {
    /* If the object has been created, filter out the dead plasma managers that
     * have it. */
    size_t num_live_managers = 0;
    for (auto manager_id : manager_ids) {
      DBClient manager = db_client_table_cache_get(state->db, manager_id);
      if (manager.is_alive) {
        num_live_managers++;
      }
    }
    /* If the object was created, but all plasma managers that had the object
     * either evicted it or failed, we reconstruct the object if and only if
     * there are no other instances of the task running. */
    if (num_live_managers == 0) {
      result_table_lookup(state->db, reconstruct_object_id, NULL,
                          reconstruct_evicted_result_lookup_callback,
                          (void *) state);
    }
  }
}

void reconstruct_object(LocalSchedulerState *state,
                        ObjectID reconstruct_object_id) {
  RAY_LOG(DEBUG) << "Starting reconstruction";
  /* If the object is locally available, no need to reconstruct. */
  if (object_locally_available(state->algorithm_state, reconstruct_object_id)) {
    return;
  }
  /* Determine if reconstruction is necessary by checking if the object exists
   * on a node. */
  RAY_CHECK(state->db != NULL);
  object_table_lookup(state->db, reconstruct_object_id, NULL,
                      reconstruct_object_lookup_callback, (void *) state);
}

void handle_client_register(
    LocalSchedulerState *state,
    LocalSchedulerClient *worker,
    const ray::local_scheduler::protocol::RegisterClientRequest *message) {
  /* Make sure this worker hasn't already registered. */
  RAY_CHECK(!worker->registered);
  worker->registered = true;
  worker->is_worker = message->is_worker();
  RAY_CHECK(worker->client_id.is_nil());
  worker->client_id = from_flatbuf(*message->client_id());

  /* Register the worker or driver. */
  if (worker->is_worker) {
    /* Update the actor mapping with the actor ID of the worker (if an actor is
     * running on the worker). */
    worker->pid = message->worker_pid();
    /* Register worker process id with the scheduler. */
    /* Determine if this worker is one of our child processes. */
    RAY_LOG(DEBUG) << "PID is " << worker->pid;
    auto it = std::find(state->child_pids.begin(), state->child_pids.end(),
                        worker->pid);
    if (it != state->child_pids.end()) {
      /* If this worker is one of our child processes, mark it as a child so
       * that we know that we can wait for the process to exit during
       * cleanup. */
      worker->is_child = true;
      state->child_pids.erase(it);
      RAY_LOG(DEBUG) << "Found matching child pid " << worker->pid;
    }
  } else {
    /* Register the driver. Currently we don't do anything here. */
  }
}

void handle_driver_removed_callback(WorkerID driver_id, void *user_context) {
  LocalSchedulerState *state = (LocalSchedulerState *) user_context;

  /* Kill any actors that were created by the removed driver, and kill any
   * workers that are currently running tasks from the dead driver. */
  auto it = state->workers.begin();
  while (it != state->workers.end()) {
    /* Increment the iterator by one before calling kill_worker, because
     * kill_worker will invalidate the iterator. Note that this requires
     * knowledge of the particular container that we are iterating over (in this
     * case it is a list). */
    auto next_it = it;
    next_it++;

    ActorID actor_id = (*it)->actor_id;
    Task *task = (*it)->task_in_progress;

    if (!actor_id.is_nil()) {
      /* This is an actor. */
      RAY_CHECK(state->actor_mapping.count(actor_id) == 1);
      if (state->actor_mapping[actor_id].driver_id == driver_id) {
        /* This actor was created by the removed driver, so kill the actor. */
        RAY_LOG(DEBUG) << "Killing an actor for a removed driver.";
        kill_worker(state, *it, false, true);
      }
    } else if (task != NULL) {
      TaskSpec *spec = Task_task_execution_spec(task)->Spec();
      if (TaskSpec_driver_id(spec) == driver_id) {
        RAY_LOG(DEBUG) << "Killing a worker executing a task for a removed "
                       << "driver.";
        kill_worker(state, *it, false, true);
      }
    }

    it = next_it;
  }

  /* Add the driver to a list of dead drivers. */
  state->removed_drivers.insert(driver_id);

  /* Notify the scheduling algorithm that the driver has been removed. It should
   * remove tasks for that driver from its data structures. */
  handle_driver_removed(state, state->algorithm_state, driver_id);
}

void handle_client_disconnect(LocalSchedulerState *state,
                              LocalSchedulerClient *worker) {
  if (!worker->registered || worker->is_worker) {
  } else {
    /* In this case, a driver is disconecting. */
    driver_table_send_driver_death(state->db, worker->client_id, NULL);
  }
  /* Suppress the warning message if the worker already disconnected. */
  kill_worker(state, worker, false, worker->disconnected);
}

void handle_get_actor_frontier(LocalSchedulerState *state,
                               LocalSchedulerClient *worker,
                               ActorID actor_id) {
  auto task_counters =
      get_actor_task_counters(state->algorithm_state, actor_id);
  auto frontier = get_actor_frontier(state->algorithm_state, actor_id);

  /* Build the ActorFrontier flatbuffer. */
  std::vector<ActorHandleID> handle_vector;
  std::vector<int64_t> task_counter_vector;
  std::vector<ObjectID> frontier_vector;
  for (auto handle : task_counters) {
    handle_vector.push_back(handle.first);
    task_counter_vector.push_back(handle.second);
    frontier_vector.push_back(frontier[handle.first]);
  }
  flatbuffers::FlatBufferBuilder fbb;
  auto reply = ray::local_scheduler::protocol::CreateActorFrontier(
      fbb, to_flatbuf(fbb, actor_id), to_flatbuf(fbb, handle_vector),
      fbb.CreateVector(task_counter_vector), to_flatbuf(fbb, frontier_vector));
  fbb.Finish(reply);
  /* Respond with the built ActorFrontier. */
  if (write_message(worker->sock,
                    static_cast<int64_t>(MessageType::GetActorFrontierReply),
                    fbb.GetSize(), (uint8_t *) fbb.GetBufferPointer()) < 0) {
    if (errno == EPIPE || errno == EBADF) {
      /* Something went wrong, so kill the worker. */
      kill_worker(state, worker, false, false);
      RAY_LOG(WARNING) << "Failed to return actor frontier to worker on fd "
                       << worker->sock << ". The client may have hung up.";
    } else {
      RAY_LOG(FATAL) << "Failed to give task to client on fd " << worker->sock;
    }
  }
}

void handle_set_actor_frontier(
    LocalSchedulerState *state,
    LocalSchedulerClient *worker,
    ray::local_scheduler::protocol::ActorFrontier const &frontier) {
  /* Parse the ActorFrontier flatbuffer. */
  ActorID actor_id = from_flatbuf(*frontier.actor_id());
  std::unordered_map<ActorID, int64_t> task_counters;
  std::unordered_map<ActorID, ObjectID> frontier_dependencies;
  for (size_t i = 0; i < frontier.handle_ids()->size(); ++i) {
    ActorID handle_id = from_flatbuf(*frontier.handle_ids()->Get(i));
    task_counters[handle_id] = frontier.task_counters()->Get(i);
    frontier_dependencies[handle_id] =
        from_flatbuf(*frontier.frontier_dependencies()->Get(i));
  }
  /* Set the actor's frontier. */
  set_actor_task_counters(state->algorithm_state, actor_id, task_counters);
  set_actor_frontier(state, state->algorithm_state, actor_id,
                     frontier_dependencies);
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  int64_t start_time = current_time_ms();

  LocalSchedulerClient *worker = (LocalSchedulerClient *) context;
  LocalSchedulerState *state = worker->local_scheduler_state;

  int64_t type;
  read_vector(client_sock, &type, state->input_buffer);
  uint8_t *input = state->input_buffer.data();

  RAY_LOG(DEBUG) << "New event of type " << type;

  switch (type) {
  case static_cast<int64_t>(MessageType::SubmitTask): {
    auto message =
        flatbuffers::GetRoot<ray::local_scheduler::protocol::SubmitTaskRequest>(
            input);
    TaskExecutionSpec execution_spec =
        TaskExecutionSpec(from_flatbuf(*message->execution_dependencies()),
                          (TaskSpec *) message->task_spec()->data(),
                          message->task_spec()->size());
    /* Set the tasks's local scheduler entrypoint time. */
    execution_spec.SetLastTimeStamp(current_time_ms());
    TaskSpec *spec = execution_spec.Spec();
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
    if (TaskSpec_actor_id(spec).is_nil()) {
      handle_task_submitted(state, state->algorithm_state, execution_spec);
    } else {
      handle_actor_task_submitted(state, state->algorithm_state,
                                  execution_spec);
    }
  } break;
  case static_cast<int64_t>(MessageType::TaskDone): {
  } break;
  case static_cast<int64_t>(MessageType::DisconnectClient): {
    finish_task(state, worker);
    RAY_CHECK(!worker->disconnected);
    worker->disconnected = true;
    /* If the disconnected worker was not an actor, start a new worker to make
     * sure there are enough workers in the pool. */
    if (worker->actor_id.is_nil()) {
      start_worker(state);
    }
  } break;
  case static_cast<int64_t>(MessageType::EventLogMessage): {
    /* Parse the message. */
    auto message =
        flatbuffers::GetRoot<ray::local_scheduler::protocol::EventLogMessage>(
            input);
    if (state->db != NULL) {
      RayLogger_log_event(state->db, (uint8_t *) message->key()->data(),
                          message->key()->size(),
                          (uint8_t *) message->value()->data(),
                          message->value()->size(), message->timestamp());
    }
  } break;
  case static_cast<int64_t>(MessageType::RegisterClientRequest): {
    auto message = flatbuffers::GetRoot<
        ray::local_scheduler::protocol::RegisterClientRequest>(input);
    handle_client_register(state, worker, message);
  } break;
  case static_cast<int64_t>(MessageType::GetTask): {
    /* If this worker reports a completed task, account for resources. */
    finish_task(state, worker);
    /* Let the scheduling algorithm process the fact that there is an available
     * worker. */
    if (worker->actor_id.is_nil()) {
      handle_worker_available(state, state->algorithm_state, worker);
    } else {
      handle_actor_worker_available(state, state->algorithm_state, worker);
    }
  } break;
  case static_cast<int64_t>(MessageType::ReconstructObject): {
    auto message =
        flatbuffers::GetRoot<ray::local_scheduler::protocol::ReconstructObject>(
            input);
    if (worker->task_in_progress != NULL && !worker->is_blocked) {
      /* If the worker was executing a task (i.e. non-driver) and it wasn't
       * already blocked on an object that's not locally available, update its
       * state to blocked. */
      worker->is_blocked = true;
      // Return the CPU resources that the blocked worker was using, but not
      // other resources. If the worker is an actor, this will not return the
      // CPU resources that the worker has acquired for its lifetime. It will
      // only return the ones associated with the current method.
      TaskSpec *spec =
          Task_task_execution_spec(worker->task_in_progress)->Spec();
      std::unordered_map<std::string, double> cpu_resources;
      cpu_resources["CPU"] = TaskSpec_get_required_resource(spec, "CPU");
      release_resources(state, worker, cpu_resources);
      /* Let the scheduling algorithm process the fact that the worker is
       * blocked. */
      if (worker->actor_id.is_nil()) {
        handle_worker_blocked(state, state->algorithm_state, worker);
      } else {
        handle_actor_worker_blocked(state, state->algorithm_state, worker);
      }
      print_worker_info("Reconstructing", state->algorithm_state);
    }
    reconstruct_object(state, from_flatbuf(*message->object_id()));
  } break;
  case static_cast<int64_t>(CommonMessageType::DISCONNECT_CLIENT): {
    RAY_LOG(DEBUG) << "Disconnecting client on fd " << client_sock;
    handle_client_disconnect(state, worker);
  } break;
  case static_cast<int64_t>(MessageType::NotifyUnblocked): {
    /* TODO(rkn): A driver may call this as well, right? */
    if (worker->task_in_progress != NULL) {
      /* If the worker was executing a task (i.e. non-driver), update its
       * state to not blocked. */
      RAY_CHECK(worker->is_blocked);
      worker->is_blocked = false;
      /* Lease back the CPU resources that the blocked worker needs (note that
       * it never released its GPU resources). TODO(swang): Leasing back the
       * resources to blocked workers can cause us to transiently exceed the
       * maximum number of resources. This could be fixed by having blocked
       * workers explicitly yield and wait to be given back resources before
       * continuing execution. */
      TaskSpec *spec =
          Task_task_execution_spec(worker->task_in_progress)->Spec();
      std::unordered_map<std::string, double> cpu_resources;
      cpu_resources["CPU"] = TaskSpec_get_required_resource(spec, "CPU");
      acquire_resources(state, worker, cpu_resources);
      /* Let the scheduling algorithm process the fact that the worker is
       * unblocked. */
      if (worker->actor_id.is_nil()) {
        handle_worker_unblocked(state, state->algorithm_state, worker);
      } else {
        handle_actor_worker_unblocked(state, state->algorithm_state, worker);
      }
    }
    print_worker_info("Worker unblocked", state->algorithm_state);
  } break;
  case static_cast<int64_t>(MessageType::PutObject): {
    auto message =
        flatbuffers::GetRoot<ray::local_scheduler::protocol::PutObject>(input);
    result_table_add(state->db, from_flatbuf(*message->object_id()),
                     from_flatbuf(*message->task_id()), true, NULL, NULL, NULL);
  } break;
  case static_cast<int64_t>(MessageType::GetActorFrontierRequest): {
    auto message = flatbuffers::GetRoot<
        ray::local_scheduler::protocol::GetActorFrontierRequest>(input);
    ActorID actor_id = from_flatbuf(*message->actor_id());
    handle_get_actor_frontier(state, worker, actor_id);
  } break;
  case static_cast<int64_t>(MessageType::SetActorFrontier): {
    auto message =
        flatbuffers::GetRoot<ray::local_scheduler::protocol::ActorFrontier>(
            input);
    handle_set_actor_frontier(state, worker, *message);
  } break;
  default:
    /* This code should be unreachable. */
    RAY_CHECK(0);
  }

  /* Print a warning if this method took too long. */
  int64_t end_time = current_time_ms();
  if (end_time - start_time >
      RayConfig::instance().max_time_for_handler_milliseconds()) {
    RAY_LOG(WARNING) << "process_message of type " << type << " took "
                     << end_time - start_time << " milliseconds.";
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
  worker->registered = false;
  worker->disconnected = false;
  /* We don't know whether this is a worker or not, so just initialize is_worker
   * to false. */
  worker->is_worker = true;
  worker->client_id = WorkerID::nil();
  worker->task_in_progress = NULL;
  worker->is_blocked = false;
  worker->pid = 0;
  worker->is_child = false;
  worker->actor_id = ActorID::nil();
  worker->local_scheduler_state = state;
  state->workers.push_back(worker);
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message,
                      worker);
  RAY_LOG(DEBUG) << "new connection with fd " << new_socket;
}

/* We need this code so we can clean up when we get a SIGTERM signal. */

LocalSchedulerState *g_state = NULL;

void signal_handler(int signal) {
  RAY_LOG(DEBUG) << "Signal was " << signal;
  if (signal == SIGTERM) {
    /* NOTE(swang): This call removes the SIGTERM handler to ensure that we
     * free the local scheduler state at most once. If another SIGTERM is
     * caught during this call, there is the possibility of orphan worker
     * processes. */
    if (g_state) {
      LocalSchedulerState_free(g_state);
    }
    exit(0);
  }
}

/* End of the cleanup code. */

void handle_task_scheduled_callback(Task *original_task,
                                    void *subscribe_context) {
  LocalSchedulerState *state = (LocalSchedulerState *) subscribe_context;
  TaskExecutionSpec *execution_spec = Task_task_execution_spec(original_task);
  TaskSpec *spec = execution_spec->Spec();

  /* Set the tasks's local scheduler entrypoint time. */
  execution_spec->SetLastTimeStamp(current_time_ms());

  /* If the driver for this task has been removed, then don't bother telling the
   * scheduling algorithm. */
  WorkerID driver_id = TaskSpec_driver_id(spec);
  if (!is_driver_alive(state, driver_id)) {
    RAY_LOG(DEBUG) << "Ignoring scheduled task for removed driver.";
    return;
  }

  if (TaskSpec_actor_id(spec).is_nil()) {
    /* This task does not involve an actor. Handle it normally. */
    handle_task_scheduled(state, state->algorithm_state, *execution_spec);
  } else {
    /* This task involves an actor. Call the scheduling algorithm's actor
     * handler. */
    handle_actor_task_scheduled(state, state->algorithm_state, *execution_spec);
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
 * @param context The context for this callback.
 * @return Void.
 */
void handle_actor_creation_callback(const ActorID &actor_id,
                                    const WorkerID &driver_id,
                                    const DBClientID &local_scheduler_id,
                                    void *context) {
  LocalSchedulerState *state = (LocalSchedulerState *) context;

  /* If the driver has been removed, don't bother doing anything. */
  if (state->removed_drivers.count(driver_id) == 1) {
    return;
  }

  // TODO(rkn): If we do not have perfect task suppression and it is possible
  // for a task to be executed simultaneously on two nodes, then we will need to
  // detect and handle that case.

  if (state->actor_mapping.count(actor_id) != 0) {
    // This actor already exists.
    auto it = state->actor_mapping.find(actor_id);
    if (it->second.local_scheduler_id == get_db_client_id(state->db)) {
      // TODO(rkn): The actor was previously assigned to this local scheduler.
      // We should kill the actor here if it is still around. Also, if it hasn't
      // registered yet, we should keep track of its PID so we can kill it
      // anyway.
      // TODO(swang): Evict actor dummy objects as part of actor cleanup.
    }
  }

  /* Create a new entry and add it to the actor mapping table. TODO(rkn):
   * Currently this is never removed (except when the local scheduler state is
   * deleted). */
  ActorMapEntry entry;
  entry.local_scheduler_id = local_scheduler_id;
  entry.driver_id = driver_id;
  state->actor_mapping[actor_id] = entry;

  /* Let the scheduling algorithm process the fact that a new actor has been
   * created. */
  handle_actor_creation_notification(state, state->algorithm_state, actor_id);
}

int heartbeat_handler(event_loop *loop, timer_id id, void *context) {
  LocalSchedulerState *state = (LocalSchedulerState *) context;
  SchedulingAlgorithmState *algorithm_state = state->algorithm_state;

  // Spillback policy invocation is synchronized with the heartbeats.
  spillback_tasks_handler(state);

  /* Check that the last heartbeat was not sent too long ago. */
  int64_t current_time = current_time_ms();
  RAY_CHECK(current_time >= state->previous_heartbeat_time);
  if (current_time - state->previous_heartbeat_time >
      RayConfig::instance().num_heartbeats_timeout() *
          RayConfig::instance().heartbeat_timeout_milliseconds()) {
    RAY_LOG(FATAL) << "The last heartbeat was sent "
                   << current_time - state->previous_heartbeat_time
                   << " milliseconds ago.";
  }
  state->previous_heartbeat_time = current_time;

  LocalSchedulerInfo info;
  /* Ask the scheduling algorithm to fill out the scheduler info struct. */
  provide_scheduler_info(state, algorithm_state, &info);
  /* Publish the heartbeat to all subscribers of the local scheduler table. */
  local_scheduler_table_send_info(state->db, &info, NULL);
  /* Reset the timer. */
  return RayConfig::instance().heartbeat_timeout_milliseconds();
}

void start_server(
    const char *node_ip_address,
    const char *socket_name,
    const char *redis_primary_addr,
    int redis_primary_port,
    const char *plasma_store_socket_name,
    const char *plasma_manager_socket_name,
    const char *plasma_manager_address,
    bool global_scheduler_exists,
    const std::unordered_map<std::string, double> &static_resource_conf,
    const char *start_worker_command,
    int num_workers) {
  /* Ignore SIGPIPE signals. If we don't do this, then when we attempt to write
   * to a client that has already died, the local scheduler could die. */
  signal(SIGPIPE, SIG_IGN);
  /* Ignore SIGCHLD signals. If we don't do this, then worker processes will
   * become zombies instead of dying gracefully. */
  signal(SIGCHLD, SIG_IGN);
  int fd = bind_ipc_sock(socket_name, true);
  event_loop *loop = event_loop_create();
  g_state = LocalSchedulerState_init(
      node_ip_address, loop, redis_primary_addr, redis_primary_port,
      socket_name, plasma_store_socket_name, plasma_manager_socket_name,
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
                         TaskStatus::SCHEDULED, handle_task_scheduled_callback,
                         g_state, NULL, NULL, NULL);
  }
  /* Subscribe to notifications about newly created actors. */
  if (g_state->db != NULL) {
    actor_notification_table_subscribe(
        g_state->db, handle_actor_creation_callback, g_state, NULL);
  }
  /* Subscribe to notifications about removed drivers. */
  if (g_state->db != NULL) {
    driver_table_subscribe(g_state->db, handle_driver_removed_callback, g_state,
                           NULL);
  }
  /* Create a timer for publishing information about the load on the local
   * scheduler to the local scheduler table. This message also serves as a
   * heartbeat. */
  if (g_state->db != NULL) {
    event_loop_add_timer(loop,
                         RayConfig::instance().heartbeat_timeout_milliseconds(),
                         heartbeat_handler, g_state);
  }
  /* Listen for new and deleted db clients. */
  if (g_state->db != NULL) {
    db_client_table_cache_init(g_state->db);
  }
  /* Create a timer for fetching queued tasks' missing object dependencies. */
  event_loop_add_timer(
      loop, RayConfig::instance().local_scheduler_fetch_timeout_milliseconds(),
      fetch_object_timeout_handler, g_state);
  /* Create a timer for initiating the reconstruction of tasks' missing object
   * dependencies. */
  event_loop_add_timer(
      loop, RayConfig::instance()
                .local_scheduler_reconstruction_timeout_milliseconds(),
      reconstruct_object_timeout_handler, g_state);
  // Create a timer for rerunning actor creation tasks for actor tasks that are
  // cached locally.
  event_loop_add_timer(
      loop, RayConfig::instance()
                .local_scheduler_reconstruction_timeout_milliseconds(),
      rerun_actor_creation_tasks_timeout_handler, g_state);
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
  /* IP address and port of the primary redis instance. */
  char *redis_primary_addr_port = NULL;
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
  std::unordered_map<std::string, double> static_resource_conf;
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
      redis_primary_addr_port = optarg;
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
      RAY_LOG(FATAL) << "unknown option " << c;
    }
  }
  if (!static_resource_list) {
    RAY_LOG(FATAL) << "please specify a static resource list with the -c "
                   << "switch";
  }
  // Parse the resource list.
  std::istringstream resource_string(static_resource_list);
  std::string resource_name;
  std::string resource_quantity;

  while (std::getline(resource_string, resource_name, ',')) {
    RAY_CHECK(std::getline(resource_string, resource_quantity, ','));
    // TODO(rkn): The line below could throw an exception. What should we do
    // about this?
    static_resource_conf[resource_name] = std::stod(resource_quantity);
  }

  if (!scheduler_socket_name) {
    RAY_LOG(FATAL) << "please specify socket for incoming connections with "
                   << "-s switch";
  }
  if (!plasma_store_socket_name) {
    RAY_LOG(FATAL) << "please specify socket for connecting to Plasma store "
                   << "with -p switch";
  }
  if (!node_ip_address) {
    RAY_LOG(FATAL) << "please specify the node IP address with -h switch";
  }
  int num_workers = 0;
  if (num_workers_str) {
    num_workers = strtol(num_workers_str, NULL, 10);
    if (num_workers < 0) {
      RAY_LOG(FATAL) << "Number of workers must be nonnegative";
    }
  }

  char redis_primary_addr[16];
  char *redis_addr = NULL;
  int redis_port = -1;
  if (!redis_primary_addr_port) {
    /* Start the local scheduler without connecting to Redis. In this case, all
     * submitted tasks will be queued and scheduled locally. */
    if (plasma_manager_socket_name) {
      RAY_LOG(FATAL) << "if a plasma manager socket name is provided with the "
                     << "-m switch, then a redis address must be provided with "
                     << "the -r switch";
    }
  } else {
    int redis_primary_port;
    /* Parse the primary Redis address into an IP address and a port. */
    if (parse_ip_addr_port(redis_primary_addr_port, redis_primary_addr,
                           &redis_primary_port) == -1) {
      RAY_LOG(FATAL) << "if a redis address is provided with the -r switch, it "
                     << "should be formatted like 127.0.0.1:6379";
    }
    if (!plasma_manager_socket_name) {
      RAY_LOG(FATAL) << "please specify socket for connecting to Plasma "
                     << "manager with -m switch";
    }
    redis_addr = redis_primary_addr;
    redis_port = redis_primary_port;
  }

  start_server(node_ip_address, scheduler_socket_name, redis_addr, redis_port,
               plasma_store_socket_name, plasma_manager_socket_name,
               plasma_manager_address, global_scheduler_exists,
               static_resource_conf, start_worker_command, num_workers);
}
#endif
