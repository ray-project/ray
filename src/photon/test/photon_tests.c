#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include "common.h"
#include "test/test_common.h"
#include "event_loop.h"
#include "io.h"
#include "utstring.h"
#include "task.h"
#include "state/object_table.h"
#include "state/task_table.h"

#include "photon.h"
#include "photon_scheduler.h"
#include "photon_algorithm.h"
#include "photon_client.h"

SUITE(photon_tests);

const char *plasma_store_socket_name = "/tmp/plasma_store_socket_1";
const char *plasma_manager_socket_name_format = "/tmp/plasma_manager_socket_%d";
const char *photon_socket_name_format = "/tmp/photon_socket_%d";

int64_t timeout_handler(event_loop *loop, int64_t id, void *context) {
  event_loop_stop(loop);
  return EVENT_LOOP_TIMER_DONE;
}

typedef struct {
  /** A socket to mock the Plasma manager. Clients (such as workers) that
   *  connect to this file descriptor must be accepted. */
  int plasma_manager_fd;
  /** A socket to communicate with the Plasma store. */
  int plasma_store_fd;
  /** Photon's socket for IPC requests. */
  int photon_fd;
  /** Photon's local scheduler state. */
  LocalSchedulerState *photon_state;
  /** Photon's event loop. */
  event_loop *loop;
  /** Number of Photon client connections, or mock workers. */
  int num_photon_conns;
  /** Photon client connections. */
  photon_conn **conns;
} photon_mock;

photon_mock *init_photon_mock(int num_workers, int num_mock_workers) {
  const char *node_ip_address = "127.0.0.1";
  const char *redis_addr = node_ip_address;
  int redis_port = 6379;
  const double static_resource_conf[MAX_RESOURCE_INDEX] = {DEFAULT_NUM_CPUS,
                                                           DEFAULT_NUM_GPUS};
  photon_mock *mock = malloc(sizeof(photon_mock));
  memset(mock, 0, sizeof(photon_mock));
  mock->loop = event_loop_create();
  /* Bind to the Photon port and initialize the Photon scheduler. */
  UT_string *plasma_manager_socket_name = bind_ipc_sock_retry(
      plasma_manager_socket_name_format, &mock->plasma_manager_fd);
  mock->plasma_store_fd =
      connect_ipc_sock_retry(plasma_store_socket_name, 5, 100);
  UT_string *photon_socket_name =
      bind_ipc_sock_retry(photon_socket_name_format, &mock->photon_fd);
  CHECK(mock->plasma_store_fd >= 0 && mock->photon_fd >= 0);

  UT_string *worker_command;
  utstring_new(worker_command);
  utstring_printf(worker_command,
                  "python ../../python/ray/workers/default_worker.py "
                  "--node-ip-address=%s --object-store-name=%s "
                  "--object-store-manager-name=%s --local-scheduler-name=%s "
                  "--redis-address=%s:%d",
                  node_ip_address, plasma_store_socket_name,
                  utstring_body(plasma_manager_socket_name),
                  utstring_body(photon_socket_name), redis_addr, redis_port);

  mock->photon_state = LocalSchedulerState_init(
      "127.0.0.1", mock->loop, redis_addr, redis_port,
      utstring_body(photon_socket_name), plasma_store_socket_name,
      utstring_body(plasma_manager_socket_name), NULL, false,
      static_resource_conf, utstring_body(worker_command), num_workers);

  /* Accept the workers as clients to the plasma manager. */
  for (int i = 0; i < num_workers; ++i) {
    accept_client(mock->plasma_manager_fd);
  }

  /* Connect a Photon client. */
  mock->num_photon_conns = num_mock_workers;
  mock->conns = malloc(sizeof(photon_conn *) * num_mock_workers);
  for (int i = 0; i < num_mock_workers; ++i) {
    mock->conns[i] =
        photon_connect(utstring_body(photon_socket_name), NIL_ACTOR_ID);
    new_client_connection(mock->loop, mock->photon_fd,
                          (void *) mock->photon_state, 0);
  }

  utstring_free(worker_command);
  utstring_free(plasma_manager_socket_name);
  utstring_free(photon_socket_name);
  return mock;
}

void destroy_photon_mock(photon_mock *mock) {
  /* Disconnect clients. */
  for (int i = 0; i < mock->num_photon_conns; ++i) {
    photon_disconnect(mock->conns[i]);
  }
  free(mock->conns);

  /* Kill all the workers and run the event loop again so that the task table
   * updates propagate and the tasks in progress are freed. */
  local_scheduler_client **worker = (local_scheduler_client **) utarray_eltptr(
      mock->photon_state->workers, 0);
  while (worker != NULL) {
    kill_worker(*worker, true);
    worker = (local_scheduler_client **) utarray_eltptr(
        mock->photon_state->workers, 0);
  }
  event_loop_add_timer(mock->loop, 500,
                       (event_loop_timer_handler) timeout_handler, NULL);
  event_loop_run(mock->loop);

  /* This also frees mock->loop. */
  LocalSchedulerState_free(mock->photon_state);
  close(mock->plasma_store_fd);
  close(mock->plasma_manager_fd);
  free(mock);
}

void reset_worker(photon_mock *mock, local_scheduler_client *worker) {
  if (worker->task_in_progress) {
    free_task(worker->task_in_progress);
    worker->task_in_progress = NULL;
  }
}

/**
 * Test that object reconstruction gets called. If a task gets submitted,
 * assigned to a worker, and then reconstruction is triggered for its return
 * value, the task should get assigned to a worker again.
 */
TEST object_reconstruction_test(void) {
  photon_mock *photon = init_photon_mock(0, 1);
  photon_conn *worker = photon->conns[0];

  /* Create a task with zero dependencies and one return value. */
  task_spec *spec = example_task_spec(0, 1);
  ObjectID return_id = task_return(spec, 0);

  /* Add an empty object table entry for the object we want to reconstruct, to
   * simulate it having been created and evicted. */
  const char *client_id = "clientid";
  redisContext *context = redisConnect("127.0.0.1", 6379);
  redisReply *reply = redisCommand(context, "RAY.OBJECT_TABLE_ADD %b %ld %b %s",
                                   return_id.id, sizeof(return_id.id), 1,
                                   NIL_DIGEST, (size_t) DIGEST_SIZE, client_id);
  freeReplyObject(reply);
  reply = redisCommand(context, "RAY.OBJECT_TABLE_REMOVE %b %s", return_id.id,
                       sizeof(return_id.id), client_id);
  freeReplyObject(reply);
  redisFree(context);

  pid_t pid = fork();
  if (pid == 0) {
    /* Make sure we receive the task twice. First from the initial submission,
     * and second from the reconstruct request. */
    photon_submit(worker, spec);
    task_spec *task_assigned = photon_get_task(worker);
    ASSERT_EQ(memcmp(task_assigned, spec, task_spec_size(spec)), 0);
    task_spec *reconstruct_task = photon_get_task(worker);
    ASSERT_EQ(memcmp(reconstruct_task, spec, task_spec_size(spec)), 0);
    /* Clean up. */
    free_task_spec(reconstruct_task);
    free_task_spec(task_assigned);
    free_task_spec(spec);
    destroy_photon_mock(photon);
    exit(0);
  } else {
    /* Run the event loop. NOTE: OSX appears to require the parent process to
     * listen for events on the open file descriptors. */
    event_loop_add_timer(photon->loop, 500,
                         (event_loop_timer_handler) timeout_handler, NULL);
    event_loop_run(photon->loop);
    /* Set the task's status to TASK_STATUS_DONE to prevent the race condition
     * that would suppress object reconstruction. */
    Task *task = alloc_task(spec, TASK_STATUS_DONE,
                            get_db_client_id(photon->photon_state->db));
    task_table_add_task(photon->photon_state->db, task, NULL, NULL, NULL);
    /* Trigger reconstruction, and run the event loop again. */
    ObjectID return_id = task_return(spec, 0);
    photon_reconstruct_object(worker, return_id);
    event_loop_add_timer(photon->loop, 500,
                         (event_loop_timer_handler) timeout_handler, NULL);
    event_loop_run(photon->loop);
    /* Wait for the child process to exit and check that there are no tasks
     * left in the local scheduler's task queue. Then, clean up. */
    wait(NULL);
    free_task_spec(spec);
    ASSERT_EQ(num_waiting_tasks(photon->photon_state->algorithm_state), 0);
    ASSERT_EQ(num_dispatch_tasks(photon->photon_state->algorithm_state), 0);
    destroy_photon_mock(photon);
    PASS();
  }
}

/**
 * Test that object reconstruction gets recursively called. In a chain of
 * tasks, if all inputs are lost, then reconstruction of the final object
 * should trigger reconstruction of all previous tasks in the lineage.
 */
TEST object_reconstruction_recursive_test(void) {
  photon_mock *photon = init_photon_mock(0, 1);
  photon_conn *worker = photon->conns[0];
  /* Create a chain of tasks, each one dependent on the one before it. Mark
   * each object as available so that tasks will run immediately. */
  const int NUM_TASKS = 10;
  task_spec *specs[NUM_TASKS];
  specs[0] = example_task_spec(0, 1);
  for (int i = 1; i < NUM_TASKS; ++i) {
    ObjectID arg_id = task_return(specs[i - 1], 0);
    handle_object_available(photon->photon_state,
                            photon->photon_state->algorithm_state, arg_id);
    specs[i] = example_task_spec_with_args(1, 1, &arg_id);
  }

  /* Add an empty object table entry for each object we want to reconstruct, to
   * simulate their having been created and evicted. */
  const char *client_id = "clientid";
  redisContext *context = redisConnect("127.0.0.1", 6379);
  for (int i = 0; i < NUM_TASKS; ++i) {
    ObjectID return_id = task_return(specs[i], 0);
    redisReply *reply = redisCommand(
        context, "RAY.OBJECT_TABLE_ADD %b %ld %b %s", return_id.id,
        sizeof(return_id.id), 1, NIL_DIGEST, (size_t) DIGEST_SIZE, client_id);
    freeReplyObject(reply);
    reply = redisCommand(context, "RAY.OBJECT_TABLE_REMOVE %b %s", return_id.id,
                         sizeof(return_id.id), client_id);
    freeReplyObject(reply);
  }
  redisFree(context);

  pid_t pid = fork();
  if (pid == 0) {
    /* Submit the tasks, and make sure each one gets assigned to a worker. */
    for (int i = 0; i < NUM_TASKS; ++i) {
      photon_submit(worker, specs[i]);
    }
    /* Make sure we receive each task from the initial submission. */
    for (int i = 0; i < NUM_TASKS; ++i) {
      task_spec *task_assigned = photon_get_task(worker);
      ASSERT_EQ(memcmp(task_assigned, specs[i], task_spec_size(task_assigned)),
                0);
      free_task_spec(task_assigned);
    }
    /* Check that the workers receive all tasks in the final return object's
     * lineage during reconstruction. */
    for (int i = 0; i < NUM_TASKS; ++i) {
      task_spec *task_assigned = photon_get_task(worker);
      bool found = false;
      for (int j = 0; j < NUM_TASKS; ++j) {
        if (specs[j] == NULL) {
          continue;
        }
        if (memcmp(task_assigned, specs[j], task_spec_size(task_assigned)) ==
            0) {
          found = true;
          free_task_spec(specs[j]);
          specs[j] = NULL;
        }
      }
      free_task_spec(task_assigned);
      ASSERT(found);
    }
    destroy_photon_mock(photon);
    exit(0);
  } else {
    /* Run the event loop. NOTE: OSX appears to require the parent process to
     * listen for events on the open file descriptors. */
    event_loop_add_timer(photon->loop, 500,
                         (event_loop_timer_handler) timeout_handler, NULL);
    event_loop_run(photon->loop);
    /* Set the final task's status to TASK_STATUS_DONE to prevent the race
     * condition that would suppress object reconstruction. */
    Task *last_task = alloc_task(specs[NUM_TASKS - 1], TASK_STATUS_DONE,
                                 get_db_client_id(photon->photon_state->db));
    task_table_add_task(photon->photon_state->db, last_task, NULL, NULL, NULL);
    /* Trigger reconstruction for the last object, and run the event loop
     * again. */
    ObjectID return_id = task_return(specs[NUM_TASKS - 1], 0);
    photon_reconstruct_object(worker, return_id);
    event_loop_add_timer(photon->loop, 500,
                         (event_loop_timer_handler) timeout_handler, NULL);
    event_loop_run(photon->loop);
    /* Wait for the child process to exit and check that there are no tasks
     * left in the local scheduler's task queue. Then, clean up. */
    wait(NULL);
    ASSERT_EQ(num_waiting_tasks(photon->photon_state->algorithm_state), 0);
    ASSERT_EQ(num_dispatch_tasks(photon->photon_state->algorithm_state), 0);
    for (int i = 0; i < NUM_TASKS; ++i) {
      free_task_spec(specs[i]);
    }
    destroy_photon_mock(photon);
    PASS();
  }
}

/**
 * Test that object reconstruction gets suppressed when there is a location
 * listed for the object in the object table.
 */
task_spec *object_reconstruction_suppression_spec;

void object_reconstruction_suppression_callback(ObjectID object_id,
                                                void *user_context) {
  /* Submit the task after adding the object to the object table. */
  photon_conn *worker = user_context;
  photon_submit(worker, object_reconstruction_suppression_spec);
}

TEST object_reconstruction_suppression_test(void) {
  photon_mock *photon = init_photon_mock(0, 1);
  photon_conn *worker = photon->conns[0];

  object_reconstruction_suppression_spec = example_task_spec(0, 1);
  ObjectID return_id = task_return(object_reconstruction_suppression_spec, 0);
  pid_t pid = fork();
  if (pid == 0) {
    /* Make sure we receive the task once. This will block until the
     * object_table_add callback completes. */
    task_spec *task_assigned = photon_get_task(worker);
    ASSERT_EQ(memcmp(task_assigned, object_reconstruction_suppression_spec,
                     task_spec_size(object_reconstruction_suppression_spec)),
              0);
    /* Trigger a reconstruction. We will check that no tasks get queued as a
     * result of this line in the event loop process. */
    photon_reconstruct_object(worker, return_id);
    /* Clean up. */
    free_task_spec(task_assigned);
    free_task_spec(object_reconstruction_suppression_spec);
    destroy_photon_mock(photon);
    exit(0);
  } else {
    /* Connect a plasma manager client so we can call object_table_add. */
    const char *db_connect_args[] = {"address", "127.0.0.1:12346"};
    DBHandle *db = db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1",
                               2, db_connect_args);
    db_attach(db, photon->loop, false);
    /* Add the object to the object table. */
    object_table_add(db, return_id, 1, (unsigned char *) NIL_DIGEST, NULL,
                     object_reconstruction_suppression_callback,
                     (void *) worker);
    /* Run the event loop. NOTE: OSX appears to require the parent process to
     * listen for events on the open file descriptors. */
    event_loop_add_timer(photon->loop, 1000,
                         (event_loop_timer_handler) timeout_handler, NULL);
    event_loop_run(photon->loop);
    /* Wait for the child process to exit and check that there are no tasks
     * left in the local scheduler's task queue. Then, clean up. */
    wait(NULL);
    ASSERT_EQ(num_waiting_tasks(photon->photon_state->algorithm_state), 0);
    ASSERT_EQ(num_dispatch_tasks(photon->photon_state->algorithm_state), 0);
    free_task_spec(object_reconstruction_suppression_spec);
    db_disconnect(db);
    destroy_photon_mock(photon);
    PASS();
  }
}

TEST task_dependency_test(void) {
  photon_mock *photon = init_photon_mock(0, 1);
  LocalSchedulerState *state = photon->photon_state;
  SchedulingAlgorithmState *algorithm_state = state->algorithm_state;
  /* Get the first worker. */
  local_scheduler_client *worker =
      *((local_scheduler_client **) utarray_eltptr(state->workers, 0));
  task_spec *spec = example_task_spec(1, 1);
  ObjectID oid = task_arg_id(spec, 0);

  /* Check that the task gets queued in the waiting queue if the task is
   * submitted, but the input and workers are not available. */
  handle_task_submitted(state, algorithm_state, spec);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Once the input is available, the task gets moved to the dispatch queue. */
  handle_object_available(state, algorithm_state, oid);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 1);
  /* Once a worker is available, the task gets assigned. */
  handle_worker_available(state, algorithm_state, worker);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker);

  /* Check that the task gets queued in the waiting queue if the task is
   * submitted and a worker is available, but the input is not. */
  handle_object_removed(state, oid);
  handle_task_submitted(state, algorithm_state, spec);
  handle_worker_available(state, algorithm_state, worker);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Once the input is available, the task gets assigned. */
  handle_object_available(state, algorithm_state, oid);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker);

  /* Check that the task gets queued in the dispatch queue if the task is
   * submitted and the input is available, but no worker is available yet. */
  handle_task_submitted(state, algorithm_state, spec);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 1);
  /* Once a worker is available, the task gets assigned. */
  handle_worker_available(state, algorithm_state, worker);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker);

  /* If an object gets removed, check the first scenario again, where the task
   * gets queued in the waiting task if the task is submitted and a worker is
   * available, but the input is not. */
  handle_task_submitted(state, algorithm_state, spec);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 1);
  /* If the input is removed while a task is in the dispatch queue, the task
   * gets moved back to the waiting queue. */
  handle_object_removed(state, oid);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Once the input is available, the task gets moved back to the dispatch
   * queue. */
  handle_object_available(state, algorithm_state, oid);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 1);
  /* Once a worker is available, the task gets assigned. */
  handle_worker_available(state, algorithm_state, worker);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);

  free_task_spec(spec);
  destroy_photon_mock(photon);
  PASS();
}

TEST task_multi_dependency_test(void) {
  photon_mock *photon = init_photon_mock(0, 1);
  LocalSchedulerState *state = photon->photon_state;
  SchedulingAlgorithmState *algorithm_state = state->algorithm_state;
  /* Get the first worker. */
  local_scheduler_client *worker =
      *((local_scheduler_client **) utarray_eltptr(state->workers, 0));
  task_spec *spec = example_task_spec(2, 1);
  ObjectID oid1 = task_arg_id(spec, 0);
  ObjectID oid2 = task_arg_id(spec, 1);

  /* Check that the task gets queued in the waiting queue if the task is
   * submitted, but the inputs and workers are not available. */
  handle_task_submitted(state, algorithm_state, spec);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Check that the task stays in the waiting queue if only one input becomes
   * available. */
  handle_object_available(state, algorithm_state, oid2);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Once all inputs are available, the task is moved to the dispatch queue. */
  handle_object_available(state, algorithm_state, oid1);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 1);
  /* Once a worker is available, the task gets assigned. */
  handle_worker_available(state, algorithm_state, worker);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker);

  /* Check that the task gets queued in the dispatch queue if the task is
   * submitted and the inputs are available, but no worker is available yet. */
  handle_task_submitted(state, algorithm_state, spec);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 1);
  /* If any input is removed while a task is in the dispatch queue, the task
   * gets moved back to the waiting queue. */
  handle_object_removed(state, oid1);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  handle_object_removed(state, oid2);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Check that the task stays in the waiting queue if only one input becomes
   * available. */
  handle_object_available(state, algorithm_state, oid2);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Check that the task stays in the waiting queue if the one input is
   * unavailable again. */
  handle_object_removed(state, oid2);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Check that the task stays in the waiting queue if the other input becomes
   * available. */
  handle_object_available(state, algorithm_state, oid1);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Once all inputs are available, the task is moved to the dispatch queue. */
  handle_object_available(state, algorithm_state, oid2);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 1);
  /* Once a worker is available, the task gets assigned. */
  handle_worker_available(state, algorithm_state, worker);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker);

  free_task_spec(spec);
  destroy_photon_mock(photon);
  PASS();
}

TEST start_kill_workers_test(void) {
  /* Start some workers. */
  int num_workers = 4;
  photon_mock *photon = init_photon_mock(num_workers, 0);
  /* We start off with num_workers children processes, but no workers
   * registered yet. */
  ASSERT_EQ(utarray_len(photon->photon_state->child_pids), num_workers);
  ASSERT_EQ(utarray_len(photon->photon_state->workers), 0);

  /* Make sure that each worker connects to the photon scheduler. This for loop
   * will hang if one of the workers does not connect. */
  for (int i = 0; i < num_workers; ++i) {
    new_client_connection(photon->loop, photon->photon_fd,
                          (void *) photon->photon_state, 0);
  }

  /* After handling each worker's initial connection, we should now have all
   * workers accounted for, but we haven't yet matched up process IDs with our
   * children processes. */
  ASSERT_EQ(utarray_len(photon->photon_state->child_pids), num_workers);
  ASSERT_EQ(utarray_len(photon->photon_state->workers), num_workers);

  /* Each worker should register its process ID. */
  for (int i = 0; i < utarray_len(photon->photon_state->workers); ++i) {
    local_scheduler_client *worker =
        *(local_scheduler_client **) utarray_eltptr(
            photon->photon_state->workers, i);
    process_message(photon->photon_state->loop, worker->sock, worker, 0);
  }
  ASSERT_EQ(utarray_len(photon->photon_state->child_pids), 0);
  ASSERT_EQ(utarray_len(photon->photon_state->workers), num_workers);

  /* After killing a worker, its state is cleaned up. */
  local_scheduler_client *worker = *(local_scheduler_client **) utarray_eltptr(
      photon->photon_state->workers, 0);
  kill_worker(worker, false);
  ASSERT_EQ(utarray_len(photon->photon_state->child_pids), 0);
  ASSERT_EQ(utarray_len(photon->photon_state->workers), num_workers - 1);

  /* Start a worker after the local scheduler has been initialized. */
  start_worker(photon->photon_state, NIL_ACTOR_ID);
  /* Accept the workers as clients to the plasma manager. */
  int new_worker_fd = accept_client(photon->plasma_manager_fd);
  /* The new worker should register its process ID. */
  ASSERT_EQ(utarray_len(photon->photon_state->child_pids), 1);
  ASSERT_EQ(utarray_len(photon->photon_state->workers), num_workers - 1);
  /* Make sure the new worker connects to the photon scheduler. */
  new_client_connection(photon->loop, photon->photon_fd,
                        (void *) photon->photon_state, 0);
  ASSERT_EQ(utarray_len(photon->photon_state->child_pids), 1);
  ASSERT_EQ(utarray_len(photon->photon_state->workers), num_workers);
  /* Make sure that the new worker registers its process ID. */
  worker = *(local_scheduler_client **) utarray_eltptr(
      photon->photon_state->workers, num_workers - 1);
  process_message(photon->photon_state->loop, worker->sock, worker, 0);
  ASSERT_EQ(utarray_len(photon->photon_state->child_pids), 0);
  ASSERT_EQ(utarray_len(photon->photon_state->workers), num_workers);

  /* Clean up. */
  close(new_worker_fd);
  destroy_photon_mock(photon);
  PASS();
}

SUITE(photon_tests) {
  RUN_REDIS_TEST(object_reconstruction_test);
  RUN_REDIS_TEST(object_reconstruction_recursive_test);
  RUN_REDIS_TEST(object_reconstruction_suppression_test);
  RUN_REDIS_TEST(task_dependency_test);
  RUN_REDIS_TEST(task_multi_dependency_test);
  RUN_REDIS_TEST(start_kill_workers_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(photon_tests);
  GREATEST_MAIN_END();
}
