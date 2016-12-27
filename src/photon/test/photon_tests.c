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
  /** A socket to mock the Plasma store. */
  int plasma_fd;
  /** Photon's socket for IPC requests. */
  int photon_fd;
  /** Photon's local scheduler state. */
  local_scheduler_state *photon_state;
  /** Photon's event loop. */
  event_loop *loop;
  /** A Photon client connection. */
  photon_conn *conn;
} photon_mock;

photon_mock *init_photon_mock(bool connect_to_redis) {
  const char *redis_addr = NULL;
  int redis_port = -1;
  if (connect_to_redis) {
    redis_addr = "127.0.0.1";
    redis_port = 6379;
  }
  photon_mock *mock = malloc(sizeof(photon_mock));
  memset(mock, 0, sizeof(photon_mock));
  mock->loop = event_loop_create();
  /* Bind to the Photon port and initialize the Photon scheduler. */
  /* TODO(rkn): Why are we reusing mock->plasma_fd for both the store and the
   * manager? */
  UT_string *plasma_manager_socket_name =
      bind_ipc_sock_retry(plasma_manager_socket_name_format, &mock->plasma_fd);
  mock->plasma_fd = socket_connect_retry(plasma_store_socket_name, 5, 100);
  UT_string *photon_socket_name =
      bind_ipc_sock_retry(photon_socket_name_format, &mock->photon_fd);
  CHECK(mock->plasma_fd >= 0 && mock->photon_fd >= 0);
  mock->photon_state = init_local_scheduler(
      "127.0.0.1", mock->loop, redis_addr, redis_port,
      utstring_body(photon_socket_name), plasma_store_socket_name,
      utstring_body(plasma_manager_socket_name), NULL, false, NULL);
  /* Connect a Photon client. */
  mock->conn = photon_connect(utstring_body(photon_socket_name));
  new_client_connection(mock->loop, mock->photon_fd,
                        (void *) mock->photon_state, 0);
  utstring_free(plasma_manager_socket_name);
  utstring_free(photon_socket_name);
  return mock;
}

void destroy_photon_mock(photon_mock *mock) {
  photon_disconnect(mock->conn);
  close(mock->photon_fd);
  close(mock->plasma_fd);
  /* This also frees mock->loop. */
  free_local_scheduler(mock->photon_state);
  free(mock);
}

void reset_worker(photon_mock *mock, int worker_index) {
  worker *available_worker =
      (worker *) utarray_eltptr(mock->photon_state->workers, worker_index);
  available_worker->task_in_progress = NULL;
}

/**
 * Test that object reconstruction gets called. If a task gets submitted,
 * assigned to a worker, and then reconstruction is triggered for its return
 * value, the task should get assigned to a worker again.
 */
TEST object_reconstruction_test(void) {
  photon_mock *photon = init_photon_mock(true);
  /* Create a task with zero dependencies and one return value. */
  task_spec *spec = example_task_spec(0, 1);
  object_id return_id = task_return(spec, 0);

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
    photon_submit(photon->conn, spec);
    task_spec *task_assigned = photon_get_task(photon->conn);
    ASSERT_EQ(memcmp(task_assigned, spec, task_spec_size(spec)), 0);
    task_spec *reconstruct_task = photon_get_task(photon->conn);
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
    task *task = alloc_task(spec, TASK_STATUS_DONE,
                            get_db_client_id(photon->photon_state->db));
    task_table_add_task(photon->photon_state->db, task,
                        (retry_info *) &photon_retry, NULL, NULL);
    /* Trigger reconstruction, and run the event loop again. */
    object_id return_id = task_return(spec, 0);
    photon_reconstruct_object(photon->conn, return_id);
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
  photon_mock *photon = init_photon_mock(true);
  /* Create a chain of tasks, each one dependent on the one before it. Mark
   * each object as available so that tasks will run immediately. */
  const int NUM_TASKS = 10;
  task_spec *specs[NUM_TASKS];
  specs[0] = example_task_spec(0, 1);
  for (int i = 1; i < NUM_TASKS; ++i) {
    object_id arg_id = task_return(specs[i - 1], 0);
    handle_object_available(photon->photon_state,
                            photon->photon_state->algorithm_state, arg_id);
    specs[i] = example_task_spec_with_args(1, 1, &arg_id);
  }

  /* Add an empty object table entry for each object we want to reconstruct, to
   * simulate their having been created and evicted. */
  const char *client_id = "clientid";
  redisContext *context = redisConnect("127.0.0.1", 6379);
  for (int i = 0; i < NUM_TASKS; ++i) {
    object_id return_id = task_return(specs[i], 0);
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
      photon_submit(photon->conn, specs[i]);
    }
    /* Make sure we receive each task from the initial submission. */
    for (int i = 0; i < NUM_TASKS; ++i) {
      task_spec *task_assigned = photon_get_task(photon->conn);
      ASSERT_EQ(memcmp(task_assigned, specs[i], task_spec_size(task_assigned)),
                0);
      free_task_spec(task_assigned);
    }
    /* Check that the workers receive all tasks in the final return object's
     * lineage during reconstruction. */
    for (int i = 0; i < NUM_TASKS; ++i) {
      task_spec *task_assigned = photon_get_task(photon->conn);
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
    task *last_task = alloc_task(specs[NUM_TASKS - 1], TASK_STATUS_DONE,
                                 get_db_client_id(photon->photon_state->db));
    task_table_add_task(photon->photon_state->db, last_task,
                        (retry_info *) &photon_retry, NULL, NULL);
    /* Trigger reconstruction for the last object, and run the event loop
     * again. */
    object_id return_id = task_return(specs[NUM_TASKS - 1], 0);
    photon_reconstruct_object(photon->conn, return_id);
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

void object_reconstruction_suppression_callback(object_id object_id,
                                                void *user_context) {
  /* Submit the task after adding the object to the object table. */
  photon_mock *photon = user_context;
  photon_submit(photon->conn, object_reconstruction_suppression_spec);
}

TEST object_reconstruction_suppression_test(void) {
  photon_mock *photon = init_photon_mock(true);
  object_reconstruction_suppression_spec = example_task_spec(0, 1);
  object_id return_id = task_return(object_reconstruction_suppression_spec, 0);
  pid_t pid = fork();
  if (pid == 0) {
    /* Make sure we receive the task once. This will block until the
     * object_table_add callback completes. */
    task_spec *task_assigned = photon_get_task(photon->conn);
    ASSERT_EQ(memcmp(task_assigned, object_reconstruction_suppression_spec,
                     task_spec_size(object_reconstruction_suppression_spec)),
              0);
    /* Trigger a reconstruction. We will check that no tasks get queued as a
     * result of this line in the event loop process. */
    photon_reconstruct_object(photon->conn, return_id);
    /* Clean up. */
    free_task_spec(task_assigned);
    free_task_spec(object_reconstruction_suppression_spec);
    destroy_photon_mock(photon);
    exit(0);
  } else {
    /* Connect a plasma manager client so we can call object_table_add. */
    const char *db_connect_args[] = {"address", "127.0.0.1:12346"};
    db_handle *db = db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1",
                               2, db_connect_args);
    db_attach(db, photon->loop, false);
    /* Add the object to the object table. */
    object_table_add(db, return_id, 1, (unsigned char *) NIL_DIGEST,
                     (retry_info *) &photon_retry,
                     object_reconstruction_suppression_callback,
                     (void *) photon);
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
  photon_mock *photon = init_photon_mock(false);
  local_scheduler_state *state = photon->photon_state;
  scheduling_algorithm_state *algorithm_state = state->algorithm_state;
  int worker_index = 0;
  task_spec *spec = example_task_spec(1, 1);
  object_id oid = task_arg_id(spec, 0);

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
  handle_worker_available(state, algorithm_state, worker_index);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker_index);

  /* Check that the task gets queued in the waiting queue if the task is
   * submitted and a worker is available, but the input is not. */
  handle_object_removed(state, oid);
  handle_task_submitted(state, algorithm_state, spec);
  handle_worker_available(state, algorithm_state, worker_index);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 1);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  /* Once the input is available, the task gets assigned. */
  handle_object_available(state, algorithm_state, oid);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker_index);

  /* Check that the task gets queued in the dispatch queue if the task is
   * submitted and the input is available, but no worker is available yet. */
  handle_task_submitted(state, algorithm_state, spec);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 1);
  /* Once a worker is available, the task gets assigned. */
  handle_worker_available(state, algorithm_state, worker_index);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker_index);

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
  handle_worker_available(state, algorithm_state, worker_index);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);

  free_task_spec(spec);
  destroy_photon_mock(photon);
  PASS();
}

TEST task_multi_dependency_test(void) {
  photon_mock *photon = init_photon_mock(false);
  local_scheduler_state *state = photon->photon_state;
  scheduling_algorithm_state *algorithm_state = state->algorithm_state;
  int worker_index = 0;
  task_spec *spec = example_task_spec(2, 1);
  object_id oid1 = task_arg_id(spec, 0);
  object_id oid2 = task_arg_id(spec, 1);

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
  handle_worker_available(state, algorithm_state, worker_index);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker_index);

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
  handle_worker_available(state, algorithm_state, worker_index);
  ASSERT_EQ(num_waiting_tasks(algorithm_state), 0);
  ASSERT_EQ(num_dispatch_tasks(algorithm_state), 0);
  reset_worker(photon, worker_index);

  free_task_spec(spec);
  destroy_photon_mock(photon);
  PASS();
}

SUITE(photon_tests) {
  RUN_REDIS_TEST(object_reconstruction_test);
  RUN_REDIS_TEST(object_reconstruction_recursive_test);
  RUN_REDIS_TEST(object_reconstruction_suppression_test);
  RUN_TEST(task_dependency_test);
  RUN_TEST(task_multi_dependency_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(photon_tests);
  GREATEST_MAIN_END();
}
