#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "common.h"
#include "test/test_common.h"
#include "event_loop.h"
#include "io.h"
#include "utstring.h"
#include "task.h"

#include "photon.h"
#include "photon_scheduler.h"
#include "photon_algorithm.h"
#include "photon_client.h"

SUITE(photon_tests);

const char *plasma_socket_name_format = "/tmp/plasma_socket_%d";
const char *photon_socket_name_format = "/tmp/photon_socket_%d";

typedef struct {
  /* A socket to mock the Plasma store. */
  int plasma_fd;
  int plasma_incoming_fd;
  /* Photon's socket for IPC requests. */
  int photon_fd;
  local_scheduler_state *photon_state;
  event_loop *loop;
  photon_conn *conn;
  int client_sock;
} photon_mock;

photon_mock *init_photon_mock() {
  const char *redis_addr = "127.0.0.1";
  int redis_port = 6379;
  photon_mock *mock = malloc(sizeof(photon_mock));
  memset(mock, 0, sizeof(photon_mock));
  mock->loop = event_loop_create();
  /* Bind to the Photon port and initialize the Photon scheduler. */
  UT_string *plasma_manager_socket_name =
      bind_ipc_sock_retry(plasma_socket_name_format, &mock->plasma_fd);
  UT_string *plasma_store_socket_name =
      bind_ipc_sock_retry(plasma_socket_name_format, &mock->plasma_fd);
  UT_string *photon_socket_name =
      bind_ipc_sock_retry(photon_socket_name_format, &mock->photon_fd);
  CHECK(mock->plasma_fd >= 0 && mock->photon_fd >= 0);
  mock->photon_state =
      init_local_scheduler(mock->loop, redis_addr, redis_port,
                           utstring_body(plasma_manager_socket_name),
                           utstring_body(plasma_store_socket_name), false);
  /* Connect a Photon client. */
  mock->conn = photon_connect(utstring_body(photon_socket_name));
  new_client_connection(mock->loop, mock->photon_fd,
                        (void *) mock->photon_state, 0);
  worker *w = (worker *) utarray_front(mock->photon_state->workers);
  mock->client_sock = w->sock;
  return mock;
}

void destroy_photon_mock(photon_mock *mock) {
  free(mock->conn);
  close(mock->photon_fd);
  close(mock->plasma_fd);
  free_local_scheduler(mock->photon_state);
}

TEST object_reconstruction_test(void) {
  photon_mock *photon = init_photon_mock();
  /* Run the event loop in a separate process. */
  pid_t pid = fork();
  if (pid == 0) {
    event_loop_run(photon->loop);
  }
  /* Create a task with zero dependencies and one return value. */
  task_spec *spec = example_task_spec(0, 1);
  /* Make sure we receive the task twice. First from the initial submission,
   * and second from the reconstruct request. */
  photon_submit(photon->conn, spec);
  object_id return_id = task_return(spec, 0);
  photon_reconstruct_object(photon->conn, return_id);
  task_spec *task_assigned = photon_get_task(photon->conn);
  ASSERT_EQ(memcmp(task_assigned, spec, task_spec_size(spec)), 0);
  task_spec *reconstruct_task = photon_get_task(photon->conn);
  ASSERT_EQ(memcmp(reconstruct_task, spec, task_spec_size(spec)), 0);
  destroy_photon_mock(photon);
  PASS();
}

TEST object_reconstruction_recursive_test(void) {
  photon_mock *photon = init_photon_mock();
  event_loop_add_file(photon->loop, photon->plasma_incoming_fd, EVENT_LOOP_READ,
                      process_plasma_notification,
                      (void *) photon->photon_state);
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
  /* Run the event loop in a separate process. */
  pid_t pid = fork();
  if (pid == 0) {
    event_loop_run(photon->loop);
  }
  /* Submit the tasks, and make sure each one gets assigned to a worker. */
  for (int i = 0; i < NUM_TASKS; ++i) {
    photon_submit(photon->conn, specs[i]);
  }
  /* Make sure we receive each task from the initial submission. */
  for (int i = 0; i < NUM_TASKS; ++i) {
    task_spec *task_assigned = photon_get_task(photon->conn);
    ASSERT_EQ(memcmp(task_assigned, specs[i], task_spec_size(task_assigned)),
              0);
  }
  /* Request reconstruction of the last return object. */
  object_id return_id = task_return(specs[9], 0);
  photon_reconstruct_object(photon->conn, return_id);
  /* Check that the workers receive all tasks in the final return object's
   * lineage during reconstruction. */
  for (int i = 0; i < NUM_TASKS; ++i) {
    task_spec *task_assigned = photon_get_task(photon->conn);
    bool found = false;
    for (int j = 0; j < NUM_TASKS; ++j) {
      if (specs[j] == NULL) {
        continue;
      }
      if (memcmp(task_assigned, specs[j], task_spec_size(task_assigned)) == 0) {
        found = true;
        specs[j] = NULL;
      }
    }
    ASSERT(found);
  }
  destroy_photon_mock(photon);
  PASS();
}

SUITE(photon_tests) {
  RUN_REDIS_TEST(object_reconstruction_test);
  RUN_REDIS_TEST(object_reconstruction_recursive_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(photon_tests);
  GREATEST_MAIN_END();
}
