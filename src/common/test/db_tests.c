#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>

#include "event_loop.h"
#include "test_common.h"
#include "state/db.h"
#include "state/object_table.h"
#include "state/task_log.h"
#include "state/redis.h"
#include "task.h"

SUITE(db_tests);

/* Retry 10 times with an 100ms timeout. */
const int NUM_RETRIES = 10;
const uint64_t TIMEOUT = 50;

const char *manager_addr = "127.0.0.1";
int manager_port1 = 12345;
int manager_port2 = 12346;
char received_addr1[16] = {0};
char received_port1[6] = {0};
char received_addr2[16] = {0};
char received_port2[6] = {0};

typedef struct { int test_number; } user_context;

const int TEST_NUMBER = 10;

/* Test if entries have been written to the database. */

void lookup_done_callback(object_id object_id,
                          int manager_count,
                          const char *manager_vector[],
                          void *user_context) {
  CHECK(manager_count == 2);
  if (!manager_vector[0] ||
      sscanf(manager_vector[0], "%15[0-9.]:%5[0-9]", received_addr1,
             received_port1) != 2) {
    CHECK(0);
  }
  if (!manager_vector[1] ||
      sscanf(manager_vector[1], "%15[0-9.]:%5[0-9]", received_addr2,
             received_port2) != 2) {
    CHECK(0);
  }
  free(manager_vector);
}

/* Entry added to database successfully. */
void add_done_callback(object_id object_id, void *user_context) {}

/* Test if we got a timeout callback if we couldn't connect database. */
void timeout_callback(object_id object_id, void *context, void *user_data) {
  user_context *uc = (user_context *) context;
  CHECK(uc->test_number == TEST_NUMBER)
}

int64_t timeout_handler(event_loop *loop, int64_t id, void *context) {
  event_loop_stop(loop);
  return EVENT_LOOP_TIMER_DONE;
}

TEST object_table_lookup_location_test(void) {
  event_loop *loop = event_loop_create();
  db_handle *db1 = db_connect("127.0.0.1", 6379, "plasma_manager", manager_addr,
                              manager_port1);
  db_handle *db2 = db_connect("127.0.0.1", 6379, "plasma_manager", manager_addr,
                              manager_port2);
  db_attach(db1, loop);
  db_attach(db2, loop);
  unique_id id = globally_unique_id();
  retry_info retry = {
      .num_retries = NUM_RETRIES,
      .timeout = TIMEOUT,
      .fail_callback = timeout_callback,
  };
  object_table_add_location(db1, id, &retry, add_done_callback, NULL);
  object_table_add_location(db2, id, &retry, add_done_callback, NULL);
  event_loop_add_timer(loop, 200, (event_loop_timer_handler) timeout_handler,
                       NULL);
  event_loop_run(loop);
  object_table_lookup_location(db1, id, &retry, lookup_done_callback, NULL);
  event_loop_add_timer(loop, 200, (event_loop_timer_handler) timeout_handler,
                       NULL);
  event_loop_run(loop);
  int port1 = atoi(received_port1);
  int port2 = atoi(received_port2);
  ASSERT_STR_EQ(&received_addr1[0], manager_addr);
  ASSERT((port1 == manager_port1 && port2 == manager_port2) ||
         (port2 == manager_port1 && port1 == manager_port2));

  db_disconnect(db1);
  db_disconnect(db2);

  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  PASS();
}

void task_log_test_callback(task_instance *instance, void *userdata) {
  task_instance *other = userdata;
  CHECK(*task_instance_state(instance) == TASK_STATUS_SCHEDULED);
  CHECK(task_instance_size(instance) == task_instance_size(other));
  CHECK(memcmp(instance, other, task_instance_size(instance)) == 0);
}

TEST task_log_test(void) {
  event_loop *loop = event_loop_create();
  db_handle *db = db_connect("127.0.0.1", 6379, "local_scheduler", "", -1);
  db_attach(db, loop);
  node_id node = globally_unique_id();
  task_spec *task = example_task();
  task_instance *instance = make_task_instance(globally_unique_id(), task,
                                               TASK_STATUS_SCHEDULED, node);
  retry_info retry = {
      .num_retries = NUM_RETRIES, .timeout = TIMEOUT, .fail_callback = NULL,
  };
  task_log_subscribe(db, node, TASK_STATUS_SCHEDULED, task_log_test_callback,
                     instance, &retry, NULL, NULL);
  task_log_publish(db, instance, &retry, NULL, NULL);
  event_loop_add_timer(loop, 200, (event_loop_timer_handler) timeout_handler,
                       NULL);
  event_loop_run(loop);
  task_instance_free(instance);
  free_task_spec(task);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  PASS();
}

int num_test_callback_called = 0;

void task_log_all_test_callback(task_instance *instance, void *userdata) {
  num_test_callback_called += 1;
}

TEST task_log_all_test(void) {
  event_loop *loop = event_loop_create();
  db_handle *db = db_connect("127.0.0.1", 6379, "local_scheduler", "", -1);
  db_attach(db, loop);
  task_spec *task = example_task();
  /* Schedule two tasks on different nodes. */
  task_instance *instance1 = make_task_instance(
      globally_unique_id(), task, TASK_STATUS_SCHEDULED, globally_unique_id());
  task_instance *instance2 = make_task_instance(
      globally_unique_id(), task, TASK_STATUS_SCHEDULED, globally_unique_id());
  retry_info retry = {
      .num_retries = NUM_RETRIES, .timeout = TIMEOUT, .fail_callback = NULL,
  };
  task_log_subscribe(db, NIL_ID, TASK_STATUS_SCHEDULED,
                     task_log_all_test_callback, NULL, &retry, NULL, NULL);
  event_loop_add_timer(loop, 50, (event_loop_timer_handler) timeout_handler,
                       NULL);
  event_loop_run(loop);
  /* TODO(pcm): Get rid of this sleep once the robust pubsub is implemented. */
  task_log_publish(db, instance1, &retry, NULL, NULL);
  task_log_publish(db, instance2, &retry, NULL, NULL);
  event_loop_add_timer(loop, 200, (event_loop_timer_handler) timeout_handler,
                       NULL);
  event_loop_run(loop);
  task_instance_free(instance2);
  task_instance_free(instance1);
  free_task_spec(task);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(num_test_callback_called == 2);
  PASS();
}

TEST unique_client_id_test(void) {
  const int num_conns = 50;

  db_handle *db;
  pid_t pid = fork();
  for (int i = 0; i < num_conns; ++i) {
    db = db_connect("127.0.0.1", 6379, "plasma_manager", manager_addr,
                    manager_port1);
    db_disconnect(db);
  }
  if (pid == 0) {
    exit(0);
  } else {
    wait(NULL);
  }

  db = db_connect("127.0.0.1", 6379, "plasma_manager", manager_addr,
                  manager_port1);
  ASSERT_EQ(get_client_id(db), num_conns * 2);
  db_disconnect(db);
  PASS();
}

SUITE(db_tests) {
  RUN_REDIS_TEST(object_table_lookup_location_test);
  RUN_REDIS_TEST(task_log_test);
  RUN_REDIS_TEST(task_log_all_test);
  RUN_REDIS_TEST(unique_client_id_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(db_tests);
  GREATEST_MAIN_END();
}
