#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>

#include "event_loop.h"
#include "test_common.h"
#include "state/db.h"
#include "state/object_table.h"
#include "state/task_table.h"
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

TEST object_table_lookup_test(void) {
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
  object_table_add(db1, id, &retry, add_done_callback, NULL);
  object_table_add(db2, id, &retry, add_done_callback, NULL);
  event_loop_add_timer(loop, 200, (event_loop_timer_handler) timeout_handler,
                       NULL);
  event_loop_run(loop);
  object_table_lookup(db1, id, &retry, lookup_done_callback, NULL);
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

int task_table_test_callback_called = 0;
task *task_table_test_task;

void task_table_test_fail_callback(unique_id id,
                                   void *context,
                                   void *user_data) {
  event_loop *loop = user_data;
  event_loop_stop(loop);
}

int64_t task_table_delayed_add_task(event_loop *loop,
                                    int64_t id,
                                    void *context) {
  db_handle *db = context;
  retry_info retry = {
      .num_retries = NUM_RETRIES,
      .timeout = TIMEOUT,
      .fail_callback = task_table_test_fail_callback,
  };
  task_table_add_task(db, task_table_test_task, &retry, NULL, (void *) loop);
  return EVENT_LOOP_TIMER_DONE;
}

void task_table_test_callback(task *callback_task, void *user_data) {
  task_table_test_callback_called = 1;
  CHECK(task_state(callback_task) == TASK_STATUS_SCHEDULED);
  CHECK(task_size(callback_task) == task_size(task_table_test_task));
  CHECK(memcmp(callback_task, task_table_test_task, task_size(callback_task)) ==
        0);
  event_loop *loop = user_data;
  event_loop_stop(loop);
}

TEST task_table_test(void) {
  task_table_test_callback_called = 0;
  event_loop *loop = event_loop_create();
  db_handle *db = db_connect("127.0.0.1", 6379, "local_scheduler", "", -1);
  db_attach(db, loop);
  node_id node = globally_unique_id();
  task_spec *spec = example_task_spec();
  task_table_test_task = alloc_task(spec, TASK_STATUS_SCHEDULED, node);
  free_task_spec(spec);
  retry_info retry = {
      .num_retries = NUM_RETRIES,
      .timeout = TIMEOUT,
      .fail_callback = task_table_test_fail_callback,
  };
  task_table_subscribe(db, node, TASK_STATUS_SCHEDULED,
                       task_table_test_callback, (void *) loop, &retry, NULL,
                       (void *) loop);
  event_loop_add_timer(
      loop, 200, (event_loop_timer_handler) task_table_delayed_add_task, db);
  event_loop_run(loop);
  free_task(task_table_test_task);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(task_table_test_callback_called);
  PASS();
}

int num_test_callback_called = 0;

void task_table_all_test_callback(task *task, void *user_data) {
  num_test_callback_called += 1;
}

TEST task_table_all_test(void) {
  event_loop *loop = event_loop_create();
  db_handle *db = db_connect("127.0.0.1", 6379, "local_scheduler", "", -1);
  db_attach(db, loop);
  task_spec *spec = example_task_spec();
  /* Schedule two tasks on different nodes. */
  task *task1 = alloc_task(spec, TASK_STATUS_SCHEDULED, globally_unique_id());
  task *task2 = alloc_task(spec, TASK_STATUS_SCHEDULED, globally_unique_id());
  retry_info retry = {
      .num_retries = NUM_RETRIES, .timeout = TIMEOUT, .fail_callback = NULL,
  };
  task_table_subscribe(db, NIL_ID, TASK_STATUS_SCHEDULED,
                       task_table_all_test_callback, NULL, &retry, NULL, NULL);
  event_loop_add_timer(loop, 50, (event_loop_timer_handler) timeout_handler,
                       NULL);
  event_loop_run(loop);
  /* TODO(pcm): Get rid of this sleep once the robust pubsub is implemented. */
  task_table_update(db, task1, &retry, NULL, NULL);
  task_table_update(db, task2, &retry, NULL, NULL);
  event_loop_add_timer(loop, 200, (event_loop_timer_handler) timeout_handler,
                       NULL);
  event_loop_run(loop);
  free(task2);
  free(task1);
  free_task_spec(spec);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(num_test_callback_called == 2);
  PASS();
}

TEST unique_client_id_test(void) {
  const int num_conns = 100;

  client_id ids[num_conns];
  db_handle *db;
  for (int i = 0; i < num_conns; ++i) {
    db = db_connect("127.0.0.1", 6379, "plasma_manager", manager_addr,
                    manager_port1);
    ids[i] = get_client_id(db);
    db_disconnect(db);
  }
  for (int i = 0; i < num_conns; ++i) {
    for (int j = 0; j < i; ++j) {
      ASSERT(!client_ids_equal(ids[i], ids[j]));
    }
  }
  PASS();
}

SUITE(db_tests) {
  RUN_REDIS_TEST(object_table_lookup_test);
  RUN_REDIS_TEST(task_table_test);
  RUN_REDIS_TEST(task_table_all_test);
  RUN_REDIS_TEST(unique_client_id_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(db_tests);
  GREATEST_MAIN_END();
}
