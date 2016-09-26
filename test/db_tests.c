#include "greatest.h"

#include <assert.h>

#include "event_loop.h"
#include "test/example_task.h"
#include "state/db.h"
#include "state/object_table.h"
#include "state/task_queue.h"
#include "state/redis.h"
#include "task.h"

SUITE(db_tests);

const char *manager_addr = "127.0.0.1";
int manager_port1 = 12345;
int manager_port2 = 12346;
char received_addr1[16] = {0};
char received_port1[6] = {0};
char received_addr2[16] = {0};
char received_port2[6] = {0};

/* Test if entries have been written to the database. */
void test_callback(object_id object_id,
                   int manager_count,
                   const char *manager_vector[]) {
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

int64_t timeout_handler(event_loop *loop, int64_t id, void *context) {
  event_loop_stop(loop);
  return -1;
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
  object_table_add(db1, id);
  object_table_add(db2, id);
  event_loop_add_timer(loop, 100, timeout_handler, NULL);
  event_loop_run(loop);
  object_table_lookup(db1, id, test_callback);
  event_loop_add_timer(loop, 100, timeout_handler, NULL);
  event_loop_run(loop);
  int port1 = atoi(received_port1);
  int port2 = atoi(received_port2);
  ASSERT_STR_EQ(&received_addr1[0], manager_addr);
  ASSERT((port1 == manager_port1 && port2 == manager_port2) ||
         (port2 == manager_port1 && port1 == manager_port2));

  db_disconnect(db1);
  db_disconnect(db2);

  event_loop_destroy(loop);
  PASS();
}

TEST task_queue_test(void) {
  event_loop *loop = event_loop_create();
  db_handle *db = db_connect("127.0.0.1", 6379, "local_scheduler", "", -1);
  db_attach(db, loop);

  task_spec *task = example_task();
  task_queue_submit_task(db, globally_unique_id(), task);
  event_loop_add_timer(loop, 100, timeout_handler, NULL);
  event_loop_run(loop);

  free_task_spec(task);
  db_disconnect(db);
  event_loop_destroy(loop);
  PASS();
}

SUITE(db_tests) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  RUN_REDIS_TEST(context, object_table_lookup_test);
  RUN_REDIS_TEST(context, task_queue_test);
  redisFree(context);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(db_tests);
  GREATEST_MAIN_END();
}
