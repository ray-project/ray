#include "greatest.h"

#include <assert.h>

#include "event_loop.h"
#include "state/db.h"
#include "state/object_table.h"
#include "state/redis.h"
#include "task.h"

SUITE(db_tests);

int lookup_successful = 0;
const char *manager_addr = "127.0.0.1";
int manager_port1 = 12345;
int manager_port2 = 12346;
char received_addr1[16] = {0};
char received_port1[6] = {0};
char received_addr2[16] = {0};
char received_port2[6] = {0};

/* This is for synchronizing to make sure both entries have been written. */
void sync_test_callback(object_id object_id,
                        int manager_count,
                        const char *manager_vector[]) {
  lookup_successful = 1;
  free(manager_vector);
}

/* This performs the actual test. */
void test_callback(object_id object_id,
                   int manager_count,
                   const char *manager_vector[]) {
  CHECK(manager_count == 2);
  lookup_successful = 1;
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

TEST object_table_lookup_test(void) {
  event_loop loop;
  event_loop_init(&loop);
  db_conn conn1;
  db_connect("127.0.0.1", 6379, "plasma_manager", manager_addr, manager_port1,
             &conn1);
  db_conn conn2;
  db_connect("127.0.0.1", 6379, "plasma_manager", manager_addr, manager_port2,
             &conn2);
  int64_t index1 = db_attach(&conn1, &loop, 0);
  int64_t index2 = db_attach(&conn2, &loop, 1);
  unique_id id = globally_unique_id();
  object_table_add(&conn1, id);
  object_table_add(&conn2, id);
  object_table_lookup(&conn1, id, sync_test_callback);
  while (!lookup_successful) {
    int num_ready = event_loop_poll(&loop);
    if (num_ready < 0) {
      exit(-1);
    }
    for (int i = 0; i < event_loop_size(&loop); ++i) {
      struct pollfd *waiting = event_loop_get(&loop, i);
      if (waiting->revents == 0)
        continue;
      if (i == index1) {
        db_event(&conn1);
      }
      if (i == index2) {
        db_event(&conn2);
      }
    }
  }
  lookup_successful = 0;
  object_table_lookup(&conn1, id, test_callback);
  while (!lookup_successful) {
    int num_ready = event_loop_poll(&loop);
    if (num_ready < 0) {
      exit(-1);
    }
    for (int i = 0; i < event_loop_size(&loop); ++i) {
      struct pollfd *waiting = event_loop_get(&loop, i);
      if (waiting->revents == 0)
        continue;
      if (i == index1) {
        db_event(&conn1);
      }
      if (i == index2) {
        db_event(&conn2);
      }
    }
  }
  int port1 = atoi(received_port1);
  int port2 = atoi(received_port2);
  ASSERT_STR_EQ(&received_addr1[0], manager_addr);
  ASSERT((port1 == manager_port1 && port2 == manager_port2) ||
         (port2 == manager_port1 && port1 == manager_port2));

  db_disconnect(&conn1);
  db_disconnect(&conn2);

  event_loop_free(&loop);

  PASS();
}

SUITE(db_tests) {
  RUN_TEST(object_table_lookup_test);
  /* RUN_TEST(task_queue_test); */
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(db_tests);
  GREATEST_MAIN_END();
}
