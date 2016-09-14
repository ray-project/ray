#include "greatest.h"

#include <assert.h>

#include "event_loop.h"
#include "state/db.h"
#include "state/object_table.h"
#include "state/redis.h"

SUITE(db_tests);

int lookup_successful = 0;
const char *manager_addr = "127.0.0.1";
int manager_port = 12345;
char received_addr[16] = {0};
char received_port[6] = {0};

void test_callback(void *userdata);

void test_callback(void *userdata) {
  char *reply = userdata;
  lookup_successful = 1;
  if (!reply ||
      sscanf(reply, "%15[0-9.]:%5[0-9]", received_addr, received_port) != 2) {
    assert(0);
  }
  free(reply);
}

TEST object_table_lookup_test(void) {
  event_loop loop;
  event_loop_init(&loop);
  db_conn conn;
  db_connect("127.0.0.1", 6379, "plasma_manager", manager_addr, manager_port,
             &conn);
  int64_t index = db_attach(&conn, &loop, 0);
  unique_id id = {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}};
  object_table_add(&conn, id);
  object_table_lookup(&conn, id, test_callback);
  while (!lookup_successful) {
    int num_ready = event_loop_poll(&loop);
    if (num_ready < 0) {
      exit(-1);
    }
    for (int i = 0; i < event_loop_size(&loop); ++i) {
      struct pollfd *waiting = event_loop_get(&loop, i);
      if (waiting->revents == 0)
        continue;
      if (i == index) {
        db_event(&conn);
      }
    }
  }
  ASSERT_STR_EQ(&received_addr[0], manager_addr);
  ASSERT_EQ(atoi(received_port), manager_port);
  PASS();
}

SUITE(db_tests) {
  RUN_TEST(object_table_lookup_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(db_tests);
  GREATEST_MAIN_END();
}
