#include "greatest.h"

#include "event_loop.h"
#include "test_common.h"
#include "common.h"
#include "state/object_table.h"
#include "state/redis.h"

#include <unistd.h>
#include <ae.h>

SUITE(task_table_tests);

event_loop *g_loop;

/* ==== Test operations in non-failure scenario ==== */

/* === A lookup of a task not in the table === */

TaskID lookup_nil_id;
int lookup_nil_success = 0;
const char *lookup_nil_context = "lookup_nil";

void lookup_nil_fail_callback(UniqueID id,
                              void *user_context,
                              void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

void lookup_nil_success_callback(Task *task, void *context) {
  lookup_nil_success = 1;
  CHECK(task == NULL);
  CHECK(context == (void *) lookup_nil_context);
  event_loop_stop(g_loop);
}

TEST lookup_nil_test(void) {
  lookup_nil_id = globally_unique_id();
  g_loop = event_loop_create();
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 0, NULL);
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 1000,
      .fail_callback = lookup_nil_fail_callback,
  };
  task_table_get_task(db, lookup_nil_id, &retry, lookup_nil_success_callback,
                      (void *) lookup_nil_context);
  /* Disconnect the database to see if the lookup times out. */
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(lookup_nil_success);
  PASS();
}

/* === A lookup of a task after it's added returns the same spec === */

int add_success = 0;
int lookup_success = 0;
Task *add_lookup_task;
const char *add_lookup_context = "add_lookup";

void add_lookup_fail_callback(UniqueID id,
                              void *user_context,
                              void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

void lookup_success_callback(Task *task, void *context) {
  lookup_success = 1;
  CHECK(memcmp(task, add_lookup_task, Task_size(task)) == 0);
  event_loop_stop(g_loop);
}

void add_success_callback(TaskID task_id, void *context) {
  add_success = 1;
  CHECK(TaskID_equal(task_id, Task_task_id(add_lookup_task)));

  DBHandle *db = (DBHandle *) context;
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 1000,
      .fail_callback = add_lookup_fail_callback,
  };
  task_table_get_task(db, task_id, &retry, lookup_success_callback,
                      (void *) add_lookup_context);
}

TEST add_lookup_test(void) {
  add_lookup_task = example_task(1, 1, TASK_STATUS_WAITING);
  g_loop = event_loop_create();
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 0, NULL);
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 1000,
      .fail_callback = add_lookup_fail_callback,
  };
  task_table_add_task(db, Task_copy(add_lookup_task), &retry,
                      add_success_callback, (void *) db);
  /* Disconnect the database to see if the lookup times out. */
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(add_success);
  ASSERT(lookup_success);
  PASS();
}

/* ==== Test if operations time out correctly ==== */

/* === Test subscribe timeout === */

const char *subscribe_timeout_context = "subscribe_timeout";
int subscribe_failed = 0;

void subscribe_done_callback(TaskID task_id, void *user_context) {
  /* The done callback should not be called. */
  CHECK(0);
}

void subscribe_fail_callback(UniqueID id, void *user_context, void *user_data) {
  subscribe_failed = 1;
  CHECK(user_context == (void *) subscribe_timeout_context);
  event_loop_stop(g_loop);
}

TEST subscribe_timeout_test(void) {
  g_loop = event_loop_create();
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 0, NULL);
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = subscribe_fail_callback,
  };
  task_table_subscribe(db, NIL_ID, TASK_STATUS_WAITING, NULL, NULL, &retry,
                       subscribe_done_callback,
                       (void *) subscribe_timeout_context);
  /* Disconnect the database to see if the subscribe times out. */
  close(db->sub_context->c.fd);
  aeProcessEvents(g_loop, AE_TIME_EVENTS);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(subscribe_failed);
  PASS();
}

/* === Test publish timeout === */

const char *publish_timeout_context = "publish_timeout";
const int publish_test_number = 272;
int publish_failed = 0;

void publish_done_callback(TaskID task_id, void *user_context) {
  /* The done callback should not be called. */
  CHECK(0);
}

void publish_fail_callback(UniqueID id, void *user_context, void *user_data) {
  publish_failed = 1;
  CHECK(user_context == (void *) publish_timeout_context);
  event_loop_stop(g_loop);
}

TEST publish_timeout_test(void) {
  g_loop = event_loop_create();
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 0, NULL);
  db_attach(db, g_loop, false);
  Task *task = example_task(1, 1, TASK_STATUS_WAITING);
  RetryInfo retry = {
      .num_retries = 5, .timeout = 100, .fail_callback = publish_fail_callback,
  };
  task_table_add_task(db, task, &retry, publish_done_callback,
                      (void *) publish_timeout_context);
  /* Disconnect the database to see if the publish times out. */
  close(db->context->c.fd);
  aeProcessEvents(g_loop, AE_TIME_EVENTS);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(publish_failed);
  PASS();
}

/* ==== Test if the retry is working correctly ==== */

int64_t reconnect_db_callback(event_loop *loop,
                              int64_t timer_id,
                              void *context) {
  DBHandle *db = (DBHandle *) context;
  /* Reconnect to redis. */
  redisAsyncFree(db->sub_context);
  db->sub_context = redisAsyncConnect("127.0.0.1", 6379);
  db->sub_context->data = (void *) db;
  /* Re-attach the database to the event loop (the file descriptor changed). */
  db_attach(db, loop, true);
  return EVENT_LOOP_TIMER_DONE;
}

int64_t terminate_event_loop_callback(event_loop *loop,
                                      int64_t timer_id,
                                      void *context) {
  event_loop_stop(loop);
  return EVENT_LOOP_TIMER_DONE;
}

/* === Test subscribe retry === */

const char *subscribe_retry_context = "subscribe_retry";
const int subscribe_retry_test_number = 273;
int subscribe_retry_succeeded = 0;

void subscribe_retry_done_callback(ObjectID object_id, void *user_context) {
  CHECK(user_context == (void *) subscribe_retry_context);
  subscribe_retry_succeeded = 1;
}

void subscribe_retry_fail_callback(UniqueID id,
                                   void *user_context,
                                   void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

TEST subscribe_retry_test(void) {
  g_loop = event_loop_create();
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 0, NULL);
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = subscribe_retry_fail_callback,
  };
  task_table_subscribe(db, NIL_ID, TASK_STATUS_WAITING, NULL, NULL, &retry,
                       subscribe_retry_done_callback,
                       (void *) subscribe_retry_context);
  /* Disconnect the database to see if the subscribe times out. */
  close(db->sub_context->c.fd);
  /* Install handler for reconnecting the database. */
  event_loop_add_timer(g_loop, 150,
                       (event_loop_timer_handler) reconnect_db_callback, db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(subscribe_retry_succeeded);
  PASS();
}

/* === Test publish retry === */

const char *publish_retry_context = "publish_retry";
int publish_retry_succeeded = 0;

void publish_retry_done_callback(ObjectID object_id, void *user_context) {
  CHECK(user_context == (void *) publish_retry_context);
  publish_retry_succeeded = 1;
}

void publish_retry_fail_callback(UniqueID id,
                                 void *user_context,
                                 void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

TEST publish_retry_test(void) {
  g_loop = event_loop_create();
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 0, NULL);
  db_attach(db, g_loop, false);
  Task *task = example_task(1, 1, TASK_STATUS_WAITING);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = publish_retry_fail_callback,
  };
  task_table_add_task(db, task, &retry, publish_retry_done_callback,
                      (void *) publish_retry_context);
  /* Disconnect the database to see if the publish times out. */
  close(db->sub_context->c.fd);
  /* Install handler for reconnecting the database. */
  event_loop_add_timer(g_loop, 150,
                       (event_loop_timer_handler) reconnect_db_callback, db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(publish_retry_succeeded);
  PASS();
}

/* ==== Test if late succeed is working correctly ==== */

/* === Test subscribe late succeed === */

const char *subscribe_late_context = "subscribe_late";
int subscribe_late_failed = 0;

void subscribe_late_fail_callback(UniqueID id,
                                  void *user_context,
                                  void *user_data) {
  CHECK(user_context == (void *) subscribe_late_context);
  subscribe_late_failed = 1;
}

void subscribe_late_done_callback(TaskID task_id, void *user_context) {
  /* This function should never be called. */
  CHECK(0);
}

TEST subscribe_late_test(void) {
  g_loop = event_loop_create();
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 0, NULL);
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 0,
      .timeout = 0,
      .fail_callback = subscribe_late_fail_callback,
  };
  task_table_subscribe(db, NIL_ID, TASK_STATUS_WAITING, NULL, NULL, &retry,
                       subscribe_late_done_callback,
                       (void *) subscribe_late_context);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* First process timer events to make sure the timeout is processed before
   * anything else. */
  aeProcessEvents(g_loop, AE_TIME_EVENTS);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(subscribe_late_failed);
  PASS();
}

/* === Test publish late succeed === */

const char *publish_late_context = "publish_late";
int publish_late_failed = 0;

void publish_late_fail_callback(UniqueID id,
                                void *user_context,
                                void *user_data) {
  CHECK(user_context == (void *) publish_late_context);
  publish_late_failed = 1;
}

void publish_late_done_callback(TaskID task_id, void *user_context) {
  /* This function should never be called. */
  CHECK(0);
}

TEST publish_late_test(void) {
  g_loop = event_loop_create();
  DBHandle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 0, NULL);
  db_attach(db, g_loop, false);
  Task *task = example_task(1, 1, TASK_STATUS_WAITING);
  RetryInfo retry = {
      .num_retries = 0,
      .timeout = 0,
      .fail_callback = publish_late_fail_callback,
  };
  task_table_add_task(db, task, &retry, publish_late_done_callback,
                      (void *) publish_late_context);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* First process timer events to make sure the timeout is processed before
   * anything else. */
  aeProcessEvents(g_loop, AE_TIME_EVENTS);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(publish_late_failed);
  PASS();
}

SUITE(task_table_tests) {
  RUN_REDIS_TEST(lookup_nil_test);
  RUN_REDIS_TEST(add_lookup_test);
  RUN_REDIS_TEST(subscribe_timeout_test);
  RUN_REDIS_TEST(publish_timeout_test);
  RUN_REDIS_TEST(subscribe_retry_test);
  RUN_REDIS_TEST(publish_retry_test);
  RUN_REDIS_TEST(subscribe_late_test);
  RUN_REDIS_TEST(publish_late_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(task_table_tests);
  GREATEST_MAIN_END();
}
