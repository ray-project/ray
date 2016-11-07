#include "greatest.h"

#include "event_loop.h"
#include "test_common.h"
#include "common.h"
#include "state/task_table.h"
#include "state/redis.h"

#include <unistd.h>

SUITE(task_table_tests);

static event_loop *g_loop;

/* ==== Test operations in non-failure scenario ==== */

/* === A lookup of a task not in the table === */

task_id lookup_nil_id;
int lookup_nil_success = 0;
const char *lookup_nil_context = "lookup_nil";

void lookup_nil_fail_callback(unique_id id,
                              void *user_context,
                              void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

void lookup_nil_success_callback(task_id task_id,
                                 task_spec *task,
                                 void *context) {
  lookup_nil_success = 1;
  CHECK(context == (void *) lookup_nil_context);
  CHECK(memcmp(task_id.id, lookup_nil_id.id, UNIQUE_ID_SIZE) == 0);
  CHECK(task == NULL);
  event_loop_stop(g_loop);
}

TEST lookup_nil_test(void) {
  lookup_nil_id = globally_unique_id();
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, g_loop);
  retry_info retry = {
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
task_id add_lookup_id;
task_spec *add_lookup_task;
const char *add_lookup_context = "add_lookup";

void add_lookup_fail_callback(unique_id id,
                              void *user_context,
                              void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

void lookup_success_callback(task_id task_id, task_spec *task, void *context) {
  lookup_success = 1;
  CHECK(context == (void *) add_lookup_context);
  CHECK(memcmp(task_id.id, add_lookup_id.id, UNIQUE_ID_SIZE) == 0);
  CHECK(memcmp(task, add_lookup_task, task_size(task)) == 0);
  event_loop_stop(g_loop);
}

void add_success_callback(task_id task_id, task_spec *task, void *context) {
  add_success = 1;
  CHECK(memcmp(task_id.id, add_lookup_id.id, UNIQUE_ID_SIZE) == 0);
  CHECK(task == add_lookup_task);

  db_handle *db = context;
  retry_info retry = {
      .num_retries = 5,
      .timeout = 1000,
      .fail_callback = add_lookup_fail_callback,
  };
  task_table_get_task(db, task_id, &retry, lookup_success_callback,
                      (void *) add_lookup_context);
}

TEST add_lookup_test(void) {
  add_lookup_id = globally_unique_id();
  add_lookup_task = example_task();
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 1000,
      .fail_callback = add_lookup_fail_callback,
  };
  task_table_add_task(db, add_lookup_id, add_lookup_task, &retry,
                      add_success_callback, (void *) db);
  /* Disconnect the database to see if the lookup times out. */
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  free_task_spec(add_lookup_task);
  ASSERT(add_success);
  ASSERT(lookup_success);
  PASS();
}

/* ==== Test if operations time out correctly ==== */

/* === Test get task timeout === */

const char *lookup_timeout_context = "lookup_timeout";
int lookup_failed = 0;

void lookup_done_callback(task_id task_id, task_spec *task, void *context) {
  /* The done callback should not be called. */
  CHECK(0);
}

void lookup_fail_callback(unique_id id, void *user_context, void *user_data) {
  lookup_failed = 1;
  CHECK(user_context == (void *) lookup_timeout_context);
  event_loop_stop(g_loop);
}

TEST lookup_timeout_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5, .timeout = 100, .fail_callback = lookup_fail_callback,
  };
  task_table_get_task(db, NIL_ID, &retry, lookup_done_callback,
                      (void *) lookup_timeout_context);
  /* Disconnect the database to see if the lookup times out. */
  close(db->context->c.fd);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(lookup_failed);
  PASS();
}

/* === Test add timeout === */

const char *add_timeout_context = "add_timeout";
int add_failed = 0;

void add_done_callback(task_id task_id, task_spec *task, void *context) {
  /* The done callback should not be called. */
  CHECK(0);
}

void add_fail_callback(unique_id id, void *user_context, void *user_data) {
  add_failed = 1;
  CHECK(user_context == (void *) add_timeout_context);
  event_loop_stop(g_loop);
}

TEST add_timeout_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5, .timeout = 100, .fail_callback = add_fail_callback,
  };
  task_table_get_task(db, NIL_ID, &retry, add_done_callback,
                   (void *) add_timeout_context);
  /* Disconnect the database to see if the lookup times out. */
  close(db->context->c.fd);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(add_failed);
  PASS();
}

/* ==== Test if the retry is working correctly ==== */

int64_t reconnect_context_callback(event_loop *loop,
                                   int64_t timer_id,
                                   void *context) {
  db_handle *db = context;
  /* Reconnect to redis. This is not reconnecting the pub/sub channel. */
  redisAsyncFree(db->context);
  redisFree(db->sync_context);
  db->context = redisAsyncConnect("127.0.0.1", 6379);
  db->context->data = (void *) db;
  db->sync_context = redisConnect("127.0.0.1", 6379);
  /* Re-attach the database to the event loop (the file descriptor changed). */
  db_attach(db, loop);
  return EVENT_LOOP_TIMER_DONE;
}

int64_t terminate_event_loop_callback(event_loop *loop,
                                      int64_t timer_id,
                                      void *context) {
  event_loop_stop(loop);
  return EVENT_LOOP_TIMER_DONE;
}

/* === Test lookup retry === */

const char *lookup_retry_context = "lookup_retry";
int lookup_retry_succeeded = 0;

void lookup_retry_done_callback(task_id task_id,
                                task_spec *task,
                                void *context) {
  CHECK(context == (void *) lookup_retry_context);
  lookup_retry_succeeded = 1;
}

void lookup_retry_fail_callback(unique_id id,
                                void *user_context,
                                void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

TEST lookup_retry_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11235);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = lookup_retry_fail_callback,
  };
  task_table_get_task(db, NIL_ID, &retry, lookup_retry_done_callback,
                      (void *) lookup_retry_context);
  /* Disconnect the database to let the lookup time out the first time. */
  close(db->context->c.fd);
  /* Install handler for reconnecting the database. */
  event_loop_add_timer(
      g_loop, 150, (event_loop_timer_handler) reconnect_context_callback, db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(lookup_retry_succeeded);
  PASS();
}

/* === Test add retry === */

const char *add_retry_context = "add_retry";
int add_retry_succeeded = 0;

void add_retry_done_callback(task_id task_id,
                             task_spec *task,
                             void *user_context) {
  CHECK(user_context == (void *) add_retry_context);
  add_retry_succeeded = 1;
  free_task_spec(task);
}

void add_retry_fail_callback(unique_id id,
                             void *user_context,
                             void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

TEST add_retry_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11235);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = add_retry_fail_callback,
  };
  task_spec *task = example_task();
  task_table_add_task(db, NIL_ID, task, &retry, add_retry_done_callback,
                   (void *) add_retry_context);
  /* Disconnect the database to let the add time out the first time. */
  close(db->context->c.fd);
  /* Install handler for reconnecting the database. */
  event_loop_add_timer(
      g_loop, 150, (event_loop_timer_handler) reconnect_context_callback, db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(add_retry_succeeded);
  PASS();
}

/* ==== Test if late succeed is working correctly ==== */

/* === Test lookup late succeed === */

const char *lookup_late_context = "lookup_late";
int lookup_late_failed = 0;

void lookup_late_fail_callback(unique_id id,
                               void *user_context,
                               void *user_data) {
  CHECK(user_context == (void *) lookup_late_context);
  lookup_late_failed = 1;
}

void lookup_late_done_callback(task_id task_id,
                               task_spec *task,
                               void *context) {
  /* This function should never be called. */
  CHECK(0);
}

TEST lookup_late_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 0,
      .timeout = 0,
      .fail_callback = lookup_late_fail_callback,
  };
  task_table_get_task(db, NIL_ID, &retry, lookup_late_done_callback,
                      (void *) lookup_late_context);
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
  ASSERT(lookup_late_failed);
  PASS();
}

/* === Test add late succeed === */

const char *add_late_context = "add_late";
int add_late_failed = 0;

void add_late_fail_callback(unique_id id, void *user_context, void *user_data) {
  CHECK(user_context == (void *) add_late_context);
  add_late_failed = 1;
  task_spec *task = user_data;
  free_task_spec(task);
}

void add_late_done_callback(task_id task_id,
                            task_spec *task,
                            void *user_context) {
  /* This function should never be called. */
  CHECK(0);
}

TEST add_late_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 0, .timeout = 0, .fail_callback = add_late_fail_callback,
  };
  task_spec *task = example_task();
  task_table_add_task(db, NIL_ID, task, &retry, add_late_done_callback,
                   (void *) add_late_context);
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
  ASSERT(add_late_failed);
  PASS();
}

SUITE(task_table_tests) {
  RUN_REDIS_TEST(lookup_nil_test);
  RUN_REDIS_TEST(add_lookup_test);
  RUN_REDIS_TEST(lookup_timeout_test);
  RUN_REDIS_TEST(add_timeout_test);
  RUN_REDIS_TEST(lookup_retry_test);
  RUN_REDIS_TEST(add_retry_test);
  RUN_REDIS_TEST(lookup_late_test);
  RUN_REDIS_TEST(add_late_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(task_table_tests);
  GREATEST_MAIN_END();
}
