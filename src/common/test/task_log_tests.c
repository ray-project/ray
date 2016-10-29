#include "greatest.h"

#include "event_loop.h"
#include "example_task.h"
#include "common.h"
#include "state/object_table.h"
#include "state/redis.h"

#include <unistd.h>
#include <ae.h>
// #include <hiredis/adapters/ae.h>

SUITE(task_log_tests);

event_loop *loop;

/* ==== Test if operations time out correctly ==== */

/* === Test subscribe timeout === */

const char *subscribe_timeout_context = "subscribe_timeout";
int subscribe_failed = 0;

void subscribe_done_callback(task_iid task_iid, void *user_context) {
  /* The done callback should not be called. */
  CHECK(0);
}

void subscribe_fail_callback(unique_id id, void *user_data) {
  subscribe_failed = 1;
  CHECK(user_data == (void *) subscribe_timeout_context);
  event_loop_stop(loop);
}

TEST subscribe_timeout_test(void) {
  loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = subscribe_fail_callback,
  };
  task_log_subscribe(db, NIL_ID, TASK_STATUS_WAITING, NULL, NULL, &retry,
                     subscribe_done_callback,
                     (void *) subscribe_timeout_context);
  /* Disconnect the database to see if the subscribe times out. */
  close(db->sub_context->c.fd);
  aeProcessEvents(loop, AE_TIME_EVENTS);
  event_loop_run(loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(subscribe_failed);
  PASS();
}

/* === Test publish timeout === */

const char *publish_timeout_context = "publish_timeout";
const int publish_test_number = 272;
int publish_failed = 0;

void publish_done_callback(task_iid task_iid, void *user_context) {
  /* The done callback should not be called. */
  CHECK(0);
}

void publish_fail_callback(unique_id id, void *user_data) {
  publish_failed = 1;
  CHECK(user_data == (void *) publish_timeout_context);
  event_loop_stop(loop);
}

TEST publish_timeout_test(void) {
  loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, loop);
  task_instance *task = example_task_instance();
  retry_info retry = {
      .num_retries = 5, .timeout = 100, .fail_callback = publish_fail_callback,
  };
  task_log_publish(db, task, &retry, publish_done_callback,
                   (void *) publish_timeout_context);
  /* Disconnect the database to see if the publish times out. */
  close(db->context->c.fd);
  aeProcessEvents(loop, AE_TIME_EVENTS);
  event_loop_run(loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(publish_failed);
  task_instance_free(task);
  PASS();
}

/* ==== Test if the retry is working correctly ==== */

int64_t reconnect_db_callback(event_loop *loop,
                              int64_t timer_id,
                              void *context) {
  db_handle *db = context;
  /* Reconnect to redis. */
  redisAsyncFree(db->sub_context);
  db->sub_context = redisAsyncConnect("127.0.0.1", 6379);
  db->sub_context->data = (void *) db;
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

/* === Test subscribe retry === */

const char *subscribe_retry_context = "subscribe_retry";
const int subscribe_retry_test_number = 273;
int subscribe_retry_succeeded = 0;

void subscribe_retry_done_callback(object_id object_id, void *user_context) {
  CHECK(user_context == (void *) subscribe_retry_context);
  subscribe_retry_succeeded = 1;
}

void subscribe_retry_fail_callback(unique_id id, void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

TEST subscribe_retry_test(void) {
  loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11235);
  db_attach(db, loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = subscribe_retry_fail_callback,
  };
  task_log_subscribe(db, NIL_ID, TASK_STATUS_WAITING, NULL, NULL, &retry,
                     subscribe_retry_done_callback,
                     (void *) subscribe_retry_context);
  /* Disconnect the database to see if the subscribe times out. */
  close(db->sub_context->c.fd);
  /* Install handler for reconnecting the database. */
  event_loop_add_timer(loop, 150,
                       (event_loop_timer_handler) reconnect_db_callback, db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  event_loop_run(loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(subscribe_retry_succeeded);
  PASS();
}

/* === Test publish retry === */

const char *publish_retry_context = "publish_retry";
int publish_retry_succeeded = 0;

void publish_retry_done_callback(object_id object_id, void *user_context) {
  CHECK(user_context == (void *) publish_retry_context);
  publish_retry_succeeded = 1;
}

void publish_retry_fail_callback(unique_id id, void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

TEST publish_retry_test(void) {
  loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11235);
  db_attach(db, loop);
  task_instance *task = example_task_instance();
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = publish_retry_fail_callback,
  };
  task_log_publish(db, task, &retry, publish_retry_done_callback,
                   (void *) publish_retry_context);
  /* Disconnect the database to see if the publish times out. */
  close(db->sub_context->c.fd);
  /* Install handler for reconnecting the database. */
  event_loop_add_timer(loop, 150,
                       (event_loop_timer_handler) reconnect_db_callback, db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  event_loop_run(loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(publish_retry_succeeded);
  task_instance_free(task);
  PASS();
}

/* ==== Test if late succeed is working correctly ==== */

/* === Test subscribe late succeed === */

const char *subscribe_late_context = "subscribe_late";
int subscribe_late_failed = 0;

void subscribe_late_fail_callback(unique_id id, void *user_context) {
  CHECK(user_context == (void *) subscribe_late_context);
  subscribe_late_failed = 1;
}

void subscribe_late_done_callback(task_iid task_iid, void *user_context) {
  /* This function should never be called. */
  CHECK(0);
}

TEST subscribe_late_test(void) {
  loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, loop);
  retry_info retry = {
      .num_retries = 0,
      .timeout = 0,
      .fail_callback = subscribe_late_fail_callback,
  };
  task_log_subscribe(db, NIL_ID, TASK_STATUS_WAITING, NULL, NULL, &retry,
                     subscribe_late_done_callback,
                     (void *) subscribe_late_context);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* First process timer events to make sure the timeout is processed before
   * anything else. */
  aeProcessEvents(loop, AE_TIME_EVENTS);
  event_loop_run(loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(subscribe_late_failed);
  PASS();
}

/* === Test publish late succeed === */

const char *publish_late_context = "publish_late";
int publish_late_failed = 0;

void publish_late_fail_callback(unique_id id, void *user_context) {
  CHECK(user_context == (void *) publish_late_context);
  publish_late_failed = 1;
}

void publish_late_done_callback(task_iid task_iik, void *user_context) {
  /* This function should never be called. */
  CHECK(0);
}

TEST publish_late_test(void) {
  loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, loop);
  task_instance *task = example_task_instance();
  retry_info retry = {
      .num_retries = 0,
      .timeout = 0,
      .fail_callback = publish_late_fail_callback,
  };
  task_log_publish(db, task, &retry, publish_late_done_callback,
                   (void *) publish_late_context);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* First process timer events to make sure the timeout is processed before
   * anything else. */
  aeProcessEvents(loop, AE_TIME_EVENTS);
  event_loop_run(loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(loop);
  event_loop_destroy(loop);
  ASSERT(publish_late_failed);
  task_instance_free(task);
  PASS();
}

SUITE(task_log_tests) {
  RUN_TEST(subscribe_timeout_test);
  RUN_TEST(publish_timeout_test);
  RUN_TEST(subscribe_retry_test);
  RUN_TEST(publish_retry_test);
  RUN_TEST(subscribe_late_test);
  RUN_TEST(publish_late_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(task_log_tests);
  GREATEST_MAIN_END();
}
