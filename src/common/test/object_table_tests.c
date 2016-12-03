#include "greatest.h"

#include "event_loop.h"
#include "test_common.h"
#include "common.h"
#include "state/object_table.h"
#include "state/redis.h"

#include <unistd.h>

SUITE(object_table_tests);

static event_loop *g_loop;

/* ==== Test adding and looking up metadata ==== */

int new_object_failed = 0;
int new_object_succeeded = 0;
object_id new_object_id;
task *new_object_task;
task_spec *new_object_task_spec;
task_id new_object_task_id;

void new_object_fail_callback(unique_id id,
                              void *user_context,
                              void *user_data) {
  new_object_failed = 1;
  event_loop_stop(g_loop);
}

/* === Test adding an object with an associated task === */

void new_object_done_callback(object_id object_id,
                              task *task,
                              void *user_context) {
  new_object_succeeded = 1;
  CHECK(object_ids_equal(object_id, new_object_id));
  CHECK(task);
  CHECK(memcmp(task, new_object_task, task_size(task)) == 0);
  event_loop_stop(g_loop);
}

void new_object_lookup_callback(object_id object_id, void *user_context) {
  CHECK(object_ids_equal(object_id, new_object_id));
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  db_handle *db = user_context;
  result_table_lookup(db, new_object_id, &retry, new_object_done_callback,
                      NULL);
}

void new_object_task_callback(task_id task_id, void *user_context) {
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  db_handle *db = user_context;
  result_table_add(db, new_object_id, new_object_task_id, &retry,
                   new_object_lookup_callback, (void *) db);
}

TEST new_object_test(void) {
  new_object_failed = 0;
  new_object_succeeded = 0;
  new_object_id = globally_unique_id();
  new_object_task = example_task();
  new_object_task_spec = task_task_spec(new_object_task);
  new_object_task_id = task_spec_id(new_object_task_spec);
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  task_table_add_task(db, new_object_task, &retry, new_object_task_callback,
                      db);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  free_task(new_object_task);
  ASSERT(new_object_succeeded);
  ASSERT(!new_object_failed);
  PASS();
}

/* === Test adding an object without an associated task === */

void new_object_no_task_lookup_callback(object_id object_id,
                                        task *task,
                                        void *user_context) {
  new_object_succeeded = 1;
  CHECK(task == NULL);
  event_loop_stop(g_loop);
}

void new_object_no_task_callback(object_id object_id, void *user_context) {
  CHECK(node_ids_equal(object_id, new_object_id));
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  db_handle *db = user_context;
  result_table_lookup(db, object_id, &retry, new_object_no_task_lookup_callback,
                      NULL);
}

TEST new_object_no_task_test(void) {
  new_object_failed = 0;
  new_object_succeeded = 0;
  new_object_id = globally_unique_id();
  new_object_task_id = globally_unique_id();
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  result_table_add(db, new_object_id, new_object_task_id, &retry,
                   new_object_no_task_callback, db);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(new_object_succeeded);
  ASSERT(!new_object_failed);
  PASS();
}

/* ==== Test if operations time out correctly ==== */

/* === Test lookup timeout === */

const char *lookup_timeout_context = "lookup_timeout";
int lookup_failed = 0;

void lookup_done_callback(object_id object_id,
                          int manager_count,
                          OWNER const char *manager_vector[],
                          void *context) {
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
  object_table_lookup(db, NIL_ID, &retry, lookup_done_callback,
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

void add_done_callback(object_id object_id, void *user_context) {
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
  object_table_add(db, NIL_ID, &retry, add_done_callback,
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

/* === Test subscribe timeout === */

const char *subscribe_timeout_context = "subscribe_timeout";
int subscribe_failed = 0;

void subscribe_done_callback(object_id object_id, void *user_context) {
  /* The done callback should not be called. */
  CHECK(0);
}

void subscribe_fail_callback(unique_id id,
                             void *user_context,
                             void *user_data) {
  subscribe_failed = 1;
  CHECK(user_context == (void *) subscribe_timeout_context);
  event_loop_stop(g_loop);
}

TEST subscribe_timeout_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 1234);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = subscribe_fail_callback,
  };
  object_table_subscribe(db, NIL_ID, NULL, NULL, &retry,
                         subscribe_done_callback,
                         (void *) subscribe_timeout_context);
  /* Disconnect the database to see if the lookup times out. */
  close(db->sub_context->c.fd);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(subscribe_failed);
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

void lookup_retry_done_callback(object_id object_id,
                                int manager_count,
                                OWNER const char *manager_vector[],
                                void *context) {
  CHECK(context == (void *) lookup_retry_context);
  lookup_retry_succeeded = 1;
  free(manager_vector);
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
  object_table_lookup(db, NIL_ID, &retry, lookup_retry_done_callback,
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

void add_retry_done_callback(object_id object_id, void *user_context) {
  CHECK(user_context == (void *) add_retry_context);
  add_retry_succeeded = 1;
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
  object_table_add(db, NIL_ID, &retry, add_retry_done_callback,
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

/* === Test subscribe retry === */

const char *subscribe_retry_context = "subscribe_retry";
int subscribe_retry_succeeded = 0;

int64_t reconnect_sub_context_callback(event_loop *loop,
                                       int64_t timer_id,
                                       void *context) {
  db_handle *db = context;
  /* Reconnect to redis. This is not reconnecting the pub/sub channel. */
  redisAsyncFree(db->sub_context);
  redisAsyncFree(db->context);
  redisFree(db->sync_context);
  db->sub_context = redisAsyncConnect("127.0.0.1", 6379);
  db->sub_context->data = (void *) db;
  db->context = redisAsyncConnect("127.0.0.1", 6379);
  db->context->data = (void *) db;
  db->sync_context = redisConnect("127.0.0.1", 6379);
  /* Re-attach the database to the event loop (the file descriptor changed). */
  db_attach(db, loop);
  return EVENT_LOOP_TIMER_DONE;
}

void subscribe_retry_done_callback(object_id object_id, void *user_context) {
  CHECK(user_context == (void *) subscribe_retry_context);
  subscribe_retry_succeeded = 1;
}

void subscribe_retry_fail_callback(unique_id id,
                                   void *user_context,
                                   void *user_data) {
  /* The fail callback should not be called. */
  CHECK(0);
}

TEST subscribe_retry_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11235);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = subscribe_retry_fail_callback,
  };
  object_table_subscribe(db, NIL_ID, NULL, NULL, &retry,
                         subscribe_retry_done_callback,
                         (void *) subscribe_retry_context);
  /* Disconnect the database to let the subscribe times out the first time. */
  close(db->sub_context->c.fd);
  /* Install handler for reconnecting the database. */
  event_loop_add_timer(
      g_loop, 150, (event_loop_timer_handler) reconnect_sub_context_callback,
      db);
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

void lookup_late_done_callback(object_id object_id,
                               int manager_count,
                               OWNER const char *manager_vector[],
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
  object_table_lookup(db, NIL_ID, &retry, lookup_late_done_callback,
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
}

void add_late_done_callback(object_id object_id, void *user_context) {
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
  object_table_add(db, NIL_ID, &retry, add_late_done_callback,
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

/* === Test subscribe late succeed === */

const char *subscribe_late_context = "subscribe_late";
int subscribe_late_failed = 0;

void subscribe_late_fail_callback(unique_id id,
                                  void *user_context,
                                  void *user_data) {
  CHECK(user_context == (void *) subscribe_late_context);
  subscribe_late_failed = 1;
}

void subscribe_late_done_callback(object_id object_id, void *user_context) {
  /* This function should never be called. */
  CHECK(0);
}

TEST subscribe_late_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  retry_info retry = {
      .num_retries = 0,
      .timeout = 0,
      .fail_callback = subscribe_late_fail_callback,
  };
  object_table_subscribe(db, NIL_ID, NULL, NULL, &retry,
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

/* === Test subscribe object available succeed === */

const char *subscribe_success_context = "subscribe_success";
int subscribe_success_done = 0;
int subscribe_success_succeeded = 0;

void subscribe_success_fail_callback(unique_id id,
                                     void *user_context,
                                     void *user_data) {
  /* This function should never be called. */
  CHECK(0);
}

void subscribe_success_done_callback(object_id object_id, void *user_context) {
  retry_info retry = {
      .num_retries = 0, .timeout = 0, .fail_callback = NULL,
  };
  object_table_add((db_handle *) user_context, object_id, &retry, NULL, NULL);
  subscribe_success_done = 1;
}

void subscribe_success_object_available_callback(object_id object_id,
                                                 void *user_context) {
  printf("first: %s\n", subscribe_success_context);
  printf("second: %s\n", user_context);
  CHECK(user_context == (void *) subscribe_success_context);
  subscribe_success_succeeded = 1;
}

TEST subscribe_success_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  unique_id id = globally_unique_id();

  retry_info retry = {
      .num_retries = 0,
      .timeout = 100,
      .fail_callback = subscribe_success_fail_callback,
  };
  object_table_subscribe(db, id, subscribe_success_object_available_callback,
                         (void *) subscribe_success_context, &retry,
                         subscribe_success_done_callback, (void *) db);

  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);

  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);

  ASSERT(subscribe_success_done);
  ASSERT(subscribe_success_succeeded);
  PASS();
}

/* Test if subscribe succeeds if the object is already present. */

const char *subscribe_object_present_context = "subscribe_object_present";
int subscribe_object_present_succeeded = 0;

void subscribe_object_present_object_available_callback(object_id object_id,
                                                        void *user_context) {
  CHECK(user_context == (void *) subscribe_object_present_context);
  subscribe_object_present_succeeded = 1;
}

TEST subscribe_object_present_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  unique_id id = globally_unique_id();
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  object_table_add(db, id, &retry, NULL, NULL);
  object_table_subscribe(
      db, id, subscribe_object_present_object_available_callback,
      (void *) subscribe_object_present_context, &retry, NULL, (void *) db);

  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);

  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(subscribe_object_present_succeeded == 1);
  PASS();
}

/* Test if subscribe is not called if object is not present. */

const char *subscribe_object_not_present_context =
    "subscribe_object_not_present";
int subscribe_object_not_present_succeeded = 0;

void subscribe_object_not_present_object_available_callback(
    object_id object_id,
    void *user_context) {
  CHECK(user_context == (void *) subscribe_object_not_present_context);
  subscribe_object_not_present_succeeded = 1;
}

TEST subscribe_object_not_present_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  unique_id id = globally_unique_id();
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  object_table_subscribe(
      db, id, subscribe_object_not_present_object_available_callback,
      (void *) subscribe_object_not_present_context, &retry, NULL, (void *) db);

  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);

  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(subscribe_object_not_present_succeeded == 0);
  PASS();
}

/* Test if subscribe is called if object becomes available later. */

const char *subscribe_object_available_later_context =
    "subscribe_object_available_later";
int subscribe_object_available_later_succeeded = 0;

void subscribe_object_available_later_object_available_callback(
    object_id object_id,
    void *user_context) {
  CHECK(user_context == (void *) subscribe_object_available_later_context);
  /* Make sure the callback is only called once. */
  subscribe_object_available_later_succeeded += 1;
}

int64_t add_object_callback(event_loop *loop, int64_t timer_id, void *context) {
  db_handle *db = (db_handle *) context;
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  object_table_add(db, NIL_ID, &retry, NULL, NULL);
  /* Reset the timer to this large value, so it doesn't trigger again. */
  return 10000;
}

TEST subscribe_object_available_later_test(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  unique_id id = NIL_ID;
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  object_table_subscribe(
      db, id, subscribe_object_available_later_object_available_callback,
      (void *) subscribe_object_available_later_context, &retry, NULL,
      (void *) db);

  event_loop_add_timer(g_loop, 300,
                       (event_loop_timer_handler) add_object_callback, db);

  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);

  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(subscribe_object_available_later_succeeded == 1);
  PASS();
}

SUITE(object_table_tests) {
  /*
  RUN_REDIS_TEST(new_object_test);
  RUN_REDIS_TEST(new_object_no_task_test);
  RUN_REDIS_TEST(lookup_timeout_test);
  RUN_REDIS_TEST(add_timeout_test);
  RUN_REDIS_TEST(subscribe_timeout_test);
  RUN_REDIS_TEST(lookup_retry_test);
  RUN_REDIS_TEST(add_retry_test);
  RUN_REDIS_TEST(subscribe_retry_test);
  RUN_REDIS_TEST(lookup_late_test);
  RUN_REDIS_TEST(add_late_test);
  RUN_REDIS_TEST(subscribe_late_test);
  */
  RUN_REDIS_TEST(subscribe_success_test);
  /*
  RUN_REDIS_TEST(subscribe_object_present_test);
  RUN_REDIS_TEST(subscribe_object_not_present_test);
  RUN_REDIS_TEST(subscribe_object_available_later_test);
  */
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(object_table_tests);
  GREATEST_MAIN_END();
}
