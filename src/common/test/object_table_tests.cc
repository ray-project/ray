#include "greatest.h"

#include "event_loop.h"
#include "example_task.h"
#include "test_common.h"
#include "common.h"
#include "state/db_client_table.h"
#include "state/object_table.h"
#include "state/redis.h"

#include <unistd.h>

SUITE(object_table_tests);

static event_loop *g_loop;
TaskBuilder *g_task_builder = NULL;

/* ==== Test adding and looking up metadata ==== */

int new_object_failed = 0;
int new_object_succeeded = 0;
ObjectID new_object_id;
Task *new_object_task;
TaskSpec *new_object_task_spec;
TaskID new_object_task_id;

void new_object_fail_callback(UniqueID id,
                              void *user_context,
                              void *user_data) {
  new_object_failed = 1;
  event_loop_stop(g_loop);
}

/* === Test adding an object with an associated task === */

void new_object_done_callback(ObjectID object_id,
                              TaskID task_id,
                              bool is_put,
                              void *user_context) {
  new_object_succeeded = 1;
  RAY_CHECK(object_id == new_object_id);
  RAY_CHECK(task_id == new_object_task_id);
  event_loop_stop(g_loop);
}

void new_object_lookup_callback(ObjectID object_id, void *user_context) {
  RAY_CHECK(object_id == new_object_id);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  DBHandle *db = (DBHandle *) user_context;
  result_table_lookup(db, new_object_id, &retry, new_object_done_callback,
                      NULL);
}

void new_object_task_callback(TaskID task_id, void *user_context) {
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  DBHandle *db = (DBHandle *) user_context;
  result_table_add(db, new_object_id, new_object_task_id, false, &retry,
                   new_object_lookup_callback, (void *) db);
}

void task_table_subscribe_done(TaskID task_id, void *user_context) {
  RetryInfo retry = {
      .num_retries = 5, .timeout = 100, .fail_callback = NULL,
  };
  DBHandle *db = (DBHandle *) user_context;
  task_table_add_task(db, Task_copy(new_object_task), &retry,
                      new_object_task_callback, db);
}

TEST new_object_test(void) {
  new_object_failed = 0;
  new_object_succeeded = 0;
  new_object_id = ObjectID::from_random();
  new_object_task = example_task(1, 1, TaskStatus::WAITING);
  new_object_task_spec = Task_task_execution_spec(new_object_task)->Spec();
  new_object_task_id = TaskSpec_task_id(new_object_task_spec);
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  task_table_subscribe(db, UniqueID::nil(), TaskStatus::WAITING, NULL, NULL,
                       &retry, task_table_subscribe_done, db);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(new_object_succeeded);
  ASSERT(!new_object_failed);
  PASS();
}

/* === Test adding an object without an associated task === */

void new_object_no_task_callback(ObjectID object_id,
                                 TaskID task_id,
                                 bool is_put,
                                 void *user_context) {
  new_object_succeeded = 1;
  RAY_CHECK(task_id.is_nil());
  event_loop_stop(g_loop);
}

TEST new_object_no_task_test(void) {
  new_object_failed = 0;
  new_object_succeeded = 0;
  new_object_id = ObjectID::from_random();
  new_object_task_id = TaskID::from_random();
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = new_object_fail_callback,
  };
  result_table_lookup(db, new_object_id, &retry, new_object_no_task_callback,
                      NULL);
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

void lookup_done_callback(ObjectID object_id,
                          bool never_created,
                          const std::vector<DBClientID> &manager_vector,
                          void *context) {
  /* The done callback should not be called. */
  RAY_CHECK(0);
}

void lookup_fail_callback(UniqueID id, void *user_context, void *user_data) {
  lookup_failed = 1;
  RAY_CHECK(user_context == (void *) lookup_timeout_context);
  event_loop_stop(g_loop);
}

TEST lookup_timeout_test(void) {
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5, .timeout = 100, .fail_callback = lookup_fail_callback,
  };
  object_table_lookup(db, UniqueID::nil(), &retry, lookup_done_callback,
                      (void *) lookup_timeout_context);
  /* Disconnect the database to see if the lookup times out. */
  close(db->context->c.fd);
  for (auto context : db->contexts) {
    close(context->c.fd);
  }
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

void add_done_callback(ObjectID object_id, bool success, void *user_context) {
  /* The done callback should not be called. */
  RAY_CHECK(0);
}

void add_fail_callback(UniqueID id, void *user_context, void *user_data) {
  add_failed = 1;
  RAY_CHECK(user_context == (void *) add_timeout_context);
  event_loop_stop(g_loop);
}

TEST add_timeout_test(void) {
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5, .timeout = 100, .fail_callback = add_fail_callback,
  };
  object_table_add(db, UniqueID::nil(), 0, (unsigned char *) NIL_DIGEST, &retry,
                   add_done_callback, (void *) add_timeout_context);
  /* Disconnect the database to see if the lookup times out. */
  close(db->context->c.fd);
  for (auto context : db->contexts) {
    close(context->c.fd);
  }
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(add_failed);
  PASS();
}

/* === Test subscribe timeout === */

int subscribe_failed = 0;

void subscribe_done_callback(ObjectID object_id,
                             int64_t data_size,
                             const std::vector<DBClientID> &manager_vector,
                             void *user_context) {
  /* The done callback should not be called. */
  RAY_CHECK(0);
}

void subscribe_fail_callback(UniqueID id, void *user_context, void *user_data) {
  subscribe_failed = 1;
  event_loop_stop(g_loop);
}

TEST subscribe_timeout_test(void) {
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = subscribe_fail_callback,
  };
  object_table_subscribe_to_notifications(db, false, subscribe_done_callback,
                                          NULL, &retry, NULL, NULL);
  /* Disconnect the database to see if the lookup times out. */
  close(db->subscribe_context->c.fd);
  for (auto subscribe_context : db->subscribe_contexts) {
    close(subscribe_context->c.fd);
  }
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
  DBHandle *db = (DBHandle *) context;
  /* Reconnect to redis. This is not reconnecting the pub/sub channel. */
  redisAsyncFree(db->context);
  redisFree(db->sync_context);
  db->context = redisAsyncConnect("127.0.0.1", 6379);
  db->context->data = (void *) db;
  db->sync_context = redisConnect("127.0.0.1", 6379);
  /* Re-attach the database to the event loop (the file descriptor changed). */
  db_attach(db, loop, true);
  RAY_LOG(DEBUG) << "Reconnected to Redis";
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

void lookup_retry_fail_callback(UniqueID id,
                                void *user_context,
                                void *user_data) {
  /* The fail callback should not be called. */
  RAY_CHECK(0);
}

/* === Test add retry === */

const char *add_retry_context = "add_retry";
int add_retry_succeeded = 0;

/* === Test add then lookup retry === */

void add_lookup_done_callback(ObjectID object_id,
                              bool never_created,
                              const std::vector<DBClientID> &manager_ids,
                              void *context) {
  DBHandle *db = (DBHandle *) context;
  RAY_CHECK(manager_ids.size() == 1);
  const std::vector<std::string> managers =
      db_client_table_get_ip_addresses(db, manager_ids);
  RAY_CHECK(managers.at(0) == "127.0.0.1:11235");
  lookup_retry_succeeded = 1;
}

void add_lookup_callback(ObjectID object_id, bool success, void *user_context) {
  RAY_CHECK(success);
  DBHandle *db = (DBHandle *) user_context;
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = lookup_retry_fail_callback,
  };
  object_table_lookup(db, UniqueID::nil(), &retry, add_lookup_done_callback,
                      (void *) db);
}

TEST add_lookup_test(void) {
  g_loop = event_loop_create();
  lookup_retry_succeeded = 0;
  /* Construct the arguments to db_connect. */
  std::vector<std::string> db_connect_args;
  db_connect_args.push_back("manager_address");
  db_connect_args.push_back("127.0.0.1:11235");
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", db_connect_args);
  db_attach(db, g_loop, true);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = lookup_retry_fail_callback,
  };
  object_table_add(db, UniqueID::nil(), 0, (unsigned char *) NIL_DIGEST, &retry,
                   add_lookup_callback, (void *) db);
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

/* === Test add, remove, then lookup === */
void add_remove_lookup_done_callback(
    ObjectID object_id,
    bool never_created,
    const std::vector<DBClientID> &manager_vector,
    void *context) {
  RAY_CHECK(context == (void *) lookup_retry_context);
  RAY_CHECK(manager_vector.size() == 0);
  lookup_retry_succeeded = 1;
}

void add_remove_lookup_callback(ObjectID object_id,
                                bool success,
                                void *user_context) {
  RAY_CHECK(success);
  DBHandle *db = (DBHandle *) user_context;
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = lookup_retry_fail_callback,
  };
  object_table_lookup(db, UniqueID::nil(), &retry,
                      add_remove_lookup_done_callback,
                      (void *) lookup_retry_context);
}

void add_remove_callback(ObjectID object_id, bool success, void *user_context) {
  RAY_CHECK(success);
  DBHandle *db = (DBHandle *) user_context;
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = lookup_retry_fail_callback,
  };
  object_table_remove(db, UniqueID::nil(), NULL, &retry,
                      add_remove_lookup_callback, (void *) db);
}

TEST add_remove_lookup_test(void) {
  g_loop = event_loop_create();
  lookup_retry_succeeded = 0;
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, true);
  RetryInfo retry = {
      .num_retries = 5,
      .timeout = 100,
      .fail_callback = lookup_retry_fail_callback,
  };
  object_table_add(db, UniqueID::nil(), 0, (unsigned char *) NIL_DIGEST, &retry,
                   add_remove_callback, (void *) db);
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

/* ==== Test if late succeed is working correctly ==== */

/* === Test lookup late succeed === */

const char *lookup_late_context = "lookup_late";
int lookup_late_failed = 0;

void lookup_late_fail_callback(UniqueID id,
                               void *user_context,
                               void *user_data) {
  RAY_CHECK(user_context == (void *) lookup_late_context);
  lookup_late_failed = 1;
}

void lookup_late_done_callback(ObjectID object_id,
                               bool never_created,
                               const std::vector<DBClientID> &manager_vector,
                               void *context) {
  /* This function should never be called. */
  RAY_CHECK(0);
}

TEST lookup_late_test(void) {
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 0,
      .timeout = 0,
      .fail_callback = lookup_late_fail_callback,
  };
  object_table_lookup(db, UniqueID::nil(), &retry, lookup_late_done_callback,
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

void add_late_fail_callback(UniqueID id, void *user_context, void *user_data) {
  RAY_CHECK(user_context == (void *) add_late_context);
  add_late_failed = 1;
}

void add_late_done_callback(ObjectID object_id,
                            bool success,
                            void *user_context) {
  /* This function should never be called. */
  RAY_CHECK(0);
}

TEST add_late_test(void) {
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 0, .timeout = 0, .fail_callback = add_late_fail_callback,
  };
  object_table_add(db, UniqueID::nil(), 0, (unsigned char *) NIL_DIGEST, &retry,
                   add_late_done_callback, (void *) add_late_context);
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

void subscribe_late_fail_callback(UniqueID id,
                                  void *user_context,
                                  void *user_data) {
  RAY_CHECK(user_context == (void *) subscribe_late_context);
  subscribe_late_failed = 1;
}

void subscribe_late_done_callback(ObjectID object_id,
                                  bool never_created,
                                  const std::vector<DBClientID> &manager_vector,
                                  void *user_context) {
  /* This function should never be called. */
  RAY_CHECK(0);
}

TEST subscribe_late_test(void) {
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  RetryInfo retry = {
      .num_retries = 0,
      .timeout = 0,
      .fail_callback = subscribe_late_fail_callback,
  };
  object_table_subscribe_to_notifications(db, false, NULL, NULL, &retry,
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
ObjectID subscribe_id;

void subscribe_success_fail_callback(UniqueID id,
                                     void *user_context,
                                     void *user_data) {
  /* This function should never be called. */
  RAY_CHECK(0);
}

void subscribe_success_done_callback(
    ObjectID object_id,
    bool never_created,
    const std::vector<DBClientID> &manager_vector,
    void *user_context) {
  RetryInfo retry = {
      .num_retries = 0, .timeout = 750, .fail_callback = NULL,
  };
  object_table_add((DBHandle *) user_context, subscribe_id, 0,
                   (unsigned char *) NIL_DIGEST, &retry, NULL, NULL);
  subscribe_success_done = 1;
}

void subscribe_success_object_available_callback(
    ObjectID object_id,
    int64_t data_size,
    const std::vector<DBClientID> &manager_vector,
    void *user_context) {
  RAY_CHECK(user_context == (void *) subscribe_success_context);
  RAY_CHECK(object_id == subscribe_id);
  RAY_CHECK(manager_vector.size() == 1);
  subscribe_success_succeeded = 1;
}

TEST subscribe_success_test(void) {
  g_loop = event_loop_create();

  /* Construct the arguments to db_connect. */
  std::vector<std::string> db_connect_args;
  db_connect_args.push_back("manager_address");
  db_connect_args.push_back("127.0.0.1:11236");
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", db_connect_args);
  db_attach(db, g_loop, false);
  subscribe_id = ObjectID::from_random();

  RetryInfo retry = {
      .num_retries = 0,
      .timeout = 100,
      .fail_callback = subscribe_success_fail_callback,
  };
  object_table_subscribe_to_notifications(
      db, false, subscribe_success_object_available_callback,
      (void *) subscribe_success_context, &retry,
      subscribe_success_done_callback, (void *) db);

  ObjectID object_ids[1] = {subscribe_id};
  object_table_request_notifications(db, 1, object_ids, &retry);

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
typedef struct {
  const char *teststr;
  int64_t data_size;
} subscribe_object_present_context_t;

const char *subscribe_object_present_str = "subscribe_object_present";
int subscribe_object_present_succeeded = 0;

void subscribe_object_present_object_available_callback(
    ObjectID object_id,
    int64_t data_size,
    const std::vector<DBClientID> &manager_vector,
    void *user_context) {
  subscribe_object_present_context_t *ctx =
      (subscribe_object_present_context_t *) user_context;
  RAY_CHECK(ctx->data_size == data_size);
  RAY_CHECK(strcmp(subscribe_object_present_str, ctx->teststr) == 0);
  subscribe_object_present_succeeded = 1;
  RAY_CHECK(manager_vector.size() == 1);
}

void fatal_fail_callback(UniqueID id, void *user_context, void *user_data) {
  /* This function should never be called. */
  RAY_CHECK(0);
}

TEST subscribe_object_present_test(void) {
  int64_t data_size = 0xF1F0;
  subscribe_object_present_context_t myctx = {subscribe_object_present_str,
                                              data_size};

  g_loop = event_loop_create();
  /* Construct the arguments to db_connect. */
  std::vector<std::string> db_connect_args;
  db_connect_args.push_back("manager_address");
  db_connect_args.push_back("127.0.0.1:11236");
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", db_connect_args);
  db_attach(db, g_loop, false);
  UniqueID id = UniqueID::from_random();
  RetryInfo retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = fatal_fail_callback,
  };
  object_table_add(db, id, data_size, (unsigned char *) NIL_DIGEST, &retry,
                   NULL, NULL);
  object_table_subscribe_to_notifications(
      db, false, subscribe_object_present_object_available_callback,
      (void *) &myctx, &retry, NULL, (void *) db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to create do the add and subscribe. */
  event_loop_run(g_loop);

  ObjectID object_ids[1] = {id};
  object_table_request_notifications(db, 1, object_ids, &retry);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to do the request notifications. */
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

void subscribe_object_not_present_object_available_callback(
    ObjectID object_id,
    int64_t data_size,
    const std::vector<DBClientID> &manager_vector,
    void *user_context) {
  /* This should not be called. */
  RAY_CHECK(0);
}

TEST subscribe_object_not_present_test(void) {
  g_loop = event_loop_create();
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", std::vector<std::string>());
  db_attach(db, g_loop, false);
  UniqueID id = UniqueID::from_random();
  RetryInfo retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  object_table_subscribe_to_notifications(
      db, false, subscribe_object_not_present_object_available_callback,
      (void *) subscribe_object_not_present_context, &retry, NULL, (void *) db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to do the subscribe. */
  event_loop_run(g_loop);

  ObjectID object_ids[1] = {id};
  object_table_request_notifications(db, 1, object_ids, &retry);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to do the request notifications. */
  event_loop_run(g_loop);

  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  PASS();
}

/* Test if subscribe is called if object becomes available later. */

const char *subscribe_object_available_later_context =
    "subscribe_object_available_later";
int subscribe_object_available_later_succeeded = 0;

void subscribe_object_available_later_object_available_callback(
    ObjectID object_id,
    int64_t data_size,
    const std::vector<DBClientID> &manager_vector,
    void *user_context) {
  subscribe_object_present_context_t *myctx =
      (subscribe_object_present_context_t *) user_context;
  RAY_CHECK(myctx->data_size == data_size);
  RAY_CHECK(strcmp(myctx->teststr, subscribe_object_available_later_context) ==
            0);
  /* Make sure the callback is only called once. */
  subscribe_object_available_later_succeeded += 1;
  RAY_CHECK(manager_vector.size() == 1);
}

TEST subscribe_object_available_later_test(void) {
  int64_t data_size = 0xF1F0;
  subscribe_object_present_context_t *myctx =
      (subscribe_object_present_context_t *) malloc(
          sizeof(subscribe_object_present_context_t));
  myctx->teststr = subscribe_object_available_later_context;
  myctx->data_size = data_size;

  g_loop = event_loop_create();
  /* Construct the arguments to db_connect. */
  std::vector<std::string> db_connect_args;
  db_connect_args.push_back("manager_address");
  db_connect_args.push_back("127.0.0.1:11236");
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", db_connect_args);
  db_attach(db, g_loop, false);
  UniqueID id = UniqueID::from_random();
  RetryInfo retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  object_table_subscribe_to_notifications(
      db, false, subscribe_object_available_later_object_available_callback,
      (void *) myctx, &retry, NULL, (void *) db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to do the subscribe. */
  event_loop_run(g_loop);

  ObjectID object_ids[1] = {id};
  object_table_request_notifications(db, 1, object_ids, &retry);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to do the request notifications. */
  event_loop_run(g_loop);

  ASSERT_EQ(subscribe_object_available_later_succeeded, 0);
  object_table_add(db, id, data_size, (unsigned char *) NIL_DIGEST, &retry,
                   NULL, NULL);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to do the object table add. */
  event_loop_run(g_loop);

  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT_EQ(subscribe_object_available_later_succeeded, 1);
  /* Reset the global variable before exiting this unit test. */
  subscribe_object_available_later_succeeded = 0;
  free(myctx);
  PASS();
}

TEST subscribe_object_available_subscribe_all(void) {
  int64_t data_size = 0xF1F0;
  subscribe_object_present_context_t myctx = {
      subscribe_object_available_later_context, data_size};
  g_loop = event_loop_create();
  /* Construct the arguments to db_connect. */
  std::vector<std::string> db_connect_args;
  db_connect_args.push_back("manager_address");
  db_connect_args.push_back("127.0.0.1:11236");
  DBHandle *db = db_connect(std::string("127.0.0.1"), 6379, "plasma_manager",
                            "127.0.0.1", db_connect_args);
  db_attach(db, g_loop, false);
  UniqueID id = UniqueID::from_random();
  RetryInfo retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  object_table_subscribe_to_notifications(
      db, true, subscribe_object_available_later_object_available_callback,
      (void *) &myctx, &retry, NULL, (void *) db);
  /* Install handler for terminating the event loop. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to do the subscribe. */
  event_loop_run(g_loop);

  /* At this point we don't expect any object notifications received. */
  ASSERT_EQ(subscribe_object_available_later_succeeded, 0);
  object_table_add(db, id, data_size, (unsigned char *) NIL_DIGEST, &retry,
                   NULL, NULL);
  /* Install handler to terminate event loop after 750ms. */
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  /* Run the event loop to do the object table add. */
  event_loop_run(g_loop);
  /* At this point we assume that object table add completed. */

  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  /* Assert that the object table add completed and notification callback fired.
   */
  printf("subscribe_all object info test: callback fired: %d times\n",
         subscribe_object_available_later_succeeded);
  fflush(stdout);
  ASSERT_EQ(subscribe_object_available_later_succeeded, 1);
  /* Reset the global variable before exiting this unit test. */
  subscribe_object_available_later_succeeded = 0;
  PASS();
}

SUITE(object_table_tests) {
  RUN_REDIS_TEST(new_object_test);
  RUN_REDIS_TEST(new_object_no_task_test);
  // RUN_REDIS_TEST(lookup_timeout_test);
  // RUN_REDIS_TEST(add_timeout_test);
  // RUN_REDIS_TEST(subscribe_timeout_test);
  RUN_REDIS_TEST(add_lookup_test);
  RUN_REDIS_TEST(add_remove_lookup_test);
  // RUN_REDIS_TEST(lookup_late_test);
  // RUN_REDIS_TEST(add_late_test);
  // RUN_REDIS_TEST(subscribe_late_test);
  RUN_REDIS_TEST(subscribe_success_test);
  RUN_REDIS_TEST(subscribe_object_not_present_test);
  RUN_REDIS_TEST(subscribe_object_available_later_test);
  RUN_REDIS_TEST(subscribe_object_available_subscribe_all);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  g_task_builder = make_task_builder();
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(object_table_tests);
  GREATEST_MAIN_END();
}
