#include "greatest.h"

#include "test_common.h"
#include "event_loop.h"
#include "state/pubsub.h"
#include "state/redis.h"

SUITE(pubsub_tests);

static event_loop *g_loop;

object_id pubsub_object_id;
int pubsub_destination = 7;
int pubsub_transfer_test_success = 0;

int64_t terminate_event_loop_callback(event_loop *loop,
                                      int64_t timer_id,
                                      void *context) {
  event_loop_stop(loop);
  return EVENT_LOOP_TIMER_DONE;
}

void pubsub_transfer_test_callback(object_id object_id,
                                   int source,
                                   int destination,
                                   void *user_context) {
  CHECK(memcmp(&object_id.id, &pubsub_object_id.id, UNIQUE_ID_SIZE));
  CHECK(destination == pubsub_destination);
  pubsub_transfer_test_success = 1;
}

void pubsub_subscribe_done_callback(object_id object_id,
                                    int source,
                                    int destination,
                                    void *user_context) {
  db_handle *db = user_context;
  retry_info retry = {.num_retries = 0, .timeout = 100, .fail_callback = NULL};
  pubsub_request_transfer(db, pubsub_object_id, source, pubsub_destination,
                          &retry, NULL, NULL);
}

/* This test makes sure that the subscriber of a transfer gets notified
 * if a transfer is requested from that source. */
TEST pubsub_transfer_test(void) {
  pubsub_object_id = globally_unique_id();
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  retry_info retry = {.num_retries = 0, .timeout = 100, .fail_callback = NULL};
  pubsub_subscribe_transfer(db, 0, pubsub_transfer_test_callback, &retry,
                            pubsub_subscribe_done_callback, db);

  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  ASSERT(pubsub_transfer_test_success == 1);
  PASS();
}

void pubsub_transfer_test_callback2(object_id object_id,
                                    int source,
                                    int destination,
                                    void *user_context) {
  CHECK(0);
}

void pubsub_subscribe_done_callback2(object_id object_id,
                                     int source,
                                     int destination,
                                     void *user_context) {
  db_handle *db = user_context;
  retry_info retry = {.num_retries = 0, .timeout = 100, .fail_callback = NULL};
  pubsub_request_transfer(db, NIL_ID, 1, pubsub_destination, &retry, NULL,
                          NULL);
}

/* This test makes sure that the subscriber of a transfer doesn't get notified
 * if a transfer is requested from a different source */
TEST pubsub_transfer_test2(void) {
  g_loop = event_loop_create();
  db_handle *db =
      db_connect("127.0.0.1", 6379, "plasma_manager", "127.0.0.1", 11236);
  db_attach(db, g_loop);
  retry_info retry = {.num_retries = 0, .timeout = 100, .fail_callback = NULL};
  pubsub_subscribe_transfer(db, 0, pubsub_transfer_test_callback2, &retry,
                            pubsub_subscribe_done_callback2, db);
  event_loop_add_timer(g_loop, 750,
                       (event_loop_timer_handler) terminate_event_loop_callback,
                       NULL);
  event_loop_run(g_loop);
  db_disconnect(db);
  destroy_outstanding_callbacks(g_loop);
  event_loop_destroy(g_loop);
  PASS();
}

SUITE(pubsub_tests) {
  RUN_REDIS_TEST(pubsub_transfer_test);
  RUN_REDIS_TEST(pubsub_transfer_test2);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(pubsub_tests);
  GREATEST_MAIN_END();
}
