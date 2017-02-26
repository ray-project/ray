#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include "plasma.h"
#include "plasma_protocol.h"
#include "plasma_client.h"

SUITE(plasma_client_tests);

TEST plasma_status_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect(
      "/tmp/store1", "/tmp/manager1", PLASMA_DEFAULT_RELEASE_DELAY);
  plasma_connection *plasma_conn2 = plasma_connect(
      "/tmp/store2", "/tmp/manager2", PLASMA_DEFAULT_RELEASE_DELAY);
  ObjectID oid1 = globally_unique_id();

  /* Test for object non-existence. */
  int status = plasma_status(plasma_conn1, oid1);
  ASSERT(status == ObjectStatus_Nonexistent);

  /* Test for the object being in local Plasma store. */
  /* First create object. */
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn1, oid1);
  /* Sleep to avoid race condition of Plasma Manager waiting for notification.
   */
  sleep(1);
  status = plasma_status(plasma_conn1, oid1);
  ASSERT(status == ObjectStatus_Local);

  /* Test for object being remote. */
  status = plasma_status(plasma_conn2, oid1);
  ASSERT(status == ObjectStatus_Remote);

  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);

  PASS();
}

TEST plasma_fetch_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect(
      "/tmp/store1", "/tmp/manager1", PLASMA_DEFAULT_RELEASE_DELAY);
  plasma_connection *plasma_conn2 = plasma_connect(
      "/tmp/store2", "/tmp/manager2", PLASMA_DEFAULT_RELEASE_DELAY);
  ObjectID oid1 = globally_unique_id();

  /* Test for object non-existence. */
  int status;

  /* No object in the system */
  status = plasma_status(plasma_conn1, oid1);
  ASSERT(status == ObjectStatus_Nonexistent);

  /* Test for the object being in local Plasma store. */
  /* First create object. */
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn1, oid1);

  /* Object with ID oid1 has been just inserted. On the next fetch we might
   * either find the object or not, depending on whether the Plasma Manager has
   * received the notification from the Plasma Store or not. */
  ObjectID oid_array1[1] = {oid1};
  plasma_fetch(plasma_conn1, 1, oid_array1);
  status = plasma_status(plasma_conn1, oid1);
  ASSERT((status == ObjectStatus_Local) ||
         (status == ObjectStatus_Nonexistent));

  /* Sleep to make sure Plasma Manager got the notification. */
  sleep(1);
  status = plasma_status(plasma_conn1, oid1);
  ASSERT(status == ObjectStatus_Local);

  /* Test for object being remote. */
  status = plasma_status(plasma_conn2, oid1);
  ASSERT(status == ObjectStatus_Remote);

  /* Sleep to make sure the object has been fetched and it is now stored in the
   * local Plasma Store. */
  plasma_fetch(plasma_conn2, 1, oid_array1);
  sleep(1);
  status = plasma_status(plasma_conn2, oid1);
  ASSERT(status == ObjectStatus_Local);

  sleep(1);
  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);

  PASS();
}

void init_data_123(uint8_t *data, uint64_t size, uint8_t base) {
  for (int i = 0; i < size; i++) {
    data[i] = base + i;
  }
}

bool is_equal_data_123(uint8_t *data1, uint8_t *data2, uint64_t size) {
  for (int i = 0; i < size; i++) {
    if (data1[i] != data2[i]) {
      return false;
    };
  }
  return true;
}

TEST plasma_nonblocking_get_tests(void) {
  plasma_connection *plasma_conn = plasma_connect(
      "/tmp/store1", "/tmp/manager1", PLASMA_DEFAULT_RELEASE_DELAY);
  ObjectID oid = globally_unique_id();
  ObjectID oid_array[1] = {oid};
  object_buffer obj_buffer;

  /* Test for object non-existence. */
  plasma_get(plasma_conn, oid_array, 1, 0, &obj_buffer);
  ASSERT(obj_buffer.data_size == -1);

  /* Test for the object being in local Plasma store. */
  /* First create object. */
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn, oid, data_size, metadata, metadata_size, &data);
  init_data_123(data, data_size, 0);
  plasma_seal(plasma_conn, oid);

  sleep(1);
  plasma_get(plasma_conn, oid_array, 1, 0, &obj_buffer);
  ASSERT(is_equal_data_123(data, obj_buffer.data, data_size) == true);

  sleep(1);
  plasma_disconnect(plasma_conn);

  PASS();
}

TEST plasma_wait_for_objects_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect(
      "/tmp/store1", "/tmp/manager1", PLASMA_DEFAULT_RELEASE_DELAY);
  plasma_connection *plasma_conn2 = plasma_connect(
      "/tmp/store2", "/tmp/manager2", PLASMA_DEFAULT_RELEASE_DELAY);
  ObjectID oid1 = globally_unique_id();
  ObjectID oid2 = globally_unique_id();
#define NUM_OBJ_REQUEST 2
#define WAIT_TIMEOUT_MS 1000
  ObjectRequest obj_requests[NUM_OBJ_REQUEST];

  obj_requests[0].object_id = oid1;
  obj_requests[0].type = PLASMA_QUERY_ANYWHERE;
  obj_requests[1].object_id = oid2;
  obj_requests[1].type = PLASMA_QUERY_ANYWHERE;

  struct timeval start, end;
  gettimeofday(&start, NULL);
  int n = plasma_wait(plasma_conn1, NUM_OBJ_REQUEST, obj_requests,
                      NUM_OBJ_REQUEST, WAIT_TIMEOUT_MS);
  ASSERT(n == 0);
  gettimeofday(&end, NULL);
  float diff_ms = (end.tv_sec - start.tv_sec);
  diff_ms = (((diff_ms * 1000000.) + end.tv_usec) - (start.tv_usec)) / 1000.;
  /* Reduce threshold by 10% to make sure we pass consistently. */
  ASSERT(diff_ms > WAIT_TIMEOUT_MS * 0.9);

  /* Create and insert an object in plasma_conn1. */
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn1, oid1);

  n = plasma_wait(plasma_conn1, NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                  WAIT_TIMEOUT_MS);
  ASSERT(n == 1);

  /* Create and insert an object in plasma_conn2. */
  plasma_create(plasma_conn2, oid2, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn2, oid2);

  n = plasma_wait(plasma_conn1, NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                  WAIT_TIMEOUT_MS);
  ASSERT(n == 2);

  n = plasma_wait(plasma_conn2, NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                  WAIT_TIMEOUT_MS);
  ASSERT(n == 2);

  obj_requests[0].type = PLASMA_QUERY_LOCAL;
  obj_requests[1].type = PLASMA_QUERY_LOCAL;
  n = plasma_wait(plasma_conn1, NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                  WAIT_TIMEOUT_MS);
  ASSERT(n == 1);

  n = plasma_wait(plasma_conn2, NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                  WAIT_TIMEOUT_MS);
  ASSERT(n == 1);

  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);

  PASS();
}

TEST plasma_get_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect(
      "/tmp/store1", "/tmp/manager1", PLASMA_DEFAULT_RELEASE_DELAY);
  plasma_connection *plasma_conn2 = plasma_connect(
      "/tmp/store2", "/tmp/manager2", PLASMA_DEFAULT_RELEASE_DELAY);
  ObjectID oid1 = globally_unique_id();
  ObjectID oid2 = globally_unique_id();
  object_buffer obj_buffer;

  ObjectID oid_array1[1] = {oid1};
  ObjectID oid_array2[1] = {oid2};

  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  init_data_123(data, data_size, 1);
  plasma_seal(plasma_conn1, oid1);

  plasma_get(plasma_conn1, oid_array1, 1, -1, &obj_buffer);
  ASSERT(data[0] == obj_buffer.data[0]);

  plasma_create(plasma_conn2, oid2, data_size, metadata, metadata_size, &data);
  init_data_123(data, data_size, 2);
  plasma_seal(plasma_conn2, oid2);

  plasma_fetch(plasma_conn1, 1, oid_array2);
  plasma_get(plasma_conn1, oid_array2, 1, -1, &obj_buffer);
  ASSERT(data[0] == obj_buffer.data[0]);

  sleep(1);
  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);

  PASS();
}

TEST plasma_get_multiple_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect(
      "/tmp/store1", "/tmp/manager1", PLASMA_DEFAULT_RELEASE_DELAY);
  plasma_connection *plasma_conn2 = plasma_connect(
      "/tmp/store2", "/tmp/manager2", PLASMA_DEFAULT_RELEASE_DELAY);
  ObjectID oid1 = globally_unique_id();
  ObjectID oid2 = globally_unique_id();
  ObjectID obj_ids[NUM_OBJ_REQUEST];
  object_buffer obj_buffer[NUM_OBJ_REQUEST];
  int obj1_first = 1, obj2_first = 2;

  obj_ids[0] = oid1;
  obj_ids[1] = oid2;

  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  init_data_123(data, data_size, obj1_first);
  plasma_seal(plasma_conn1, oid1);

  /* This only waits for oid1. */
  plasma_get(plasma_conn1, obj_ids, 1, -1, obj_buffer);
  ASSERT(data[0] == obj_buffer[0].data[0]);

  plasma_create(plasma_conn2, oid2, data_size, metadata, metadata_size, &data);
  init_data_123(data, data_size, obj2_first);
  plasma_seal(plasma_conn2, oid2);

  plasma_fetch(plasma_conn1, 2, obj_ids);
  plasma_get(plasma_conn1, obj_ids, 2, -1, obj_buffer);
  ASSERT(obj1_first == obj_buffer[0].data[0]);
  ASSERT(obj2_first == obj_buffer[1].data[0]);

  sleep(1);
  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);

  PASS();
}

SUITE(plasma_client_tests) {
  RUN_TEST(plasma_status_tests);
  RUN_TEST(plasma_fetch_tests);
  RUN_TEST(plasma_nonblocking_get_tests);
  RUN_TEST(plasma_wait_for_objects_tests);
  RUN_TEST(plasma_get_tests);
  RUN_TEST(plasma_get_multiple_tests);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_client_tests);
  GREATEST_MAIN_END();
}
