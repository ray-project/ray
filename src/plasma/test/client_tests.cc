#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include "plasma/test-util.h"

#include "plasma/common.h"
#include "plasma/client.h"

using namespace plasma;

SUITE(plasma_client_tests);

TEST plasma_status_tests(void) {
  PlasmaClient client1;
  ARROW_CHECK_OK(client1.Connect("/tmp/store1", "/tmp/manager1",
                                 plasma::kPlasmaDefaultReleaseDelay));
  PlasmaClient client2;
  ARROW_CHECK_OK(client2.Connect("/tmp/store2", "/tmp/manager2",
                                 plasma::kPlasmaDefaultReleaseDelay));
  ObjectID oid1 = random_object_id();

  /* Test for object non-existence. */
  int status;
  ARROW_CHECK_OK(client1.Info(oid1, &status));
  ASSERT(status == static_cast<int>(ObjectLocation::Nonexistent));

  /* Test for the object being in local Plasma store. */
  /* First create object. */
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client1.Create(oid1, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client1.Seal(oid1));
  /* Sleep to avoid race condition of Plasma Manager waiting for notification.
   */
  sleep(1);
  ARROW_CHECK_OK(client1.Info(oid1, &status));
  ASSERT(status == static_cast<int>(ObjectLocation::Local));

  /* Test for object being remote. */
  ARROW_CHECK_OK(client2.Info(oid1, &status));
  ASSERT(status == static_cast<int>(ObjectLocation::Remote));

  ARROW_CHECK_OK(client1.Disconnect());
  ARROW_CHECK_OK(client2.Disconnect());

  PASS();
}

TEST plasma_fetch_tests(void) {
  PlasmaClient client1;
  ARROW_CHECK_OK(client1.Connect("/tmp/store1", "/tmp/manager1",
                                 plasma::kPlasmaDefaultReleaseDelay));
  PlasmaClient client2;
  ARROW_CHECK_OK(client2.Connect("/tmp/store2", "/tmp/manager2",
                                 plasma::kPlasmaDefaultReleaseDelay));
  ObjectID oid1 = random_object_id();

  /* Test for object non-existence. */
  int status;

  /* No object in the system */
  ARROW_CHECK_OK(client1.Info(oid1, &status));
  ASSERT(status == static_cast<int>(ObjectLocation::Nonexistent));

  /* Test for the object being in local Plasma store. */
  /* First create object. */
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client1.Create(oid1, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client1.Seal(oid1));

  /* Object with ID oid1 has been just inserted. On the next fetch we might
   * either find the object or not, depending on whether the Plasma Manager has
   * received the notification from the Plasma Store or not. */
  ObjectID oid_array1[1] = {oid1};
  ARROW_CHECK_OK(client1.Fetch(1, oid_array1));
  ARROW_CHECK_OK(client1.Info(oid1, &status));
  ASSERT(status == static_cast<int>(ObjectLocation::Local) ||
         status == static_cast<int>(ObjectLocation::Nonexistent));

  /* Sleep to make sure Plasma Manager got the notification. */
  sleep(1);
  ARROW_CHECK_OK(client1.Info(oid1, &status));
  ASSERT(status == static_cast<int>(ObjectLocation::Local));

  /* Test for object being remote. */
  ARROW_CHECK_OK(client2.Info(oid1, &status));
  ASSERT(status == static_cast<int>(ObjectLocation::Remote));

  /* Sleep to make sure the object has been fetched and it is now stored in the
   * local Plasma Store. */
  ARROW_CHECK_OK(client2.Fetch(1, oid_array1));
  sleep(1);
  ARROW_CHECK_OK(client2.Info(oid1, &status));
  ASSERT(status == static_cast<int>(ObjectLocation::Local));

  sleep(1);
  ARROW_CHECK_OK(client1.Disconnect());
  ARROW_CHECK_OK(client2.Disconnect());

  PASS();
}

void init_data_123(uint8_t *data, uint64_t size, uint8_t base) {
  for (size_t i = 0; i < size; i++) {
    data[i] = base + i;
  }
}

bool is_equal_data_123(const uint8_t *data1,
                       const uint8_t *data2,
                       uint64_t size) {
  for (size_t i = 0; i < size; i++) {
    if (data1[i] != data2[i]) {
      return false;
    };
  }
  return true;
}

TEST plasma_nonblocking_get_tests(void) {
  PlasmaClient client;
  ARROW_CHECK_OK(client.Connect("/tmp/store1", "/tmp/manager1",
                                plasma::kPlasmaDefaultReleaseDelay));
  ObjectID oid = random_object_id();
  ObjectID oid_array[1] = {oid};
  ObjectBuffer obj_buffer;

  /* Test for object non-existence. */
  ARROW_CHECK_OK(client.Get(oid_array, 1, 0, &obj_buffer));
  ASSERT(obj_buffer.data == nullptr);

  /* Test for the object being in local Plasma store. */
  /* First create object. */
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client.Create(oid, data_size, metadata, metadata_size, &data));
  init_data_123(data->mutable_data(), data_size, 0);
  ARROW_CHECK_OK(client.Seal(oid));

  sleep(1);
  ARROW_CHECK_OK(client.Get(oid_array, 1, 0, &obj_buffer));
  ASSERT(is_equal_data_123(data->data(), obj_buffer.data->data(), data_size) ==
         true);

  sleep(1);
  ARROW_CHECK_OK(client.Disconnect());

  PASS();
}

TEST plasma_wait_for_objects_tests(void) {
  PlasmaClient client1;
  ARROW_CHECK_OK(client1.Connect("/tmp/store1", "/tmp/manager1",
                                 plasma::kPlasmaDefaultReleaseDelay));
  PlasmaClient client2;
  ARROW_CHECK_OK(client2.Connect("/tmp/store2", "/tmp/manager2",
                                 plasma::kPlasmaDefaultReleaseDelay));
  ObjectID oid1 = random_object_id();
  ObjectID oid2 = random_object_id();
#define NUM_OBJ_REQUEST 2
#define WAIT_TIMEOUT_MS 1000
  ObjectRequest obj_requests[NUM_OBJ_REQUEST];

  obj_requests[0].object_id = oid1;
  obj_requests[0].type = ObjectRequestType::PLASMA_QUERY_ANYWHERE;
  obj_requests[1].object_id = oid2;
  obj_requests[1].type = ObjectRequestType::PLASMA_QUERY_ANYWHERE;

  struct timeval start, end;
  gettimeofday(&start, NULL);
  int n;
  ARROW_CHECK_OK(client1.Wait(NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                              WAIT_TIMEOUT_MS, &n));
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
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client1.Create(oid1, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client1.Seal(oid1));

  ARROW_CHECK_OK(client1.Wait(NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                              WAIT_TIMEOUT_MS, &n));
  ASSERT(n == 1);

  /* Create and insert an object in client2. */
  ARROW_CHECK_OK(
      client2.Create(oid2, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client2.Seal(oid2));

  ARROW_CHECK_OK(client1.Wait(NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                              WAIT_TIMEOUT_MS, &n));
  ASSERT(n == 2);

  ARROW_CHECK_OK(client2.Wait(NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                              WAIT_TIMEOUT_MS, &n));
  ASSERT(n == 2);

  obj_requests[0].type = ObjectRequestType::PLASMA_QUERY_LOCAL;
  obj_requests[1].type = ObjectRequestType::PLASMA_QUERY_LOCAL;
  ARROW_CHECK_OK(client1.Wait(NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                              WAIT_TIMEOUT_MS, &n));
  ASSERT(n == 1);

  ARROW_CHECK_OK(client2.Wait(NUM_OBJ_REQUEST, obj_requests, NUM_OBJ_REQUEST,
                              WAIT_TIMEOUT_MS, &n));
  ASSERT(n == 1);

  ARROW_CHECK_OK(client1.Disconnect());
  ARROW_CHECK_OK(client2.Disconnect());

  PASS();
}

TEST plasma_get_tests(void) {
  PlasmaClient client1, client2;
  ARROW_CHECK_OK(client1.Connect("/tmp/store1", "/tmp/manager1",
                                 plasma::kPlasmaDefaultReleaseDelay));
  ARROW_CHECK_OK(client2.Connect("/tmp/store2", "/tmp/manager2",
                                 plasma::kPlasmaDefaultReleaseDelay));
  ObjectID oid1 = random_object_id();
  ObjectID oid2 = random_object_id();
  ObjectBuffer obj_buffer1;

  ObjectID oid_array1[1] = {oid1};
  ObjectID oid_array2[1] = {oid2};

  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client1.Create(oid1, data_size, metadata, metadata_size, &data));
  init_data_123(data->mutable_data(), data_size, 1);
  ARROW_CHECK_OK(client1.Seal(oid1));

  ARROW_CHECK_OK(client1.Get(oid_array1, 1, -1, &obj_buffer1));
  ASSERT(data->data()[0] == obj_buffer1.data->data()[0]);

  ObjectBuffer obj_buffer2;
  ARROW_CHECK_OK(
      client2.Create(oid2, data_size, metadata, metadata_size, &data));
  init_data_123(data->mutable_data(), data_size, 2);
  ARROW_CHECK_OK(client2.Seal(oid2));

  ARROW_CHECK_OK(client1.Fetch(1, oid_array2));
  ARROW_CHECK_OK(client1.Get(oid_array2, 1, -1, &obj_buffer2));
  ASSERT(data->data()[0] == obj_buffer2.data->data()[0]);

  sleep(1);
  ARROW_CHECK_OK(client1.Disconnect());
  ARROW_CHECK_OK(client2.Disconnect());

  PASS();
}

TEST plasma_get_multiple_tests(void) {
  PlasmaClient client1, client2;
  ARROW_CHECK_OK(client1.Connect("/tmp/store1", "/tmp/manager1",
                                 plasma::kPlasmaDefaultReleaseDelay));
  ARROW_CHECK_OK(client2.Connect("/tmp/store2", "/tmp/manager2",
                                 plasma::kPlasmaDefaultReleaseDelay));
  ObjectID oid1 = random_object_id();
  ObjectID oid2 = random_object_id();
  ObjectID obj_ids[NUM_OBJ_REQUEST];
  ObjectBuffer obj_buffer[NUM_OBJ_REQUEST];
  int obj1_first = 1, obj2_first = 2;

  obj_ids[0] = oid1;
  obj_ids[1] = oid2;

  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client1.Create(oid1, data_size, metadata, metadata_size, &data));
  init_data_123(data->mutable_data(), data_size, obj1_first);
  ARROW_CHECK_OK(client1.Seal(oid1));

  /* This only waits for oid1. */
  ARROW_CHECK_OK(client1.Get(obj_ids, 1, -1, obj_buffer));
  ASSERT(data->data()[0] == obj_buffer[0].data->data()[0]);

  ARROW_CHECK_OK(
      client2.Create(oid2, data_size, metadata, metadata_size, &data));
  init_data_123(data->mutable_data(), data_size, obj2_first);
  ARROW_CHECK_OK(client2.Seal(oid2));

  ARROW_CHECK_OK(client1.Fetch(2, obj_ids));
  ARROW_CHECK_OK(client1.Get(obj_ids, 2, -1, obj_buffer));
  ASSERT(obj1_first == obj_buffer[0].data->data()[0]);
  ASSERT(obj2_first == obj_buffer[1].data->data()[0]);

  sleep(1);
  ARROW_CHECK_OK(client1.Disconnect());
  ARROW_CHECK_OK(client2.Disconnect());

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
