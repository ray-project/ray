//
// Created by Ion Stoica on 11/20/16.
//
#include "greatest.h"

#include <assert.h>
#include <unistd.h>

#include "plasma.h"
#include "plasma_client.h"

SUITE(plasma_client_tests);


TEST plasma_status_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect("/tmp/store1", "/tmp/manager1");
  plasma_connection *plasma_conn2 = plasma_connect("/tmp/store2", "/tmp/manager2");
  object_id oid1 = {{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};
  object_id oid2 = {{2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};

  /** Test for object non-existence */
  int status = plasma_status(plasma_conn1, oid1);
  ASSERT(status == PLASMA_OBJECT_DOES_NOT_EXIST);

  /** Test for the object being in local Plasma store. */
  /** First cerate object */
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn1, oid1);
  /** sleep to avoid race condition of Plasma Manager waiting for notification. */
  sleep(1);
  status = plasma_status(plasma_conn1, oid1);
  ASSERT(status == PLASMA_OBJECT_LOCAL);

  /** Test for object being remote. */
  status = plasma_status(plasma_conn2, oid1);
  ASSERT(status == PLASMA_OBJECT_REMOTE);

  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);
  PASS();
}

TEST plasma_fetch_remote_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect("/tmp/store1", "/tmp/manager1");
  plasma_connection *plasma_conn2 = plasma_connect("/tmp/store2", "/tmp/manager2");
  object_id oid1 = {{3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};
  object_id oid2 = {{4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};

  /** Test for object non-existence */
  int status = plasma_fetch_remote(plasma_conn1, oid1);
  ASSERT(status == PLASMA_OBJECT_DOES_NOT_EXIST);

  /** Test for the object being in local Plasma store. */
  /** First cerate object */
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn1, oid1);

  /** XXX The next command causes a segfault saying it cannot create the object twice . */
  status = plasma_fetch_remote(plasma_conn1, oid1);
  /** sleep to avoid race condition of Plasma Manager waiting for notification. */
  sleep(1);
  status = plasma_fetch_remote(plasma_conn1, oid1);
  ASSERT(status == PLASMA_OBJECT_LOCAL);

  /** Test for object being remote. */
  status = plasma_fetch_remote(plasma_conn2, oid1);
  ASSERT(status == PLASMA_OBJECT_REMOTE);

  /* Plasma Manager should wait for the fetch notification. */
  sleep(1);
  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);
  PASS();
}

SUITE(plasma_client_tests) {
  // RUN_TEST(plasma_status_tests);
  RUN_TEST(plasma_fetch_remote_tests);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_client_tests);
  GREATEST_MAIN_END();
}
