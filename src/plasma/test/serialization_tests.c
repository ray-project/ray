#include "greatest.h"

#include <sys/types.h>
#include <unistd.h>

#include "plasma.h"
#include "plasma_protocol.h"
#include "common.h"
#include "io.h"

SUITE(plasma_serialization_tests);

int create_temp_file() {
  static char template[] = "/tmp/tempfileXXXXXX";
  char file_name[32];
  strncpy(file_name, template, 32);
  return mkstemp(file_name);
}

TEST plasma_create_request_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  int64_t data_size1 = 42;
  int64_t metadata_size1 = 11;
  plasma_send_create_request(fd, object_id1, data_size1, metadata_size1);
  /* Go to the beginning of the file. */
  lseek(fd, 0, SEEK_SET);
  int64_t type;
  int64_t length;
  uint8_t *data;
  read_message(fd, PLASMA_PROTOCOL_VERSION, &type, &length, &data);
  object_id object_id2;
  int64_t data_size2;
  int64_t metadata_size2;
  plasma_read_create_request(data, &object_id2, &data_size2, &metadata_size2);
  ASSERT_EQ(data_size1, data_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  free(data);
  close(fd);
}

TEST plasma_create_reply_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  plasma_object object1;
  memset(&object1, 0, sizeof(object1));
  object1.handle.store_fd = 7;
	object1.handle.mmap_size = 42;
  object1.data_offset = 1;
  object1.metadata_offset = 2;
  object1.data_size = 3;
  object1.metadata_size = 4;
  plasma_send_create_reply(fd, object_id1, &object1);
  /* Go to the beginning of the file. */
  lseek(fd, 0, SEEK_SET);
  int64_t type;
  int64_t length;
  uint8_t *data;
  read_message(fd, PLASMA_PROTOCOL_VERSION, &type, &length, &data);
  object_id object_id2;
  plasma_object object2;
  memset(&object2, 0, sizeof(object2));
  plasma_read_create_reply(data, &object_id2, &object2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  ASSERT(memcmp(&object1, &object2, sizeof(object1)) == 0);
  free(data);
  close(fd);
}

SUITE(plasma_serialization_tests) {
  RUN_TEST(plasma_create_request_test);
  RUN_TEST(plasma_create_reply_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_serialization_tests);
  GREATEST_MAIN_END();
}
