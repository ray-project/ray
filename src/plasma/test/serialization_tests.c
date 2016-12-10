#include "greatest.h"

#include <sys/types.h>
#include <unistd.h>

#include "plasma.h"
#include "plasma_protocol.h"
#include "common.h"
#include "io.h"

SUITE(plasma_serialization_tests);

/**
 * Create a temporary file. Needs to be closed by the caller.
 *
 * @return File descriptor of the file.
 */
int create_temp_file() {
  static char template[] = "/tmp/tempfileXXXXXX";
  char file_name[32];
  strncpy(file_name, template, 32);
  return mkstemp(file_name);
}

/**
 * Seek to the beginning of a file and read a message from it.
 *
 * @param fd File descriptor of the file.
 * @param message type Message type that we expect in the file.
 * 
 * @return Pointer to the content of the message. Needs to be freed by the caller.
 */
uint8_t *read_message_from_file(int fd, int message_type) {
  /* Go to the beginning of the file. */
  lseek(fd, 0, SEEK_SET);
  int64_t type;
  int64_t length;
  uint8_t *data;
  read_message(fd, PLASMA_PROTOCOL_VERSION, &type, &length, &data);
  CHECK(type == message_type);
  return data;
}

TEST plasma_create_request_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  int64_t data_size1 = 42;
  int64_t metadata_size1 = 11;
  plasma_send_create_request(fd, object_id1, data_size1, metadata_size1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaCreateRequest);
  object_id object_id2;
  int64_t data_size2;
  int64_t metadata_size2;
  plasma_read_create_request(data, &object_id2, &data_size2, &metadata_size2);
  ASSERT_EQ(data_size1, data_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  free(data);
  close(fd);
  PASS();
}

plasma_object random_plasma_object() {
  int random = rand();
  plasma_object object;
  memset(&object, 0, sizeof(object));
  object.handle.store_fd = random + 7;
	object.handle.mmap_size = random + 42;
  object.data_offset = random + 1;
  object.metadata_offset = random + 2;
  object.data_size = random + 3;
  object.metadata_size = random + 4;
  return object;
}

TEST plasma_create_reply_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  plasma_object object1 = random_plasma_object();
  plasma_send_create_reply(fd, object_id1, &object1, 0);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaCreateReply);
  object_id object_id2;
  plasma_object object2;
  memset(&object2, 0, sizeof(object2));
  int error_code;
  plasma_read_create_reply(data, &object_id2, &object2, &error_code);
  ASSERT(object_ids_equal(object_id1, object_id2));
  ASSERT(memcmp(&object1, &object2, sizeof(object1)) == 0);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_get_local_request_test(void) {
  int fd = create_temp_file();
  object_id object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  plasma_send_get_local_request(fd, object_ids, 2);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaGetLocalRequest);
  int64_t num_objects;
  object_id *object_ids_return;
  plasma_read_get_local_request(data, &object_ids_return, &num_objects);
  ASSERT(object_ids_equal(object_ids[0], object_ids_return[0]));
  ASSERT(object_ids_equal(object_ids[1], object_ids_return[1]));
  free(object_ids_return);
  free(data);
  close(fd);
  PASS();
}


TEST plasma_get_local_reply_test(void) {
  int fd = create_temp_file();
  object_id object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  plasma_object plasma_objects[2];
  plasma_objects[0] = random_plasma_object();
  plasma_objects[1] = random_plasma_object();
  plasma_send_get_local_reply(fd, object_ids, plasma_objects, 2);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaGetLocalReply);
  int64_t num_objects;
  object_id *object_ids_return;
  plasma_object plasma_objects_return[2];
  plasma_read_get_local_reply(data, &object_ids_return, &plasma_objects_return[0], &num_objects);
  ASSERT(object_ids_equal(object_ids[0], object_ids_return[0]));
  ASSERT(object_ids_equal(object_ids[1], object_ids_return[1]));
  ASSERT(memcmp(&plasma_objects[0], &plasma_objects_return[0], sizeof(plasma_object)) == 0);
  ASSERT(memcmp(&plasma_objects[1], &plasma_objects_return[1], sizeof(plasma_object)) == 0);
  free(object_ids_return);
  free(data);
  close(fd);
  PASS();
}

SUITE(plasma_serialization_tests) {
  RUN_TEST(plasma_create_request_test);
  RUN_TEST(plasma_create_reply_test);
  RUN_TEST(plasma_get_local_request_test);
  RUN_TEST(plasma_get_local_reply_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_serialization_tests);
  GREATEST_MAIN_END();
}
