#include "greatest.h"

#include <sys/types.h>
#include <unistd.h>

#include "plasma.h"
#include "plasma_protocol.h"
#include "common.h"
#include "io.h"
#include "../plasma.h"

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

TEST data_int_message_test(void) {
  int fd = create_temp_file();
  int value = 5;
  int message_type = 12;
  send_data_int(fd, message_type, value);
  // Read message.
  uint8_t *data = read_message_from_file(fd, message_type);
  int value_read;
  read_data_int(data, &value_read);
  ASSERT(value == value_read);
  free(data);
  close(fd);
  PASS();
}

TEST object_id_message_test(void) {
  int fd = create_temp_file();
  object_id object_id_sent = globally_unique_id();
  int message_type = 10;
  send_object_id(fd, message_type, object_id_sent);
  uint8_t *data = read_message_from_file(fd, message_type);
  object_id object_id_read;
  read_object_id(data, &object_id_read);
  ASSERT(object_ids_equal(object_id_sent, object_id_read));
  free(data);
  close(fd);
  PASS();
}

TEST object_id_and_info_message_test(void) {
  int fd = create_temp_file();
  object_id object_id_sent = globally_unique_id();
  int message_type = 11;
  int info_sent = 5;
  send_object_id_and_info(fd, message_type, object_id_sent, info_sent);
  // Read message.
  uint8_t *data = read_message_from_file(fd, message_type);
  object_id object_id_read;
  int info_read;
  read_object_id_and_info(data, &object_id_read, &info_read);
  ASSERT(object_ids_equal(object_id_sent, object_id_read));
  ASSERT(info_sent == info_read);
  free(data);
  close(fd);
  PASS();
}

TEST object_ids_message_test(void) {
  int fd = create_temp_file();
  object_id object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  int message_type =15;
  send_object_ids(fd, message_type, object_ids, 2);
  uint8_t *data = read_message_from_file(fd, message_type);
  int64_t num_objects;
  object_id *object_ids_read;
  read_object_ids(data, &object_ids_read, &num_objects);
  ASSERT(object_ids_equal(object_ids[0], object_ids_read[0]));
  ASSERT(object_ids_equal(object_ids[1], object_ids_read[1]));
  free(object_ids_read);
  free(data);
  close(fd);
  PASS();
}

TEST object_ids_and_infos_message_test(void) {
  int fd = create_temp_file();
  object_id object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  int32_t object_infos[2];
  object_infos[0] = 5;
  object_infos[1] = 11;
  send_object_ids_and_infos(fd, MessageType_PlasmaGetLocalReply, object_ids, object_infos, 2);
  //
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaGetLocalReply);
  int64_t num_objects;
  object_id *object_ids_read;
  int32_t *object_infos_read;
  read_object_ids_and_infos(data, &object_ids_read, &object_infos_read, &num_objects);
  ASSERT(object_ids_equal(object_ids[0], object_ids_read[0]));
  ASSERT(object_ids_equal(object_ids[1], object_ids_read[1]));
  ASSERT(object_infos[0] == object_infos_read[0]);
  ASSERT(object_infos[1] == object_infos_read[1]);
  free(object_ids_read);
  free(data);
  close(fd);
  PASS();
}

TEST object_requests_message_test(void) {
  int fd = create_temp_file();
  object_request object_requests[2];
  object_requests[0].object_id = globally_unique_id();
  object_requests[0].type = PLASMA_QUERY_ANYWHERE;
  object_requests[0].status = PLASMA_OBJECT_LOCAL;
  object_requests[1].object_id = globally_unique_id();
  object_requests[1].type = PLASMA_QUERY_LOCAL;
  object_requests[1].status = PLASMA_OBJECT_NONEXISTENT;
  int num_requests = 2;
  send_object_requests(fd, MessageType_PlasmaWaitReply, object_requests, num_requests);
  /* Read message back. */
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaWaitReply);
  object_request *object_requests_read;
  int num_requests_read;
  read_object_requests(data, &object_requests_read, &num_requests_read);
  ASSERT(num_requests == num_requests_read);
  ASSERT(object_ids_equal(object_requests[0].object_id, object_requests_read[0].object_id));
  ASSERT(object_ids_equal(object_requests[1].object_id, object_requests_read[1].object_id));
  ASSERT(object_requests[0].type == object_requests_read[0].type);
  ASSERT(object_requests[1].type == object_requests_read[1].type);
  ASSERT(object_requests[0].status == object_requests_read[0].status);
  ASSERT(object_requests[1].status == object_requests_read[1].status);
  free(object_requests_read);
  free(data);
  close(fd);
  PASS();
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

TEST plasma_seal_request_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  plasma_send_seal_request(fd, object_id1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaSealRequest);
  object_id object_id2;
  plasma_read_seal_request(data, &object_id2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  free(data);
  close(fd);
  PASS();
}

TEST plasma_seal_reply_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  int error1 = 5;
  plasma_send_seal_reply(fd, object_id1, error1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaSealReply);
  object_id object_id2;
  int error2;
  plasma_read_seal_reply(data, &object_id2, &error2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  ASSERT(error1 == error2);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_release_request_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  plasma_send_release_request(fd, object_id1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaReleaseRequest);
  object_id object_id2;
  plasma_read_release_request(data, &object_id2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  free(data);
  close(fd);
  PASS();
}

TEST plasma_release_reply_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  int error1 = 5;
  plasma_send_release_reply(fd, object_id1, error1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaReleaseReply);
  object_id object_id2;
  int error2;
  plasma_read_release_reply(data, &object_id2, &error2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  ASSERT(error1 == error2);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_delete_request_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  plasma_send_delete_request(fd, object_id1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaDeleteRequest);
  object_id object_id2;
  plasma_read_delete_request(data, &object_id2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  free(data);
  close(fd);
  PASS();
}

TEST plasma_delete_reply_test(void) {
  int fd = create_temp_file();
  object_id object_id1 = globally_unique_id();
  int error1 = 5;
  plasma_send_delete_reply(fd, object_id1, error1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaDeleteReply);
  object_id object_id2;
  int error2;
  plasma_read_delete_reply(data, &object_id2, &error2);
  ASSERT(object_ids_equal(object_id1, object_id2));
  ASSERT(error1 == error2);
  free(data);
  close(fd);
  PASS();
}


TEST plasma_fetch_remote_request_test(void) {
  int fd = create_temp_file();
  object_id object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  plasma_send_fetch_remote_request(fd, object_ids, 2);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaFetchRemoteRequest);
  int64_t num_objects;
  object_id *object_ids_read;
  plasma_read_fetch_remote_request(data, &object_ids_read, &num_objects);
  ASSERT(object_ids_equal(object_ids[0], object_ids_read[0]));
  ASSERT(object_ids_equal(object_ids[1], object_ids_read[1]));
  free(object_ids_read);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_fetch_remote_reply_test(void) {
  int fd = create_temp_file();
  object_id object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  int32_t object_statuses[2];
  object_statuses[0] = 5;
  object_statuses[1] = 11;
  plasma_send_fetch_remote_reply(fd, object_ids, object_statuses, 2);
  /* Read message back. */
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaFetchRemoteReply);
  int64_t num_objects;
  object_id *object_ids_read;
  int *object_statuses_read;
  plasma_read_fetch_remote_reply(data, &object_ids_read, &object_statuses_read, &num_objects);
  ASSERT(object_ids_equal(object_ids[0], object_ids_read[0]));
  ASSERT(object_ids_equal(object_ids[1], object_ids_read[1]));
  ASSERT(object_statuses[0] == object_statuses_read[0]);
  ASSERT(object_statuses[1] == object_statuses_read[1]);
  free(object_statuses_read);
  free(object_ids_read);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_wait_request_test(void) {
  int fd = create_temp_file();
  object_request object_requests[2];
  object_requests[0].object_id = globally_unique_id();
  object_requests[0].type = PLASMA_QUERY_ANYWHERE;
  object_requests[0].status = PLASMA_OBJECT_LOCAL;
  object_requests[1].object_id = globally_unique_id();
  object_requests[1].type = PLASMA_QUERY_LOCAL;
  object_requests[1].status = PLASMA_OBJECT_NONEXISTENT;
  int num_ready_objects = 1;
  int timeout_ms = 1000;
  plasma_send_wait_request(fd, object_requests, 2, num_ready_objects, timeout_ms);
  /* Read message back. */
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaWaitRequest);
  object_request object_requests_read[2];
  int num_ready_objects_read;
  plasma_read_wait_request(data, &object_requests_read[0], &num_ready_objects_read);
  ASSERT(object_ids_equal(object_requests[0].object_id, object_requests_read[0].object_id));
  ASSERT(object_ids_equal(object_requests[1].object_id, object_requests_read[1].object_id));
  ASSERT(object_requests[0].type == object_requests_read[0].type);
  ASSERT(object_requests[1].type == object_requests_read[1].type);
  ASSERT(object_requests[0].status == object_requests_read[0].status);
  ASSERT(object_requests[1].status == object_requests_read[1].status);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_wait_reply_test(void) {
  int fd = create_temp_file();
  object_request object_requests[2];
  object_requests[0].object_id = globally_unique_id();
  object_requests[0].type = PLASMA_QUERY_ANYWHERE;
  object_requests[0].status = PLASMA_OBJECT_LOCAL;
  object_requests[1].object_id = globally_unique_id();
  object_requests[1].type = PLASMA_QUERY_LOCAL;
  object_requests[1].status = PLASMA_OBJECT_NONEXISTENT;
  int num_requests = 2;
  plasma_send_wait_reply(fd, object_requests, num_requests);
  /* Read message back. */
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaWaitReply);
  object_request *object_requests_read;
  int num_requests_read;
  plasma_read_wait_reply(data, &object_requests_read, &num_requests_read);
  ASSERT(num_requests == num_requests_read);
  ASSERT(object_ids_equal(object_requests[0].object_id, object_requests_read[0].object_id));
  ASSERT(object_ids_equal(object_requests[1].object_id, object_requests_read[1].object_id));
  ASSERT(object_requests[0].type == object_requests_read[0].type);
  ASSERT(object_requests[1].type == object_requests_read[1].type);
  ASSERT(object_requests[0].status == object_requests_read[0].status);
  ASSERT(object_requests[1].status == object_requests_read[1].status);
  free(object_requests_read);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_transfer_request_test(void) {
  int fd = create_temp_file();
  object_id object_id_send = globally_unique_id();
  uint8_t addr[IPv4_ADDR_LEN] = {1, 2, 3, 4};
  int port = 1001;
  plasma_send_transfer_request(fd, MessageType_PlasmaTransferRequest, object_id_send, addr, port);
  // Read message.
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaTransferRequest);
  object_id object_id_read;
  uint8_t addr_read[IPv4_ADDR_LEN];
  int port_read;
  plasma_read_transfer_request(data, &object_id_read, addr_read, &port_read);
  ASSERT(object_ids_equal(object_id_send, object_id_read));
  for (int i = 0; i < IPv4_ADDR_LEN; i++) {
    ASSERT(addr[i] == addr_read[i]);
  }
  ASSERT(port == port_read);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_transfer_reply_header_test(void) {
  int fd = create_temp_file();
  object_id object_id_send = globally_unique_id();
  int64_t data_size = random();
  int64_t metadata_size = random();
  plasma_send_transfer_reply_header(fd, MessageType_PlasmaTransferReplyHeader,
                                    object_id_send, data_size, metadata_size);
  // Read message.
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaTransferReplyHeader);
  object_id object_id_read;
  int64_t data_size_read;
  int64_t metadata_size_read;
  plasma_read_transfer_reply_header(data, &object_id_read, &data_size_read, &metadata_size_read);
  ASSERT(object_ids_equal(object_id_send, object_id_read));
  ASSERT(data_size == data_size_read);
  ASSERT(metadata_size == metadata_size_read);
  free(data);
  close(fd);
  PASS();
}



SUITE(plasma_serialization_tests) {
  RUN_TEST(data_int_message_test);
  RUN_TEST(object_id_message_test);
  RUN_TEST(object_id_and_info_message_test);
  RUN_TEST(object_ids_message_test);
  RUN_TEST(object_ids_and_infos_message_test);
  RUN_TEST(object_requests_message_test);
  RUN_TEST(plasma_create_request_test);
  RUN_TEST(plasma_create_reply_test);
  RUN_TEST(plasma_get_local_request_test);
  RUN_TEST(plasma_get_local_reply_test);
  RUN_TEST(plasma_seal_request_test);
  RUN_TEST(plasma_seal_reply_test);
  RUN_TEST(plasma_release_request_test);
  RUN_TEST(plasma_release_reply_test);
  RUN_TEST(plasma_delete_request_test);
  RUN_TEST(plasma_delete_reply_test);
  RUN_TEST(plasma_fetch_remote_request_test);
  RUN_TEST(plasma_fetch_remote_reply_test);
  RUN_TEST(plasma_wait_request_test);
  RUN_TEST(plasma_wait_reply_test);
  RUN_TEST(plasma_transfer_request_test);
  RUN_TEST(plasma_transfer_reply_header_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_serialization_tests);
  GREATEST_MAIN_END();
}
