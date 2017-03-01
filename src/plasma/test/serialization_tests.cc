#include "greatest.h"

#include <sys/types.h>
#include <unistd.h>

#include "plasma.h"
#include "plasma_protocol.h"
#include "common.h"
#include "io.h"
#include "../plasma.h"

SUITE(plasma_serialization_tests);

protocol_builder *g_B;

/**
 * Create a temporary file. Needs to be closed by the caller.
 *
 * @return File descriptor of the file.
 */
int create_temp_file(void) {
  static char temp[] = "/tmp/tempfileXXXXXX";
  char file_name[32];
  strncpy(file_name, temp, 32);
  return mkstemp(file_name);
}

/**
 * Seek to the beginning of a file and read a message from it.
 *
 * @param fd File descriptor of the file.
 * @param message type Message type that we expect in the file.
 *
 * @return Pointer to the content of the message. Needs to be freed by the
 * caller.
 */
uint8_t *read_message_from_file(int fd, int message_type) {
  /* Go to the beginning of the file. */
  lseek(fd, 0, SEEK_SET);
  int64_t type;
  int64_t length;
  uint8_t *data;
  read_message(fd, &type, &length, &data);
  CHECK(type == message_type);
  return data;
}

PlasmaObject random_plasma_object(void) {
  int random = rand();
  PlasmaObject object;
  memset(&object, 0, sizeof(object));
  object.handle.store_fd = random + 7;
  object.handle.mmap_size = random + 42;
  object.data_offset = random + 1;
  object.metadata_offset = random + 2;
  object.data_size = random + 3;
  object.metadata_size = random + 4;
  return object;
}

TEST plasma_create_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  int64_t data_size1 = 42;
  int64_t metadata_size1 = 11;
  plasma_send_CreateRequest(fd, g_B, object_id1, data_size1, metadata_size1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaCreateRequest);
  ObjectID object_id2;
  int64_t data_size2;
  int64_t metadata_size2;
  plasma_read_CreateRequest(data, &object_id2, &data_size2, &metadata_size2);
  ASSERT_EQ(data_size1, data_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  free(data);
  close(fd);
  PASS();
}

TEST plasma_create_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  PlasmaObject object1 = random_plasma_object();
  plasma_send_CreateReply(fd, g_B, object_id1, &object1, 0);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaCreateReply);
  ObjectID object_id2;
  PlasmaObject object2;
  memset(&object2, 0, sizeof(object2));
  int error_code;
  plasma_read_CreateReply(data, &object_id2, &object2, &error_code);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  ASSERT(memcmp(&object1, &object2, sizeof(object1)) == 0);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_seal_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  unsigned char digest1[DIGEST_SIZE];
  memset(&digest1[0], 7, DIGEST_SIZE);
  plasma_send_SealRequest(fd, g_B, object_id1, &digest1[0]);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaSealRequest);
  ObjectID object_id2;
  unsigned char digest2[DIGEST_SIZE];
  plasma_read_SealRequest(data, &object_id2, &digest2[0]);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  ASSERT(memcmp(&digest1[0], &digest2[0], DIGEST_SIZE) == 0);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_seal_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  int error1 = 5;
  plasma_send_SealReply(fd, g_B, object_id1, error1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaSealReply);
  ObjectID object_id2;
  int error2;
  plasma_read_SealReply(data, &object_id2, &error2);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  ASSERT(error1 == error2);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_get_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  int64_t timeout_ms = 1234;
  plasma_send_GetRequest(fd, g_B, object_ids, 2, timeout_ms);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaGetRequest);
  ObjectID object_ids_return[2];
  int64_t timeout_ms_return;
  plasma_read_GetRequest(data, &object_ids_return[0], &timeout_ms_return, 2);
  ASSERT(ObjectID_equal(object_ids[0], object_ids_return[0]));
  ASSERT(ObjectID_equal(object_ids[1], object_ids_return[1]));
  ASSERT(timeout_ms == timeout_ms_return);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_get_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  PlasmaObject plasma_objects[2];
  plasma_objects[0] = random_plasma_object();
  plasma_objects[1] = random_plasma_object();
  plasma_send_GetReply(fd, g_B, object_ids, plasma_objects, 2);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaGetReply);
  int64_t num_objects = plasma_read_GetRequest_num_objects(data);
  ObjectID object_ids_return[num_objects];
  PlasmaObject plasma_objects_return[2];
  memset(&plasma_objects_return, 0, sizeof(plasma_objects_return));
  plasma_read_GetReply(data, object_ids_return, &plasma_objects_return[0],
                       num_objects);
  ASSERT(ObjectID_equal(object_ids[0], object_ids_return[0]));
  ASSERT(ObjectID_equal(object_ids[1], object_ids_return[1]));
  ASSERT(memcmp(&plasma_objects[0], &plasma_objects_return[0],
                sizeof(PlasmaObject)) == 0);
  ASSERT(memcmp(&plasma_objects[1], &plasma_objects_return[1],
                sizeof(PlasmaObject)) == 0);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_release_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  plasma_send_ReleaseRequest(fd, g_B, object_id1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaReleaseRequest);
  ObjectID object_id2;
  plasma_read_ReleaseRequest(data, &object_id2);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  free(data);
  close(fd);
  PASS();
}

TEST plasma_release_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  int error1 = 5;
  plasma_send_ReleaseReply(fd, g_B, object_id1, error1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaReleaseReply);
  ObjectID object_id2;
  int error2;
  plasma_read_ReleaseReply(data, &object_id2, &error2);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  ASSERT(error1 == error2);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_delete_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  plasma_send_DeleteRequest(fd, g_B, object_id1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaDeleteRequest);
  ObjectID object_id2;
  plasma_read_DeleteRequest(data, &object_id2);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  free(data);
  close(fd);
  PASS();
}

TEST plasma_delete_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  int error1 = 5;
  plasma_send_DeleteReply(fd, g_B, object_id1, error1);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaDeleteReply);
  ObjectID object_id2;
  int error2;
  plasma_read_DeleteReply(data, &object_id2, &error2);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  ASSERT(error1 == error2);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_status_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  plasma_send_StatusRequest(fd, g_B, object_ids, 2);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaStatusRequest);
  int64_t num_objects = plasma_read_StatusRequest_num_objects(data);
  ObjectID object_ids_read[num_objects];
  plasma_read_StatusRequest(data, object_ids_read, num_objects);
  ASSERT(ObjectID_equal(object_ids[0], object_ids_read[0]));
  ASSERT(ObjectID_equal(object_ids[1], object_ids_read[1]));
  free(data);
  close(fd);
  PASS();
}

TEST plasma_status_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  int object_statuses[2] = {42, 43};
  plasma_send_StatusReply(fd, g_B, object_ids, object_statuses, 2);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaStatusReply);
  int64_t num_objects = plasma_read_StatusReply_num_objects(data);
  ObjectID object_ids_read[num_objects];
  int object_statuses_read[num_objects];
  plasma_read_StatusReply(data, object_ids_read, object_statuses_read,
                          num_objects);
  ASSERT(ObjectID_equal(object_ids[0], object_ids_read[0]));
  ASSERT(ObjectID_equal(object_ids[1], object_ids_read[1]));
  ASSERT_EQ(object_statuses[0], object_statuses_read[0]);
  ASSERT_EQ(object_statuses[1], object_statuses_read[1]);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_evict_request_test(void) {
  int fd = create_temp_file();
  int64_t num_bytes = 111;
  plasma_send_EvictRequest(fd, g_B, num_bytes);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaEvictRequest);
  int64_t num_bytes_received;
  plasma_read_EvictRequest(data, &num_bytes_received);
  ASSERT_EQ(num_bytes, num_bytes_received);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_evict_reply_test(void) {
  int fd = create_temp_file();
  int64_t num_bytes = 111;
  plasma_send_EvictReply(fd, g_B, num_bytes);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaEvictReply);
  int64_t num_bytes_received;
  plasma_read_EvictReply(data, &num_bytes_received);
  ASSERT_EQ(num_bytes, num_bytes_received);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_fetch_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = globally_unique_id();
  object_ids[1] = globally_unique_id();
  plasma_send_FetchRequest(fd, g_B, object_ids, 2);
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaFetchRequest);
  ObjectID object_ids_read[2];
  plasma_read_FetchRequest(data, &object_ids_read[0], 2);
  ASSERT(ObjectID_equal(object_ids[0], object_ids_read[0]));
  ASSERT(ObjectID_equal(object_ids[1], object_ids_read[1]));
  free(data);
  close(fd);
  PASS();
}

TEST plasma_wait_request_test(void) {
  int fd = create_temp_file();
  ObjectRequest object_requests[2];
  object_requests[0].object_id = globally_unique_id();
  object_requests[0].type = PLASMA_QUERY_ANYWHERE;
  object_requests[1].object_id = globally_unique_id();
  object_requests[1].type = PLASMA_QUERY_LOCAL;
  int num_ready_objects = 1;
  int64_t timeout_ms = 1000;
  plasma_send_WaitRequest(fd, g_B, object_requests, 2, num_ready_objects,
                          timeout_ms);
  /* Read message back. */
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaWaitRequest);
  ObjectRequest object_requests_read[2];
  int num_object_ids_read = plasma_read_WaitRequest_num_object_ids(data);
  ASSERT_EQ(num_object_ids_read, 2);
  int num_ready_objects_read;
  int64_t timeout_ms_read;
  plasma_read_WaitRequest(data, &object_requests_read[0], num_object_ids_read,
                          &timeout_ms_read, &num_ready_objects_read);
  ASSERT(ObjectID_equal(object_requests[0].object_id,
                        object_requests_read[0].object_id));
  ASSERT(ObjectID_equal(object_requests[1].object_id,
                        object_requests_read[1].object_id));
  ASSERT(object_requests[0].type == object_requests_read[0].type);
  ASSERT(object_requests[1].type == object_requests_read[1].type);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_wait_reply_test(void) {
  int fd = create_temp_file();
  ObjectRequest object_replies1[2];
  object_replies1[0].object_id = globally_unique_id();
  object_replies1[0].status = ObjectStatus_Local;
  object_replies1[1].object_id = globally_unique_id();
  object_replies1[1].status = ObjectStatus_Nonexistent;
  int num_ready_objects1 = 2;
  plasma_send_WaitReply(fd, g_B, object_replies1, num_ready_objects1);
  /* Read message back. */
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaWaitReply);
  ObjectRequest object_replies2[2];
  int num_ready_objects_read2;
  plasma_read_WaitReply(data, &object_replies2[0], &num_ready_objects_read2);
  ASSERT(ObjectID_equal(object_replies1[0].object_id,
                        object_replies2[0].object_id));
  ASSERT(ObjectID_equal(object_replies1[1].object_id,
                        object_replies2[1].object_id));
  ASSERT(object_replies1[0].status == object_replies2[0].status);
  ASSERT(object_replies1[1].status == object_replies2[1].status);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_data_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  const char *address1 = "address1";
  int port1 = 12345;
  plasma_send_DataRequest(fd, g_B, object_id1, address1, port1);
  /* Reading message back. */
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaDataRequest);
  ObjectID object_id2;
  char *address2;
  int port2;
  plasma_read_DataRequest(data, &object_id2, &address2, &port2);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  ASSERT(strcmp(address1, address2) == 0);
  ASSERT(port1 == port2);
  free(address2);
  free(data);
  close(fd);
  PASS();
}

TEST plasma_data_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = globally_unique_id();
  int64_t object_size1 = 146;
  int64_t metadata_size1 = 198;
  plasma_send_DataReply(fd, g_B, object_id1, object_size1, metadata_size1);
  /* Reading message back. */
  uint8_t *data = read_message_from_file(fd, MessageType_PlasmaDataReply);
  ObjectID object_id2;
  int64_t object_size2;
  int64_t metadata_size2;
  plasma_read_DataReply(data, &object_id2, &object_size2, &metadata_size2);
  ASSERT(ObjectID_equal(object_id1, object_id2));
  ASSERT(object_size1 == object_size2);
  ASSERT(metadata_size1 == metadata_size2);
  free(data);
  PASS();
}

SUITE(plasma_serialization_tests) {
  RUN_TEST(plasma_create_request_test);
  RUN_TEST(plasma_create_reply_test);
  RUN_TEST(plasma_seal_request_test);
  RUN_TEST(plasma_seal_reply_test);
  RUN_TEST(plasma_get_request_test);
  RUN_TEST(plasma_get_reply_test);
  RUN_TEST(plasma_release_request_test);
  RUN_TEST(plasma_release_reply_test);
  RUN_TEST(plasma_delete_request_test);
  RUN_TEST(plasma_delete_reply_test);
  RUN_TEST(plasma_status_request_test);
  RUN_TEST(plasma_status_reply_test);
  RUN_TEST(plasma_evict_request_test);
  RUN_TEST(plasma_evict_reply_test);
  RUN_TEST(plasma_fetch_request_test);
  RUN_TEST(plasma_wait_request_test);
  RUN_TEST(plasma_wait_reply_test);
  RUN_TEST(plasma_data_request_test);
  RUN_TEST(plasma_data_reply_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  g_B = make_protocol_builder();
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_serialization_tests);
  GREATEST_MAIN_END();
  free_protocol_builder(g_B);
}
