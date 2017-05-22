#include "greatest.h"

#include <sys/types.h>
#include <unistd.h>

#include "plasma_common.h"
#include "plasma.h"
#include "plasma_io.h"
#include "plasma_protocol.h"

SUITE(plasma_serialization_tests);

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
std::vector<uint8_t> read_message_from_file(int fd, int message_type) {
  /* Go to the beginning of the file. */
  lseek(fd, 0, SEEK_SET);
  int64_t type;
  std::vector<uint8_t> data;
  ARROW_CHECK_OK(ReadMessage(fd, &type, data));
  ARROW_CHECK(type == message_type);
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
  ObjectID object_id1 = ObjectID::from_random();
  int64_t data_size1 = 42;
  int64_t metadata_size1 = 11;
  ARROW_CHECK_OK(SendCreateRequest(fd, object_id1, data_size1, metadata_size1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaCreateRequest);
  ObjectID object_id2;
  int64_t data_size2;
  int64_t metadata_size2;
  ARROW_CHECK_OK(ReadCreateRequest(data.data(), &object_id2, &data_size2, &metadata_size2));
  ASSERT_EQ(data_size1, data_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
  ASSERT(object_id1 == object_id2);
  close(fd);
  PASS();
}

TEST plasma_create_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  PlasmaObject object1 = random_plasma_object();
  ARROW_CHECK_OK(SendCreateReply(fd, object_id1, &object1, 0));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaCreateReply);
  ObjectID object_id2;
  PlasmaObject object2;
  memset(&object2, 0, sizeof(object2));
  ARROW_CHECK_OK(ReadCreateReply(data.data(), &object_id2, &object2));
  ASSERT(object_id1 == object_id2);
  ASSERT(memcmp(&object1, &object2, sizeof(object1)) == 0);
  close(fd);
  PASS();
}

TEST plasma_seal_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  unsigned char digest1[kDigestSize];
  memset(&digest1[0], 7, kDigestSize);
  ARROW_CHECK_OK(SendSealRequest(fd, object_id1, &digest1[0]));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaSealRequest);
  ObjectID object_id2;
  unsigned char digest2[kDigestSize];
  ARROW_CHECK_OK(ReadSealRequest(data.data(), &object_id2, &digest2[0]));
  ASSERT(object_id1 == object_id2);
  ASSERT(memcmp(&digest1[0], &digest2[0], kDigestSize) == 0);
  close(fd);
  PASS();
}

TEST plasma_seal_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  ARROW_CHECK_OK(SendSealReply(fd, object_id1, PlasmaError_ObjectExists));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaSealReply);
  ObjectID object_id2;
  Status s = ReadSealReply(data.data(), &object_id2);
  ASSERT(object_id1 == object_id2);
  ASSERT(s.IsPlasmaObjectExists());
  close(fd);
  PASS();
}

TEST plasma_get_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  int64_t timeout_ms = 1234;
  ARROW_CHECK_OK(SendGetRequest(fd, object_ids, 2, timeout_ms));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaGetRequest);
  std::vector<ObjectID> object_ids_return;
  int64_t timeout_ms_return;
  ARROW_CHECK_OK(ReadGetRequest(data.data(), object_ids_return, &timeout_ms_return));
  ASSERT(object_ids[0] == object_ids_return[0]);
  ASSERT(object_ids[1] == object_ids_return[1]);
  ASSERT(timeout_ms == timeout_ms_return);
  close(fd);
  PASS();
}

TEST plasma_get_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  std::unordered_map<ObjectID, PlasmaObject, UniqueIDHasher> plasma_objects;
  plasma_objects[object_ids[0]] = random_plasma_object();
  plasma_objects[object_ids[1]] = random_plasma_object();
  ARROW_CHECK_OK(SendGetReply(fd, object_ids, plasma_objects, 2));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaGetReply);
  ObjectID object_ids_return[2];
  PlasmaObject plasma_objects_return[2];
  memset(&plasma_objects_return, 0, sizeof(plasma_objects_return));
  ARROW_CHECK_OK(ReadGetReply(data.data(), object_ids_return, &plasma_objects_return[0], 2));
  ASSERT(object_ids[0] == object_ids_return[0]);
  ASSERT(object_ids[1] == object_ids_return[1]);
  ASSERT(memcmp(&plasma_objects[object_ids[0]], &plasma_objects_return[0],
                sizeof(PlasmaObject)) == 0);
  ASSERT(memcmp(&plasma_objects[object_ids[1]], &plasma_objects_return[1],
                sizeof(PlasmaObject)) == 0);
  close(fd);
  PASS();
}

TEST plasma_release_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  ARROW_CHECK_OK(SendReleaseRequest(fd, object_id1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaReleaseRequest);
  ObjectID object_id2;
  ARROW_CHECK_OK(ReadReleaseRequest(data.data(), &object_id2));
  ASSERT(object_id1 == object_id2);
  close(fd);
  PASS();
}

TEST plasma_release_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  ARROW_CHECK_OK(SendReleaseReply(fd, object_id1, PlasmaError_ObjectExists));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaReleaseReply);
  ObjectID object_id2;
  Status s = ReadReleaseReply(data.data(), &object_id2);
  ASSERT(object_id1 == object_id2);
  ASSERT(s.IsPlasmaObjectExists());
  close(fd);
  PASS();
}

TEST plasma_delete_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  ARROW_CHECK_OK(SendDeleteRequest(fd, object_id1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaDeleteRequest);
  ObjectID object_id2;
  ARROW_CHECK_OK(ReadDeleteRequest(data.data(), &object_id2));
  ASSERT(object_id1 == object_id2);
  close(fd);
  PASS();
}

TEST plasma_delete_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  int error1 = PlasmaError_ObjectExists;
  ARROW_CHECK_OK(SendDeleteReply(fd, object_id1, error1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaDeleteReply);
  ObjectID object_id2;
  Status s = ReadDeleteReply(data.data(), &object_id2);
  ASSERT(object_id1 == object_id2);
  ASSERT(s.IsPlasmaObjectExists());
  close(fd);
  PASS();
}

TEST plasma_status_request_test(void) {
  int fd = create_temp_file();
  int64_t num_objects = 2;
  ObjectID object_ids[num_objects];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  ARROW_CHECK_OK(SendStatusRequest(fd, object_ids, num_objects));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaStatusRequest);
  ObjectID object_ids_read[num_objects];
  ARROW_CHECK_OK(ReadStatusRequest(data.data(), object_ids_read, num_objects));
  ASSERT(object_ids[0] == object_ids_read[0]);
  ASSERT(object_ids[1] == object_ids_read[1]);
  close(fd);
  PASS();
}

TEST plasma_status_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  int object_statuses[2] = {42, 43};
  ARROW_CHECK_OK(SendStatusReply(fd, object_ids, object_statuses, 2));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaStatusReply);
  int64_t num_objects = ReadStatusReply_num_objects(data.data());
  ObjectID object_ids_read[num_objects];
  int object_statuses_read[num_objects];
  ARROW_CHECK_OK(ReadStatusReply(data.data(), object_ids_read, object_statuses_read,
                          num_objects));
  ASSERT(object_ids[0] == object_ids_read[0]);
  ASSERT(object_ids[1] == object_ids_read[1]);
  ASSERT_EQ(object_statuses[0], object_statuses_read[0]);
  ASSERT_EQ(object_statuses[1], object_statuses_read[1]);
  close(fd);
  PASS();
}

TEST plasma_evict_request_test(void) {
  int fd = create_temp_file();
  int64_t num_bytes = 111;
  ARROW_CHECK_OK(SendEvictRequest(fd, num_bytes));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaEvictRequest);
  int64_t num_bytes_received;
  ARROW_CHECK_OK(ReadEvictRequest(data.data(), &num_bytes_received));
  ASSERT_EQ(num_bytes, num_bytes_received);
  close(fd);
  PASS();
}

TEST plasma_evict_reply_test(void) {
  int fd = create_temp_file();
  int64_t num_bytes = 111;
  ARROW_CHECK_OK(SendEvictReply(fd, num_bytes));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaEvictReply);
  int64_t num_bytes_received;
  ARROW_CHECK_OK(ReadEvictReply(data.data(), num_bytes_received));
  ASSERT_EQ(num_bytes, num_bytes_received);
  close(fd);
  PASS();
}

TEST plasma_fetch_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  ARROW_CHECK_OK(SendFetchRequest(fd, object_ids, 2));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaFetchRequest);
  std::vector<ObjectID> object_ids_read;
  ARROW_CHECK_OK(ReadFetchRequest(data.data(), object_ids_read));
  ASSERT(object_ids[0] == object_ids_read[0]);
  ASSERT(object_ids[1] == object_ids_read[1]);
  close(fd);
  PASS();
}

TEST plasma_wait_request_test(void) {
  int fd = create_temp_file();
  const int num_objects_in = 2;
  ObjectRequest object_requests_in[num_objects_in] = {
      ObjectRequest({ObjectID::from_random(), PLASMA_QUERY_ANYWHERE, 0}),
      ObjectRequest({ObjectID::from_random(), PLASMA_QUERY_LOCAL, 0})};
  const int num_ready_objects_in = 1;
  int64_t timeout_ms = 1000;

  ARROW_CHECK_OK(SendWaitRequest(fd, &object_requests_in[0], num_objects_in,
                          num_ready_objects_in, timeout_ms));
  /* Read message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaWaitRequest);
  int num_ready_objects_out;
  int64_t timeout_ms_read;
  ObjectRequestMap object_requests_out;
  ARROW_CHECK_OK(ReadWaitRequest(data.data(), object_requests_out,
                          &timeout_ms_read, &num_ready_objects_out));
  ASSERT_EQ(num_objects_in, object_requests_out.size());
  ASSERT_EQ(num_ready_objects_out, num_ready_objects_in);
  for (int i = 0; i < num_objects_in; i++) {
    const ObjectID &object_id = object_requests_in[i].object_id;
    ASSERT_EQ(1, object_requests_out.count(object_id));
    const auto &entry = object_requests_out.find(object_id);
    ASSERT(entry != object_requests_out.end());
    ASSERT(entry->second.object_id == object_requests_in[i].object_id);
    ASSERT_EQ(entry->second.type, object_requests_in[i].type);
  }
  close(fd);
  PASS();
}

TEST plasma_wait_reply_test(void) {
  int fd = create_temp_file();
  const int num_objects_in = 2;
  /* Create a map with two ObjectRequests in it. */
  ObjectRequestMap objects_in(num_objects_in);
  ObjectID id1 = ObjectID::from_random();
  objects_in[id1] = ObjectRequest({id1, 0, ObjectStatus_Local});
  ObjectID id2 = ObjectID::from_random();
  objects_in[id2] = ObjectRequest({id2, 0, ObjectStatus_Nonexistent});

  ARROW_CHECK_OK(SendWaitReply(fd, objects_in, num_objects_in));
  /* Read message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaWaitReply);
  ObjectRequest objects_out[2];
  int num_objects_out;
  ARROW_CHECK_OK(ReadWaitReply(data.data(), &objects_out[0], &num_objects_out));
  ASSERT(num_objects_in == num_objects_out);
  for (int i = 0; i < num_objects_out; i++) {
    /* Each object request must appear exactly once. */
    ASSERT(1 == objects_in.count(objects_out[i].object_id));
    const auto &entry = objects_in.find(objects_out[i].object_id);
    ASSERT(entry != objects_in.end());
    ASSERT(entry->second.object_id == objects_out[i].object_id);
    ASSERT(entry->second.status == objects_out[i].status);
  }
  close(fd);
  PASS();
}

TEST plasma_data_request_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  const char *address1 = "address1";
  int port1 = 12345;
  ARROW_CHECK_OK(SendDataRequest(fd, object_id1, address1, port1));
  /* Reading message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaDataRequest);
  ObjectID object_id2;
  char *address2;
  int port2;
  ARROW_CHECK_OK(ReadDataRequest(data.data(), &object_id2, &address2, &port2));
  ASSERT(object_id1 == object_id2);
  ASSERT(strcmp(address1, address2) == 0);
  ASSERT(port1 == port2);
  free(address2);
  close(fd);
  PASS();
}

TEST plasma_data_reply_test(void) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  int64_t object_size1 = 146;
  int64_t metadata_size1 = 198;
  ARROW_CHECK_OK(SendDataReply(fd, object_id1, object_size1, metadata_size1));
  /* Reading message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaDataReply);
  ObjectID object_id2;
  int64_t object_size2;
  int64_t metadata_size2;
  ARROW_CHECK_OK(ReadDataReply(data.data(), &object_id2, &object_size2, &metadata_size2));
  ASSERT(object_id1 == object_id2);
  ASSERT(object_size1 == object_size2);
  ASSERT(metadata_size1 == metadata_size2);
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
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_serialization_tests);
  GREATEST_MAIN_END();
}
