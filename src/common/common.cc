#include "common.h"

#include <chrono>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "io.h"
#include <functional>

const UniqueID NIL_ID = UniqueID::nil();

const unsigned char NIL_DIGEST[DIGEST_SIZE] = {0};

UniqueID globally_unique_id(void) {
  /* Use /dev/urandom for "real" randomness. */
  int fd;
  int const flags = 0 /* for Windows compatibility */;
  if ((fd = open("/dev/urandom", O_RDONLY, flags)) == -1) {
    LOG_ERROR("Could not generate random number");
  }
  UniqueID result;
  CHECK(read_bytes(fd, &result.id[0], UNIQUE_ID_SIZE) >= 0);
  close(fd);
  return result;
}

/* ObjectID equality function. */
bool operator==(const ObjectID &x, const ObjectID &y) {
  return UNIQUE_ID_EQ(x, y);
}

bool ObjectID_equal(ObjectID first_id, ObjectID second_id) {
  return UNIQUE_ID_EQ(first_id, second_id);
}

bool ObjectID_is_nil(ObjectID id) {
  return ObjectID_equal(id, NIL_OBJECT_ID);
}

bool DBClientID_equal(DBClientID first_id, DBClientID second_id) {
  return UNIQUE_ID_EQ(first_id, second_id);
}

bool DBClientID_is_nil(DBClientID id) {
  return IS_NIL_ID(id);
}

bool WorkerID_equal(WorkerID first_id, WorkerID second_id) {
  return UNIQUE_ID_EQ(first_id, second_id);
}

char *ObjectID_to_string(ObjectID obj_id, char *id_string, int id_length) {
  CHECK(id_length >= ID_STRING_SIZE);
  static const char hex[] = "0123456789abcdef";
  char *buf = id_string;

  for (int i = 0; i < UNIQUE_ID_SIZE; i++) {
    unsigned int val = obj_id.id[i];
    *buf++ = hex[val >> 4];
    *buf++ = hex[val & 0xf];
  }
  *buf = '\0';

  return id_string;
}

int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}
