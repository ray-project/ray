#include "common.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "io.h"
#include <functional>

/* This is used to define the array of object IDs. */
const UT_icd object_id_icd = {sizeof(ObjectID), NULL, NULL, NULL};

const UniqueID NIL_ID = {{255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                          255, 255, 255, 255, 255, 255, 255, 255, 255, 255}};

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

/* ObjectID hashing function. */
size_t hashObjectID(const ObjectID &key) {
    uint32_t acc = 0;
    for (int i = 0; i < UNIQUE_ID_SIZE / sizeof(uint32_t); i++) {
      acc ^=  *reinterpret_cast<const uint32_t *>(&key.id[i * sizeof(uint32_t)]);
    }
    return std::hash<uint32_t>()(acc);
}
/* ObjectID equality function. */
bool operator==(const ObjectID& x, const ObjectID& y) {
  if ((*reinterpret_cast<const uint32_t *>(&x.id[0])) !=
      (*reinterpret_cast<const uint32_t *>(&y.id[0])))
      return false;
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
