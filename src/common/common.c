#include "common.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* This is used to define the array of object IDs. */
const UT_icd object_id_icd = {sizeof(object_id), NULL, NULL, NULL};

const unique_id NIL_ID = {{255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                           255, 255, 255, 255, 255, 255, 255, 255, 255, 255}};

unique_id globally_unique_id(void) {
  /* Use /dev/urandom for "real" randomness. */
  int fd;
  if ((fd = open("/dev/urandom", O_RDONLY)) == -1) {
    LOG_ERR("Could not generate random number");
  }
  unique_id result;
  read(fd, &result.id[0], UNIQUE_ID_SIZE);
  close(fd);
  return result;
}

bool object_ids_equal(object_id first_id, object_id second_id) {
  return UNIQUE_ID_EQ(first_id, second_id) ? true : false;
}

bool object_id_is_nil(object_id id) {
  return object_ids_equal(id, NIL_OBJECT_ID);
}

char *sha1_to_hex(const unsigned char *sha1, char *buffer) {
  static const char hex[] = "0123456789abcdef";
  char *buf = buffer;

  for (int i = 0; i < UNIQUE_ID_SIZE; i++) {
    unsigned int val = *sha1++;
    *buf++ = hex[val >> 4];
    *buf++ = hex[val & 0xf];
  }
  *buf = '\0';

  return buffer;
}
