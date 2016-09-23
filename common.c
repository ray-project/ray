#include "common.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

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

const signed char hexval_table[256] = {
    -1, -1, -1, -1, -1, -1, -1, -1, /* 00-07 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 08-0f */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 10-17 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 18-1f */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 20-27 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 28-2f */
    +0, +1, +2, +3, +4, +5, +6, +7, /* 30-37 */
    +8, +9, -1, -1, -1, -1, -1, -1, /* 38-3f */
    -1, 10, 11, 12, 13, 14, 15, -1, /* 40-47 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 48-4f */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 50-57 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 58-5f */
    -1, 10, 11, 12, 13, 14, 15, -1, /* 60-67 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 68-67 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 70-77 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 78-7f */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 80-87 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 88-8f */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 90-97 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* 98-9f */
    -1, -1, -1, -1, -1, -1, -1, -1, /* a0-a7 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* a8-af */
    -1, -1, -1, -1, -1, -1, -1, -1, /* b0-b7 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* b8-bf */
    -1, -1, -1, -1, -1, -1, -1, -1, /* c0-c7 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* c8-cf */
    -1, -1, -1, -1, -1, -1, -1, -1, /* d0-d7 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* d8-df */
    -1, -1, -1, -1, -1, -1, -1, -1, /* e0-e7 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* e8-ef */
    -1, -1, -1, -1, -1, -1, -1, -1, /* f0-f7 */
    -1, -1, -1, -1, -1, -1, -1, -1, /* f8-ff */
};

static inline unsigned int hexval(unsigned char c) {
  return hexval_table[c];
}

/*
 * Convert two consecutive hexadecimal digits into a char.  Return a
 * negative value on error.  Don't run over the end of short strings.
 */
static inline int hex2chr(const char *s) {
  int val = hexval(s[0]);
  return (val < 0) ? val : (val << 4) | hexval(s[1]);
}

int hex_to_sha1(const char *hex, unsigned char *sha1) {
  int i;
  for (i = 0; i < UNIQUE_ID_SIZE; i++) {
    int val = hex2chr(hex);
    if (val < 0)
      return -1;
    *sha1++ = val;
    hex += 2;
  }
  return 0;
}
