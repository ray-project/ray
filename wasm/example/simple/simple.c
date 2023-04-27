#include "stdio.h"

#define RAY_OBJECT_ID_MAGIC 0xc0de550a

#define RAY_TYPE_INVALID 0
#define RAY_TYPE_OBJECT_ID 0x1

typedef struct {
  unsigned int magic;
  unsigned int type;
  unsigned char *buf;
  unsigned int len;
  unsigned int cap;
} ray_buffer;

__attribute__((import_module("ray"), import_name("test"))) void test();

__attribute__((import_module("ray"), import_name("sleep"))) void sleep(int);

__attribute__((import_module("ray"), import_name("call"))) int rcall(ray_buffer *id,
                                                                     void *func,
                                                                     ...);

__attribute__((import_module("ray"), import_name("get"))) int rget(ray_buffer *id,
                                                                   ray_buffer *buf);

int test2(int a, short b, char c, float d) { return 0; }

int _start() {
  ray_buffer rb;
  unsigned char buf[32];
  ray_buffer rb2;
  unsigned char buf2[100];

  test();

  rb.magic = RAY_OBJECT_ID_MAGIC;
  rb.type = RAY_TYPE_OBJECT_ID;
  rb.buf = buf;
  rb.len = 0;
  rb.cap = sizeof(buf);

  int res = rcall(&rb, test2, 0x789abcd, 0x1234, 'A', 0.1);
  if (res != 0) {
    return res;
  }

  rb2.magic = RAY_OBJECT_ID_MAGIC;
  rb2.type = RAY_TYPE_INVALID;
  rb2.buf = buf2;
  rb2.len = 0;
  rb2.cap = sizeof(buf2);

  res = rget(&rb, &rb2);
  if (res != 0) {
    return res;
  }

  sleep(10);
  return 0;
}
