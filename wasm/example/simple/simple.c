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
  unsigned int checksum;
} ray_buffer;

__attribute__((import_module("ray"), import_name("test"))) void test();

__attribute__((import_module("ray"), import_name("sleep"))) void sleep(int);

__attribute__((import_module("ray"), import_name("call"))) int rcall(ray_buffer *id,
                                                                     void *func,
                                                                     ...);

__attribute__((import_module("ray"), import_name("get"))) int rget(ray_buffer *id,
                                                                   unsigned char *buf,
                                                                   unsigned int *len);

__attribute__((import_module("ray"), import_name("put"))) int rput(ray_buffer *id,
                                                                   unsigned char *buf,
                                                                   unsigned int len);

float remote_invoke(int a, short b, char c, float d) { return a + b + c + d; }

int test_put() {
  ray_buffer rb;
  unsigned char buf[32] = {0};
  unsigned char data[32] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb,
                            0xc, 0xd, 0xe, 0xf, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6,
                            0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x0};
  unsigned char buf2[100];
  unsigned int len = sizeof(buf2);

  rb.magic = RAY_OBJECT_ID_MAGIC;
  rb.type = RAY_TYPE_OBJECT_ID;
  rb.buf = buf;
  rb.len = 0;
  rb.cap = sizeof(buf);
  rb.checksum = rb.magic ^ rb.type ^ (unsigned int)rb.buf ^ rb.len ^ rb.cap;

  int res = rput(&rb, data, sizeof(data));
  if (res != 0) {
    return res;
  }

  res = rget(&rb, buf2, &len);
  if (res != 0) {
    return res;
  }

  return 0;
}

int test_call() {
  ray_buffer rb;
  unsigned char buf[32];
  unsigned char buf2[100];
  unsigned int len = sizeof(buf2);

  test();

  rb.magic = RAY_OBJECT_ID_MAGIC;
  rb.type = RAY_TYPE_OBJECT_ID;
  rb.buf = buf;
  rb.len = 0;
  rb.cap = sizeof(buf);
  rb.checksum = rb.magic ^ rb.type ^ (unsigned int)rb.buf ^ rb.len ^ rb.cap;

  int res = rcall(&rb, remote_invoke, 0x789abcd, 0x1234, 'A', 0.1);
  if (res != 0) {
    return res;
  }

  res = rget(&rb, buf2, &len);
  if (res != 0) {
    return res;
  }

  return 0;
}

int _start() {
  if (test_call() != 0) {
    return 1;
  }
  if (test_put() != 0) {
    return 1;
  }
  return 0;
}
