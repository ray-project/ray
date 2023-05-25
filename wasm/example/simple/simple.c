#include "ray.h"
#include "stdio.h"

float remote_invoke(int a, short b, char c, float d) { return a + b + c + d; }

int test_put() {
  ray_buffer *rb = alloc_ray_buffer(RAY_TYPE_OBJECT_ID, 32);
  unsigned char data[32] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb,
                            0xc, 0xd, 0xe, 0xf, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6,
                            0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x0};
  unsigned char result_buf[100];
  unsigned int len = sizeof(result_buf);

  int res = rput(rb, data, sizeof(data));
  if (res != 0) {
    fprintf(stderr, "rput failed: %d\n", res);
    return res;
  }

  res = rget(rb, result_buf, &len);
  if (res != 0) {
    fprintf(stderr, "rget failed: %d\n", res);
    return res;
  }

  free_ray_buffer(rb);
  return 0;
}

int test_call() {
  unsigned char result_buf[100];
  unsigned int len = sizeof(result_buf);

  test();

  ray_buffer *rb = alloc_ray_buffer(RAY_TYPE_OBJECT_ID, 32);
  if (rb == NULL) {
    fprintf(stderr, "alloc_ray_buffer failed\n");
    return 1;
  }

  int res = rcall(rb, remote_invoke, 0x789abcd, 0x1234, 'A', 0.1);
  if (res != 0) {
    fprintf(stderr, "rcall failed: %d\n", res);
    return res;
  }

  res = rget(rb, result_buf, &len);
  if (res != 0) {
    fprintf(stderr, "rget failed: %d\n", res);
    return res;
  }

  free_ray_buffer(rb);
  return 0;
}

int main() {
  fprintf(stderr, "starting\n");
  int res = rinit();
  if (res != 0) {
    fprintf(stderr, "rinit failed: %d\n", res);
    return res;
  }
  fprintf(stderr, "rinit ok\n");
  if (test_call() != 0) {
    fprintf(stderr, "test_call failed\n");
    return 1;
  }
  fprintf(stderr, "test_call ok\n");
  if (test_put() != 0) {
    fprintf(stderr, "test_put failed\n");
    return 1;
  }
  fprintf(stderr, "test_put ok\n");
  return 0;
}
