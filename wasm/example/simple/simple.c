#include "stdio.h"

__attribute__((import_module("ray"), import_name("test"))) void test();

__attribute__((import_module("ray"), import_name("sleep"))) void sleep(int);

__attribute__((import_module("ray"), import_name("call"))) int rcall(void *, ...);

int test2(int a, short b, char c, float d) { return 0; }

int _start() {
  test();
  int res = rcall(test2, 0x789abcd, 0x1234, 'A', 0.1);
  sleep(10);
  return res;
}
