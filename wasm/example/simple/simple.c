#include "stdio.h"

__attribute__((import_module("ray"), import_name("test"))) void test();

__attribute__((import_module("ray"), import_name("call"))) int rcall(void *, ...);

int my_func() { return 0; }

int _start() {
  test();
  int res = rcall(my_func, 0x1234, 0x5678, 0x9abc, 0xdef0);
  return res;
}
