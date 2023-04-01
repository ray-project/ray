#include "stdio.h"

__attribute__((import_module("ray"), import_name("test"))) void test();

int _start() {
  test();
  return 0;
}
