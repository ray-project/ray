#pragma once

#include <ray/api.h>
#include <iostream>

class Foo {
 public:
  int count;

  MSGPACK_DEFINE(count);

  Foo() { count = 0; }

  static Foo *create() {
    Foo *foo = new Foo();
    return foo;
  }

  int foo(int x) { return x + 1; }

  int bar(int x, int y) { return x + y; }

  int add(int x) {
    count += x;
    return count;
  }

  static int foo_s(int x) { return x + 1; }

  static int bar_s(int x, int y) { return x + y; }
};