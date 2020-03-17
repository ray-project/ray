#pragma once

#include <ray/api.h>
#include <iostream>

class FooTest {
 public:
  int count;

  MSGPACK_DEFINE(count);

  FooTest() { count = 0; }

  static FooTest *Create() {
    FooTest *fooTest = new FooTest();
    return fooTest;
  }

  int Foo(int x) { return x + 1; }

  int Bar(int x, int y) { return x + y; }

  int Add(int x) {
    count += x;
    return count;
  }

  static int Foo_s(int x) { return x + 1; }

  static int Bar_s(int x, int y) { return x + y; }
};