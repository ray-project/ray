#pragma once

#include <ray/api.h>
#include <iostream>

class Foo {
 public:
  int count;

  Foo() { count = 0; }

  static Foo *create() {
    Foo *foo = new Foo();
    return foo;
  }

  int foo(int x) {
    return x + 1;
  }

  int bar(int x, int y) {
    return x + y;
  }

  int add(int x) {
    count += x;
    return count;
  }

  static int foo_s(int x) {
    return x + 1;
  }

  static int bar_s(int x, int y) {
    return x + y;
  }
};

namespace ray {

inline void marshall(::ray::binary_writer &writer, const Foo &foo) {
  writer.write((char *)&foo.count, sizeof(int));
}

inline void unmarshall(::ray::binary_reader &reader, Foo &foo) {
  reader.read((char *)&foo.count, sizeof(int));
}

inline void marshall(::ray::binary_writer &writer, Foo *const &foo) {
  writer.write((char *)&foo, sizeof(Foo *));
}

inline void unmarshall(::ray::binary_reader &reader, Foo *&foo) {
  reader.read((char *)&foo, sizeof(Foo *));
}
}  // namespace ray