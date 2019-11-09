
#pragma once

#include <ray/api/Blob.h>
#include <cstdint>

namespace ray {

// thread local memory management
class Tlm {
 public:
  static void begin();
  static Blob end();
  static void write(const char *ptr, uint32_t sz);
};
}
