
#pragma once

#include <stdint.h>

#include <optional>

namespace ray {

/// Represents a nullable uint64_t using int64_t, using the most significant bit to
/// represent null - when the bit is set the value is null.
typedef int64_t nuint64_t;

/// Represents null for a nullable integer value
static constexpr int64_t kNull = -1;

/// Defines a nullable, unsigned integer.
class NullableInt {
 public:
  /// Returns the smaller of the two integers, kNull if both are kNull,
  /// or one of the values if the other is kNull.
  static nuint64_t min(nuint64_t left, nuint64_t right);

  /// Checks value is a valid nullable int else fails (logs fatal)
  static void assertValidNullableInt(nuint64_t value);
};

}  // namespace ray