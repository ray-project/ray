#include "ray/common/nullable_uint.h"

#include <algorithm>

#include "ray/util/logging.h"

namespace ray {

nuint64_t NullableInt::min(nuint64_t left, nuint64_t right) {
  NullableInt::assertValidNullableInt(left);
  NullableInt::assertValidNullableInt(right);

  if (left == kNull && right == kNull) {
    return kNull;
  } else if (left == kNull) {
    return right;
  } else if (right == kNull) {
    return left;
  } else {
    return std::min(left, right);
  }
}

void NullableInt::assertValidNullableInt(nuint64_t value) { RAY_CHECK_GE(value, -1); }

}  // namespace ray
