// A few util macros.

#pragma once

// Need to have two macro invocation to allow [x] and [y] to be replaced.
#define __RAY_CONCAT(x, y) x##y

#define RAY_CONCAT(x, y) __RAY_CONCAT(x, y)

// Macros which gets unique variable name (at best effort).
#define RAY_UNIQUE_VARIABLE(base) RAY_CONCAT(base, __LINE__)
