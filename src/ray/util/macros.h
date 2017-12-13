#ifndef RAY_UTIL_MACROS_H
#define RAY_UTIL_MACROS_H

// From Google gutil
#ifndef RAY_DISALLOW_COPY_AND_ASSIGN
#define RAY_DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName &) = delete;         \
  void operator=(const TypeName &) = delete
#endif

#define RAY_UNUSED(x) (void) x

//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define RAY_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define RAY_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define RAY_NORETURN __attribute__((noreturn))
#define RAY_PREFETCH(addr) __builtin_prefetch(addr)
#elif defined(_MSC_VER)
#define RAY_NORETURN __declspec(noreturn)
#define RAY_PREDICT_FALSE(x) x
#define RAY_PREDICT_TRUE(x) x
#define RAY_PREFETCH(addr)
#else
#define RAY_NORETURN
#define RAY_PREDICT_FALSE(x) x
#define RAY_PREDICT_TRUE(x) x
#define RAY_PREFETCH(addr)
#endif

#if (defined(__GNUC__) || defined(__APPLE__))
#define RAY_MUST_USE_RESULT __attribute__((warn_unused_result))
#elif defined(_MSC_VER)
#define RAY_MUST_USE_RESULT
#else
#define RAY_MUST_USE_RESULT
#endif

#endif  // RAY_UTIL_MACROS_H
