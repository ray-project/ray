#define RAY_VERBOSE -1
#define RAY_INFO 0
#define RAY_DEBUG 1
#define RAY_FATAL 2
#define RAY_REFCOUNT RAY_VERBOSE
#define RAY_ALIAS RAY_VERBOSE

#define RAY_LOG(LEVEL, MESSAGE) \
  if (LEVEL == RAY_VERBOSE) { \
    \
  } else if (LEVEL == RAY_FATAL) { \
    std::cerr << "fatal error occured: " << MESSAGE << std::endl; \
    std::exit(1); \
  } else if (LEVEL == RAY_DEBUG) { \
    \
  } else { \
    std::cout << MESSAGE << std::endl; \
  }

#define RAY_CHECK(condition, message) \
  if (!(condition)) {\
     RAY_LOG(RAY_FATAL, "Check failed at line " << __LINE__ << " in " << __FILE__ << ": " << #condition << " with message " << message) \
  }

#define RAY_CHECK_EQ(var1, var2, message) RAY_CHECK((var1) == (var2), message)
#define RAY_CHECK_NEQ(var1, var2, message) RAY_CHECK((var1) != (var2), message)
#define RAY_CHECK_LE(var1, var2, message) RAY_CHECK((var1) <= (var2), message)
#define RAY_CHECK_LT(var1, var2, message) RAY_CHECK((var1) < (var2), message)
#define RAY_CHECK_GE(var1, var2, message) RAY_CHECK((var1) >= (var2), message)
#define RAY_CHECK_GT(var1, var2, message) RAY_CHECK((var1) > (var2), message)
