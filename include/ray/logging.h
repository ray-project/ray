#include <string>
#include <iostream>
#include <fstream>

#include <grpc++/grpc++.h>

struct RayConfig {
  bool log_to_file = false;
  std::ofstream logfile;
};

extern RayConfig global_ray_config;

#define RAY_VERBOSE -1
#define RAY_INFO 0
#define RAY_DEBUG 1
#define RAY_FATAL 2
#define RAY_REFCOUNT RAY_VERBOSE
#define RAY_ALIAS RAY_VERBOSE

#ifdef _MSC_VER
extern "C" __declspec(dllimport) int __stdcall IsDebuggerPresent();
#define RAY_BREAK_IF_DEBUGGING() IsDebuggerPresent() && (__debugbreak(), 1)
#else
#define RAY_BREAK_IF_DEBUGGING()
#endif

#define RAY_LOG(LEVEL, MESSAGE) \
  if (LEVEL == RAY_VERBOSE) { \
    \
  } else if (LEVEL == RAY_FATAL) { \
    std::cerr << "fatal error occured: " << MESSAGE << std::endl; \
    if (global_ray_config.log_to_file) { \
      global_ray_config.logfile << "fatal error occured: " << MESSAGE << std::endl; \
    } \
    RAY_BREAK_IF_DEBUGGING();  \
    std::exit(1); \
  } else if (LEVEL == RAY_DEBUG) { \
    \
  } else { \
    if (global_ray_config.log_to_file) { \
      global_ray_config.logfile << MESSAGE << std::endl; \
    } else { \
      std::cout << MESSAGE << std::endl; \
    } \
  }

#define RAY_CHECK(condition, message) \
  if (!(condition)) {\
     RAY_LOG(RAY_FATAL, "Check failed at line " << __LINE__ << " in " << __FILE__ << ": " << #condition << " with message " << message) \
  }
#define RAY_WARN(condition, message) \
  if (!(condition)) {\
     RAY_LOG(RAY_INFO, "Check failed at line " << __LINE__ << " in " << __FILE__ << ": " << #condition << " with message " << message) \
  }

#define RAY_CHECK_EQ(var1, var2, message) RAY_CHECK((var1) == (var2), message)
#define RAY_CHECK_NEQ(var1, var2, message) RAY_CHECK((var1) != (var2), message)
#define RAY_CHECK_LE(var1, var2, message) RAY_CHECK((var1) <= (var2), message)
#define RAY_CHECK_LT(var1, var2, message) RAY_CHECK((var1) < (var2), message)
#define RAY_CHECK_GE(var1, var2, message) RAY_CHECK((var1) >= (var2), message)
#define RAY_CHECK_GT(var1, var2, message) RAY_CHECK((var1) > (var2), message)

#define RAY_CHECK_GRPC(expr) \
  do { \
    grpc::Status _s = (expr); \
    RAY_WARN(_s.ok(), "grpc call failed with message " << _s.error_message()); \
  } while (0);
