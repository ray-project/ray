#ifndef RAY_UTIL_UTIL_H
#define RAY_UTIL_UTIL_H

#include <boost/system/error_code.hpp>
#include <chrono>
#include "ray/status.h"

/// Return the number of milliseconds since the steady clock epoch. NOTE: The
/// returned timestamp may be used for accurately measuring intervals but has
/// no relation to wall clock time. It must not be used for synchronization
/// across multiple nodes.
///
/// TODO(rkn): This function appears in multiple places. It should be
/// deduplicated.
///
/// \return The number of milliseconds since the steady clock epoch.
inline int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

inline int64_t current_sys_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

inline ray::Status boost_to_ray_status(const boost::system::error_code &error) {
  switch (error.value()) {
  case boost::system::errc::success:
    return ray::Status::OK();
  default:
    return ray::Status::IOError(strerror(error.value()));
  }
}

template <class Shutdown>
class InitShutdownWrapper {
 public:
  /// Create an instance of InitShutdownWrapper which will call shuntdown
  /// function when it is out of scope.
  ///
  /// \param init_func The init function.
  /// \param shuntdown_func The shutdown function.
  /// \param args The auguments for the init function.
  template <class Init, class... Args>
  InitShutdownWrapper(Init init_func, Shutdown shuntdown_func, Args &&... args)
      : shutdown(shuntdown_func) {
    init_func(args...);
  }

  /// Destructor of InitShutdownWrapper which will call the shutdown function.
  ~InitShutdownWrapper() { shutdown(); }

 private:
  Shutdown shutdown;
};

// Most of the shutdown function is the type of void (*)().
typedef InitShutdownWrapper<void (*)()> DefaultInitShutdown;

#endif  // RAY_UTIL_UTIL_H
