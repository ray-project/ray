// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <chrono>
#include <iterator>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

#include "ray/util/logging.h"
#include "ray/util/macros.h"

#ifdef _WIN32
#include <process.h>  // to ensure getpid() on Windows
#endif

// Portable code for unreachable
#if defined(_MSC_VER)
#define UNREACHABLE __assume(0)
#else
#define UNREACHABLE __builtin_unreachable()
#endif

// Boost forward-declarations (to avoid forcing slow header inclusions)
namespace boost {

namespace asio {

namespace generic {

template <class Protocol>
class basic_endpoint;

class stream_protocol;

}  // namespace generic

}  // namespace asio

}  // namespace boost

enum class CommandLineSyntax { System, POSIX, Windows };

// Transfer the string to the Hex format. It can be more readable than the ANSI mode
inline std::string StringToHex(const std::string &str) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (size_t i = 0; i < str.size(); i++) {
    unsigned char val = str[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

// Append append_str to the begining of each line of str.
inline std::string AppendToEachLine(const std::string &str,
                                    const std::string &append_str) {
  std::stringstream ss;
  ss << append_str;
  for (char c : str) {
    ss << c;
    if (c == '\n') {
      ss << append_str;
    }
  }
  return ss.str();
}

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

inline int64_t current_sys_time_us() {
  std::chrono::microseconds mu_since_epoch =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  return mu_since_epoch.count();
}

/// A helper function to parse command-line arguments in a platform-compatible manner.
///
/// \param cmdline The command-line to split.
///
/// \return The command-line arguments, after processing any escape sequences.
std::vector<std::string> ParseCommandLine(
    const std::string &cmdline, CommandLineSyntax syntax = CommandLineSyntax::System);

/// A helper function to combine command-line arguments in a platform-compatible manner.
/// The result of this function is intended to be suitable for the shell used by popen().
///
/// \param cmdline The command-line arguments to combine.
///
/// \return The command-line string, including any necessary escape sequences.
std::string CreateCommandLine(const std::vector<std::string> &args,
                              CommandLineSyntax syntax = CommandLineSyntax::System);

/// Converts the given endpoint (such as TCP or UNIX domain socket address) to a string.
/// \param include_scheme Whether to include the scheme prefix (such as tcp://).
///                       This is recommended to avoid later ambiguity when parsing.
std::string EndpointToUrl(
    const boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol> &ep,
    bool include_scheme = true);

/// Parses the endpoint socket address of a URL.
/// If a scheme:// prefix is absent, the address family is guessed automatically.
/// For TCP/IP, the endpoint comprises the IP address and port number in the URL.
/// For UNIX domain sockets, the endpoint comprises the socket path.
boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol>
ParseUrlEndpoint(const std::string &endpoint, int default_port = 0);

/// Parse the url and return a pair of base_url and query string map.
/// EX) http://abc?num_objects=9&offset=8388878
/// will be returned as
/// {
///   url: http://abc,
///   num_objects: 9,
///   offset: 8388878
/// }
std::shared_ptr<std::unordered_map<std::string, std::string>> ParseURL(std::string url);

class InitShutdownRAII {
 public:
  /// Type of the Shutdown function.
  using ShutdownFunc = void (*)();

  /// Create an instance of InitShutdownRAII which will call shutdown
  /// function when it is out of scope.
  ///
  /// \param init_func The init function.
  /// \param shutdown_func The shutdown function.
  /// \param args The arguments for the init function.
  template <class InitFunc, class... Args>
  InitShutdownRAII(InitFunc init_func, ShutdownFunc shutdown_func, Args &&...args)
      : shutdown_(shutdown_func) {
    init_func(args...);
  }

  /// Destructor of InitShutdownRAII which will call the shutdown function.
  ~InitShutdownRAII() {
    if (shutdown_ != nullptr) {
      shutdown_();
    }
  }

 private:
  ShutdownFunc shutdown_;
};

struct EnumClassHash {
  template <typename T>
  std::size_t operator()(T t) const {
    return static_cast<std::size_t>(t);
  }
};

/// unordered_map for enum class type.
template <typename Key, typename T>
using EnumUnorderedMap = std::unordered_map<Key, T, EnumClassHash>;

namespace ray {
namespace internal {
inline __suppress_ubsan__("signed-integer-overflow") int64_t GenerateSeed() {
  int64_t seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  // To increase the entropy, mix in a number of time samples instead of a single one.
  // This avoids the possibility of duplicate seeds for many workers that start in
  // close succession.
  for (int i = 0; i < 128; i++) {
    std::this_thread::sleep_for(std::chrono::microseconds(10));
    seed += std::chrono::high_resolution_clock::now().time_since_epoch().count();
  }
  return seed;
}
}  // namespace internal
}  // namespace ray

/// A helper function to fill random bytes into the `data`.
/// Warning: this is not fork-safe, we need to re-seed after that.
template <typename T>
void FillRandom(T *data) {
  RAY_CHECK(data != nullptr);
  auto randomly_seeded_mersenne_twister = []() {
    std::mt19937 seeded_engine(ray::internal::GenerateSeed());
    return seeded_engine;
  };

  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex random_engine_mutex;
  std::lock_guard<std::mutex> lock(random_engine_mutex);
  static std::mt19937 generator = randomly_seeded_mersenne_twister();
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (size_t i = 0; i < data->size(); i++) {
    (*data)[i] = static_cast<uint8_t>(dist(generator));
  }
}

inline void SetThreadName(const std::string &thread_name) {
#if defined(__APPLE__)
  pthread_setname_np(thread_name.c_str());
#elif defined(__linux__)
  pthread_setname_np(pthread_self(), thread_name.substr(0, 15).c_str());
#endif
}

inline std::string GetThreadName() {
#if defined(__linux__)
  char name[128];
  auto rc = pthread_getname_np(pthread_self(), name, sizeof(name));
  if (rc != 0) {
    return "ERROR";
  } else {
    return name;
  }
#else
  return "UNKNOWN";
#endif
}

namespace ray {
template <typename T>
class ThreadPrivate {
 public:
  template <typename... Ts>
  explicit ThreadPrivate(Ts &&...ts) : t_(std::forward<Ts>(ts)...) {}

  T &operator*() {
    ThreadCheck();
    return t_;
  }

  T *operator->() {
    ThreadCheck();
    return &t_;
  }

  const T &operator*() const {
    ThreadCheck();
    return t_;
  }

  const T *operator->() const {
    ThreadCheck();
    return &t_;
  }

 private:
  void ThreadCheck() const {
    // ThreadCheck is not a thread safe function and at the same time, multiple
    // threads might be accessing id_ at the same time.
    // Here we only introduce mutex to protect write instead of read for the
    // following reasons:
    //    - read and write at the same time for `id_` is fine since this is a
    //      trivial object. And since we are using this to detect errors,
    //      it doesn't matter which value it is.
    //    - read and write of `thread_name_` is not good. But it will only be
    //      read when we crash the program.
    //
    if (id_ == std::thread::id()) {
      // Protect thread_name_
      std::lock_guard<std::mutex> _(mutex_);
      thread_name_ = GetThreadName();
      RAY_LOG(DEBUG) << "First accessed in thread " << thread_name_;
      id_ = std::this_thread::get_id();
    }

    RAY_CHECK(id_ == std::this_thread::get_id())
        << "A variable private to thread " << thread_name_ << " was accessed in thread "
        << GetThreadName();
  }

  T t_;
  mutable std::string thread_name_;
  mutable std::thread::id id_;
  mutable std::mutex mutex_;
};

class ExponentialBackOff {
 public:
  ExponentialBackOff() = default;
  ExponentialBackOff(const ExponentialBackOff &) = default;
  ExponentialBackOff(ExponentialBackOff &&) = default;
  ExponentialBackOff &operator=(const ExponentialBackOff &) = default;
  ExponentialBackOff &operator=(ExponentialBackOff &&) = default;

  /// Construct an exponential back off counter.
  ///
  /// \param[in] initial_value The start value for this counter
  /// \param[in] multiplier The multiplier for this counter.
  /// \param[in] max_value The maximum value for this counter. By default it's
  ///    infinite double.
  ExponentialBackOff(uint64_t initial_value, double multiplier,
                     uint64_t max_value = std::numeric_limits<uint64_t>::max())
      : curr_value_(initial_value),
        initial_value_(initial_value),
        max_value_(max_value),
        multiplier_(multiplier) {
    RAY_CHECK(multiplier > 0.0) << "Multiplier must be greater than 0";
  }

  uint64_t Next() {
    auto ret = curr_value_;
    curr_value_ = curr_value_ * multiplier_;
    curr_value_ = std::min(curr_value_, max_value_);
    return ret;
  }

  void Reset() { curr_value_ = initial_value_; }

 private:
  uint64_t curr_value_;
  uint64_t initial_value_;
  uint64_t max_value_;
  double multiplier_;
};

}  // namespace ray
