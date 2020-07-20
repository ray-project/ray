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
  InitShutdownRAII(InitFunc init_func, ShutdownFunc shutdown_func, Args &&... args)
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

/// A helper function to fill random bytes into the `data`.
/// Warning: this is not fork-safe, we need to re-seed after that.
template <typename T>
void FillRandom(T *data) {
  RAY_CHECK(data != nullptr);
  auto randomly_seeded_mersenne_twister = []() {
    auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    // To increase the entropy, mix in a number of time samples instead of a single one.
    // This avoids the possibility of duplicate seeds for many workers that start in
    // close succession.
    for (int i = 0; i < 128; i++) {
      std::this_thread::sleep_for(std::chrono::microseconds(10));
      seed += std::chrono::high_resolution_clock::now().time_since_epoch().count();
    }
    std::mt19937 seeded_engine(seed);
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
