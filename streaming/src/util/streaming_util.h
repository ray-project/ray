#pragma once

#include <boost/any.hpp>
#include <functional>
#include <string>
#include <unordered_map>

#include "ray/common/id.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

class Util {
 public:
  static std::string Byte2hex(const uint8_t *data, uint32_t data_size);

  static std::string Hexqid2str(const std::string &q_id_hex);

  template <typename T>
  static std::string join(const T &v, const std::string &delimiter,
                          const std::string &prefix = "",
                          const std::string &suffix = "") {
    std::stringstream ss;
    size_t i = 0;
    ss << prefix;
    for (const auto &elem : v) {
      if (i != 0) {
        ss << delimiter;
      }
      ss << elem;
      i++;
    }
    ss << suffix;
    return ss.str();
  }

  template <class InputIterator>
  static std::string join(InputIterator first, InputIterator last,
                          const std::string &delim, const std::string &arround = "") {
    std::string a = arround;
    while (first != last) {
      a += std::to_string(*first);
      first++;
      if (first != last) a += delim;
    }
    a += arround;
    return a;
  }

  template <class InputIterator>
  static std::string join(InputIterator first, InputIterator last,
                          std::function<std::string(InputIterator)> func,
                          const std::string &delim, const std::string &arround = "") {
    std::string a = arround;
    while (first != last) {
      a += func(first);
      first++;
      if (first != last) a += delim;
    }
    a += arround;
    return a;
  }
};

class AutoSpinLock {
 public:
  explicit AutoSpinLock(std::atomic_flag &lock) : lock_(lock) {
    while (lock_.test_and_set(std::memory_order_acquire))
      ;
  }
  ~AutoSpinLock() { unlock(); }
  void unlock() { lock_.clear(std::memory_order_release); }

 private:
  std::atomic_flag &lock_;
};

inline void ConvertToValidQueueId(const ObjectID &queue_id) {
  auto addr = const_cast<ObjectID *>(&queue_id);
  *(reinterpret_cast<uint64_t *>(addr)) = 0;
}
}  // namespace streaming
}  // namespace ray
