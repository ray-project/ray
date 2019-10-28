#ifndef RAY_STREAMING_UTILITY_H
#define RAY_STREAMING_UTILITY_H
#include <string>

#include "streaming.h"

namespace ray {
namespace streaming {

typedef std::map<std::string, std::string> TagMap;

class StreamingUtility {
 public:
  static std::string Byte2hex(const uint8_t *data, uint32_t data_size);

  static std::string Hexqid2str(const std::string &q_id_hex);

  static std::string Qid2EdgeInfo(const ray::ObjectID &q_id);

  static std::string GetHostname();

  static void Split(const ray::ObjectID &q_id, std::vector<std::string> &q_splited_vec);

  static void FindTagsFromQueueName(const ray::ObjectID &q_id, TagMap &tags,
                                    bool is_reader = false);

  // Return all items in a vector, not in b vector which means the returned
  // result is set A minus set B ( |A| - |B| ).
  static std::vector<ObjectID> SetDifference(const std::vector<ObjectID> &ids_a,
                                             const std::vector<ObjectID> &ids_b);

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
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_UTILITY_H
