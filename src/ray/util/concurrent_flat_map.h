

#include "absl/container/flat_hash_map.h"
#include "ray/util/mutex_protected.h"

namespace ray {

// A flat map that's protected by a single mutex with read/write locks.
//
// Use when expecting one of the following:
// mininal thread contention
// trivially copyable values
// majority reads
// If these don't describe your use case, prefer boost::concurrent_flat_map
template <typename KeyType, typename ValueType>
class ConcurrentFlatMap {
 public:
  template <typename... Args>
  explicit ConcurrentFlatMap(Args &&...args) : map_(std::forward(args)...) {}

  ConcurrentFlatMap() = default;

  void Reserve(size_t size) {
    auto write_lock = map_.LockForWrite();
    write_lock.Get().reserve(size);
  }

  template <typename KeyLike>
  std::optional<ValueType> Get(const KeyLike &key) const {
    std::optional<ValueType> result;
    auto read_lock = map_.LockForRead();
    const auto &map = read_lock.Get();
    const auto it = map.find(key);
    if (it != map.end()) {
      result.emplace(it->second);
    }
    return result;
  }

  // Returns true if key existed and visitor was called.
  // Write-only overload.
  template <typename KeyLike>
  bool WriteVisit(const KeyLike &key, const std::function<void(ValueType &)> &visitor) {
    auto write_lock = map_.LockForWrite();
    auto &map = write_lock.Get();
    const auto it = map.find(key);
    if (it != map.end()) {
      visitor(it->second);
      return true;
    }
    return false;
  }

  // Returns true if key existed and visitor was called.
  template <typename KeyLike>
  bool ReadVisit(const KeyLike &key,
                 const std::function<void(const ValueType &)> &visitor) const {
    auto read_lock = map_.LockForRead();
    const auto &map = read_lock.Get();
    auto it = map.find(key);
    if (it != map.end()) {
      visitor(it->second);
      return true;
    }
    return false;
  }

  // Applies visitor to all values in the map.
  void ReadVisitAll(
      const std::function<void(const KeyType &, const ValueType &)> &visitor) const {
    auto read_lock = map_.LockForRead();
    const auto &map = read_lock.Get();
    for (const auto &[key, value] : map) {
      visitor(key, value);
    }
  }

  template <typename KeyLike>
  bool Contains(const KeyLike &key) const {
    auto read_lock = map_.LockForRead();
    return read_lock.Get().contains(key);
  }

  template <typename KeyLike, typename... Args>
  void Insert(KeyLike &&keyArg, Args &&...args) {
    auto write_lock = map_.LockForWrite();
    // Note: not using emplace as it won't replace value if key already exists.
    write_lock.Get().insert_or_assign(std::forward<KeyLike>(keyArg),
                                      ValueType(std::forward<Args>(args)...));
  }

  bool Erase(const KeyType &key) {
    auto write_lock = map_.LockForWrite();
    return write_lock.Get().erase(key) > 0;
  }

  int64_t EraseKeys(const std::vector<KeyType> &keys) {
    auto write_lock = map_.LockForWrite();
    int64_t num_erased = 0;
    for (const auto &key : keys) {
      num_erased += write_lock.Get().erase(key);
    }
    return num_erased;
  }

  absl::flat_hash_map<KeyType, ValueType> GetMapCopy() const {
    auto read_lock = map_.LockForRead();
    return read_lock.Get();
  }

 private:
  MutexProtected<absl::flat_hash_map<KeyType, ValueType>> map_;
};

}  // namespace ray
