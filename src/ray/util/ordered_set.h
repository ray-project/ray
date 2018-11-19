#ifndef RAY_UTIL_ORDERED_SET_H
#define RAY_UTIL_ORDERED_SET_H

#include <list>
#include <unordered_map>

/// \class ordered_set
///
/// This container has properties of both a deque and a set. It is like a deque
/// in the sense that it maintains the insertion order and allows you to
/// push_back elements and pop_front elements. It is like a set in the sense
/// that it does not allow duplicate entries. Looking up and erasing elements is
/// quick.
template <typename T>
class ordered_set {
 public:
  using elements_type = std::list<T>;
  using positions_type = std::unordered_map<T, typename elements_type::iterator>;
  using iterator_type = typename positions_type::iterator;


  ordered_set() {}

  ordered_set(const ordered_set &other) = delete;

  ordered_set &operator=(const ordered_set &other) = delete;

  void push_back(const T &value) {
    RAY_CHECK(positions_.find(value) == positions_.end());
    auto list_iterator = elements_.insert(elements_.end(), value);
    positions_[value] = list_iterator;
  }

  size_t count(const T &k) const { return positions_.count(k); }

  void pop_front() {
    positions_.erase(elements_.front());
    elements_.pop_front();
  }

  const T &front() const { return elements_.front(); }

  size_t size() const noexcept { return positions_.size(); }

  size_t erase(const T &k) {
    auto it = positions_.find(k);
    RAY_CHECK(it != positions_.end());
    elements_.erase(it->second);
    return positions_.erase(k);
  }

  iterator_type erase(const iterator_type position) {
    elements_.erase(position->second);
    return positions_.erase(position);
  }

  // TODO(rkn): The iterator returned here is not ideal because you can't simply
  // call *it to get the value, you need to do *(it->second).
  iterator_type begin() noexcept { return positions_.begin(); }

  iterator_type end() noexcept { return positions_.end(); }

 private:
  elements_type elements_;
  positions_type positions_;
};

#endif  // RAY_UTIL_ORDERED_SET_H
