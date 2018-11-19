#ifndef RAY_UTIL_ORDERED_SET_H
#define RAY_UTIL_ORDERED_SET_H

#include <list>
#include <unordered_map>

template <typename T>
class ordered_set {
 public:
  explicit ordered_set() {}

  void push_back(const T &value) {
    RAY_CHECK(iterators_.find(value) == iterators_.end());
    auto list_iterator = elements_.insert(elements_.end(), value);
    iterators_[value] = list_iterator;
  }

  size_t count(const T &k) const { return iterators_.count(k); }

  void pop_front() {
    iterators_.erase(elements_.front());
    elements_.pop_front();
  }

  const T &front() const { return elements_.front(); }

  size_t size() const noexcept {
    RAY_CHECK(elements_.size() == iterators_.size());
    return elements_.size();
  }

  size_t erase(const T &k) {
    auto it = iterators_.find(k);
    RAY_CHECK(it != iterators_.end());
    elements_.erase(it->second);
    return iterators_.erase(it);
  }

 private:
  std::list<T> elements_;
  std::unordered_map<T, typename std::list<T>::iterator> iterators_;
};

#endif  // RAY_UTIL_ORDERED_SET_H
