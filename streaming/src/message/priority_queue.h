#pragma once

#include <algorithm>
#include <memory>
#include <vector>
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

template <class T, class C>

class PriorityQueue {
 private:
  std::vector<T> merge_vec_;
  C comparator_;

 public:
  PriorityQueue(C &comparator) : comparator_(comparator){};

  inline void push(T &&item) {
    merge_vec_.push_back(std::forward<T>(item));
    std::push_heap(merge_vec_.begin(), merge_vec_.end(), comparator_);
  }

  inline void push(const T &item) {
    merge_vec_.push_back(item);
    std::push_heap(merge_vec_.begin(), merge_vec_.end(), comparator_);
  }

  inline void pop() {
    STREAMING_CHECK(!isEmpty());
    std::pop_heap(merge_vec_.begin(), merge_vec_.end(), comparator_);
    merge_vec_.pop_back();
  }

  inline void makeHeap() {
    std::make_heap(merge_vec_.begin(), merge_vec_.end(), comparator_);
  }

  inline T &top() { return merge_vec_.front(); }

  inline uint32_t size() { return merge_vec_.size(); }

  inline bool isEmpty() { return merge_vec_.empty(); }

  std::vector<T> &getRawVector() { return merge_vec_; }
};
}  // namespace streaming
}  // namespace ray
