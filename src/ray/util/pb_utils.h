#pragma once

#include <google/protobuf/map.h>
#include <google/protobuf/util/message_differencer.h>

template <typename T, typename = void>
struct has_equal_operator : std::false_type {};

template <typename T>
struct has_equal_operator<T, decltype((void)(std::declval<T>() == std::declval<T>()), void())> : std::true_type {};

template <class K, class V>
bool MapEqual(const ::google::protobuf::Map<K, V> &lhs,
              const ::google::protobuf::Map<K, V> &rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  for (const auto &pair : lhs) {
    auto it = rhs.find(pair.first);
    if (it == rhs.end()) {
        return false;
    }
    
    if constexpr (has_equal_operator<V>::value) {
        if (it->second != pair.second) {
            return false;
        }
    } else {
        if (!google::protobuf::util::MessageDifferencer::Equals(it->second, pair.second)) {
            return false;
        }
    }
  }

  return true;
}
