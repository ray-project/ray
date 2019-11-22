#include "scheduling_ids.h"

int64_t StringIdMap::Get(const std::string &string_id) {
  auto it = string_to_int_.find(string_id);
  if (it == string_to_int_.end()) {
    return -1;
  } else {
    return it->second;
  }
};

int64_t StringIdMap::Insert(const std::string &string_id, bool test) {
  auto sit = string_to_int_.find(string_id);
  if (sit == string_to_int_.end()) {
    int64_t id = hasher_(string_id);
    if (test) {
      id = id % MAX_ID_TEST;
    }
    for (int i = 0; true; i++) {
      auto it = int_to_string_.find(id);
      if (it == int_to_string_.end()) {
        /// No hash collision, so associate string_id with id.
        string_to_int_.emplace(string_id, id);
        int_to_string_.emplace(id, string_id);
        break;
      }
      id = hasher_(string_id + std::to_string(i));
      if (test) {
        id = id % MAX_ID_TEST;
      }
    }
    return id;
  } else {
    return sit->second;
  }
};

void StringIdMap::Remove(const std::string &string_id) {
  auto sit = string_to_int_.find(string_id);
  if (sit != string_to_int_.end()) {
    uint64_t id = string_to_int_[string_id];
    string_to_int_.erase(sit);
    auto it = int_to_string_.find(id);
    int_to_string_.erase(it);
  }
};

void StringIdMap::Remove(int64_t id) {
  auto it = int_to_string_.find(id);
  if (it != int_to_string_.end()) {
    std::string string_id = int_to_string_[id];
    int_to_string_.erase(it);
    auto sit = string_to_int_.find(string_id);
    string_to_int_.erase(sit);
  }
};

int64_t StringIdMap::Count() { return string_to_int_.size(); }
