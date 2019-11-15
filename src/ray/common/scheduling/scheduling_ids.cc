#include "scheduling_ids.h"
using namespace std;

int64_t StringIdMap::get(const string sid) {
  auto it = string_to_int_.find(sid);
  if (it == string_to_int_.end()) {
    return -1;
  } else {
    return it->second;
  }
};

int64_t StringIdMap::insert(const string sid, bool test) {
  auto sit = string_to_int_.find(sid);
  if (sit == string_to_int_.end()) {
    int64_t id = test ? hasher_(sid) % 10 : hasher_(sid);
    for (int i = 0; true; i++) {
      auto it = int_to_string_.find(id);
      if (it == int_to_string_.end()) {
        /// No hash collision, so associated sid with id.
        string_to_int_.insert(make_pair(sid, id));
        int_to_string_.insert(make_pair(id, sid));
        break;
      }
      id = test ? hasher_(sid + to_string(i)) % 10 : hasher_(sid + to_string(i));
    }
    return id;
  } else {
    return sit->second;
  }
};

void StringIdMap::remove(const string sid) {
  auto sit = string_to_int_.find(sid);
  if (sit != string_to_int_.end()) {
    uint64_t id = string_to_int_[sid];
    string_to_int_.erase(sit);
    auto it = int_to_string_.find(id);
    int_to_string_.erase(it);
  }
};

void StringIdMap::remove(int64_t id) {
  auto it = int_to_string_.find(id);
  if (it != int_to_string_.end()) {
    string sid = int_to_string_[id];
    int_to_string_.erase(it);
    auto sit = string_to_int_.find(sid);
    string_to_int_.erase(sit);
  }
};

int64_t StringIdMap::count() { return string_to_int_.size(); }
