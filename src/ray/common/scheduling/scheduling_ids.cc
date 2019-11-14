#include "scheduling_ids.h"
using namespace std;

int64_t ScheduleIds::getIdByInt(string sid) {
  if (string_to_int.count(sid) == 0) return 0;
  return string_to_int[sid];
};

int64_t ScheduleIds::insertIdByString(string sid, bool test) {
  auto sit = string_to_int.find(sid);
  if (sit == string_to_int.end()) {
    int64_t id = test ? hasher(sid) % 5 : hasher(sid);
    for (int i = 0; true; i++) {
      auto it = int_to_string.find(id);
      if (it == int_to_string.end()) {
        /// No hash collision, so associated sid with id.
        string_to_int.insert(make_pair(sid, id));
        int_to_string.insert(make_pair(id, sid));
        break;
      }
      id = test ? hasher(sid + to_string(i)) % 5 : hasher(sid + to_string(i));
    }
    return id;
  } else {
    return sit->second;
  }
};

void ScheduleIds::removeIdByString(string sid) {
  auto sit = string_to_int.find(sid);
  if (sit != string_to_int.end()) {
    uint64_t id = string_to_int[sid];
    string_to_int.erase(sit);
    auto it = int_to_string.find(id);
    int_to_string.erase(it);
  }
};

void ScheduleIds::removeIdByInt(int64_t id) {
  auto it = int_to_string.find(id);
  if (it != int_to_string.end()) {
    string sid = int_to_string[id];
    int_to_string.erase(it);
    auto sit = string_to_int.find(sid);
    string_to_int.erase(sit);
  }
};

int64_t ScheduleIds::count() {
  return string_to_int.size();
}
