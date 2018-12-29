#include "tags.h"

#include <numeric>
#include <unordered_map>
#include <string>
#include <set>

namespace ray {

namespace metrics {

TagKeys::TagKeys(const std::set<std::string> &keys)
    : keys_(keys) {
  DoHash();
}

void TagKeys::DoHash() {
  size_t final_code = 1;
  for (const auto &key : keys_) {
    size_t key_code = std::hash<std::string>{} (key);
    final_code = 31 * final_code + key_code;
  }

  id_ = final_code;
}

Tags::Tags(const std::map<std::string, std::string> &tag_map)
    : tag_map_(tag_map) {
  DoHash();

  std::set<std::string> keys;
  for (const auto &tag : tag_map_) {
    keys.insert(tag.first);
  }

  keys_ = TagKeys(keys);
}

//TODO(qwang): Should we implement this with SHA like UniqueID?
void Tags::DoHash() {
  size_t final_code = 1;
  for (const auto &tag : tag_map_) {
    size_t key_code = std::hash<std::string>{}(tag.first);
    final_code = 31 * final_code + key_code;

    size_t value_code = std::hash<std::string>{}(tag.second);
    final_code = 31 * final_code + value_code;
  }

  id_ = final_code;
}

}  // namespace metrics

}  // namespace ray
