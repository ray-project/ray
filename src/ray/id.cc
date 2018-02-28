#include "ray/id.h"

#include <random>

namespace ray {

UniqueID::UniqueID(const plasma::UniqueID &from) {
  std::memcpy(&id_, from.data(), kUniqueIDSize);
}

UniqueID UniqueID::from_random() {
  UniqueID id;
  uint8_t *data = id.mutable_data();
  std::random_device engine;
  for (int i = 0; i < kUniqueIDSize; i++) {
    data[i] = static_cast<uint8_t>(engine());
  }
  return id;
}

UniqueID UniqueID::from_binary(const std::string &binary) {
  UniqueID id;
  std::memcpy(&id, binary.data(), sizeof(id));
  return id;
}

const UniqueID UniqueID::nil() {
  UniqueID result;
  uint8_t *data = result.mutable_data();
  std::fill_n(data, kUniqueIDSize, 255);
  return result;
}

bool UniqueID::is_nil() const {
  const uint8_t *d = data();
  for (int i = 0; i < kUniqueIDSize; ++i) {
    if (d[i] != 255) {
      return false;
    }
  }
  return true;
}

const uint8_t *UniqueID::data() const {
  return id_;
}

uint8_t *UniqueID::mutable_data() {
  return id_;
}

size_t UniqueID::size() const {
  return kUniqueIDSize;
}

std::string UniqueID::binary() const {
  return std::string(reinterpret_cast<const char *>(id_), kUniqueIDSize);
}

std::string UniqueID::hex() const {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (int i = 0; i < kUniqueIDSize; i++) {
    unsigned int val = id_[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

plasma::UniqueID UniqueID::to_plasma_id() {
  plasma::UniqueID result;
  std::memcpy(result.mutable_data(), &id_, kUniqueIDSize);
  return result;
}

bool UniqueID::operator==(const UniqueID &rhs) const {
  return std::memcmp(data(), rhs.data(), kUniqueIDSize) == 0;
}

std::ostream &operator<<(std::ostream &os, const UniqueID &id) {
  os << id.hex();
  return os;
}

}  // namespace ray
