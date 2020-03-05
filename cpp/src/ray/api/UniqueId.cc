
#include <ray/api/uniqueId.h>
#include <random>

namespace ray {

UniqueId nilUniqueId;

const uint8_t* UniqueId::data() const { return _id; }

uint8_t* UniqueId::mutable_data() { return _id; }

std::string UniqueId::hex() const {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (int i = 0; i < plasma::kUniqueIDSize; i++) {
    unsigned int val = _id[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

bool UniqueId::operator==(const UniqueId& rhs) const {
  return std::memcmp(data(), rhs.data(), plasma::kUniqueIDSize) == 0;
}

void UniqueId::random() {
  uint8_t *data = mutable_data();
  std::random_device engine;
  for (int i = 0; i < plasma::kUniqueIDSize; i++) {
    data[i] = static_cast<uint8_t>(engine());
  }
}

std::unique_ptr<UniqueId> UniqueId::copy() const {
  std::unique_ptr<UniqueId> uidPtr(new UniqueId());
  memcpy((void *)(*uidPtr).mutable_data(), (const void *)data(), plasma::kUniqueIDSize);
  return uidPtr;
}

std::unique_ptr<UniqueId> UniqueId::taskComputePutId(uint64_t l) const {
  auto uidPtr = copy();
  uint64_t x;
  memcpy((void *)&x, (const void *)uidPtr->data(), sizeof(uint64_t));
  x = x ^ (l + 1);
  memcpy((void *)uidPtr->data(), (const void *)&x, sizeof(uint64_t));
  return uidPtr;
}

std::unique_ptr<UniqueId> UniqueId::taskComputeReturnId(uint64_t l) const {
  auto uidPtr = copy();
  uint64_t x;
  memcpy((void *)&x, (const void *)uidPtr->data(), sizeof(uint64_t));
  x = x ^ (-l - 1);
  memcpy((void *)uidPtr->data(), (const void *)&x, sizeof(uint64_t));
  return uidPtr;
}

}  // namespace ray