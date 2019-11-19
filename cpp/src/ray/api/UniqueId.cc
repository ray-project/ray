
#include <ray/api/uniqueId.h>
#include <random>

namespace ray {

UniqueId nilUniqueId;

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