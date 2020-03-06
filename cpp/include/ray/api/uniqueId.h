
#pragma once

#include <ray/core.h>
#include <memory>
#include <msgpack.hpp>

namespace ray {

class UniqueId {
 public:
  bool operator==(const UniqueId &rhs) const;
  const uint8_t *data() const;
  uint8_t *mutable_data();
  std::string hex() const;
  void random();
  std::unique_ptr<UniqueId> taskComputePutId(uint64_t l) const;
  std::unique_ptr<UniqueId> taskComputeReturnId(uint64_t l) const;
  std::unique_ptr<UniqueId> copy() const;
  MSGPACK_DEFINE(_id);

 private:
  uint8_t _id[plasma::kUniqueIDSize];
};

extern UniqueId nilUniqueId;

}  // namespace ray

namespace std {
template <>
struct hash< ::ray::UniqueId> {
  size_t operator()(const ::ray::UniqueId &id) const {
    return *(reinterpret_cast<const size_t *>(id.data()));
  }
};
}  // namespace std
