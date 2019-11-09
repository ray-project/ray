
#pragma once

#include <ray/core.h>
#include <memory>

namespace ray {

class UniqueId : public plasma::UniqueID {
  using UniqueID::UniqueID;

 public:
  void random();
  std::unique_ptr<UniqueId> taskComputePutId(uint64_t l) const;
  std::unique_ptr<UniqueId> taskComputeReturnId(uint64_t l) const;
  std::unique_ptr<UniqueId> copy() const;
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
}
