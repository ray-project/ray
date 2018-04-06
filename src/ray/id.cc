#include "ray/id.h"

#include <random>

#include "ray/constants.h"
#include "ray/status.h"

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

const ObjectID ComputeObjectId(TaskID task_id, int64_t object_index) {
  RAY_CHECK(object_index <= kMaxTaskReturns && object_index >= -kMaxTaskPuts);
  ObjectID return_id = task_id;
  int64_t *first_bytes = reinterpret_cast<int64_t *>(&return_id);
  // Zero out the lowest kObjectIdIndexSize bits of the first byte of the
  // object ID.
  uint64_t bitmask = static_cast<uint64_t>(-1) << kObjectIdIndexSize;
  *first_bytes = *first_bytes & (bitmask);
  // OR the first byte of the object ID with the return index.
  *first_bytes = *first_bytes | (object_index & ~bitmask);
  return return_id;
}

const TaskID FinishTaskId(const TaskID &task_id) { return ComputeObjectId(task_id, 0); }

const ObjectID ComputeReturnId(TaskID task_id, int64_t return_index) {
  RAY_CHECK(return_index >= 1 && return_index <= kMaxTaskReturns);
  return ComputeObjectId(task_id, return_index);
}

const ObjectID ComputePutId(TaskID task_id, int64_t put_index) {
  RAY_CHECK(put_index >= 1 && put_index <= kMaxTaskPuts);
  return ComputeObjectId(task_id, -1 * put_index);
}

const TaskID ComputeTaskId(const ObjectID &object_id) {
  TaskID task_id = object_id;
  int64_t *first_bytes = reinterpret_cast<int64_t *>(&task_id);
  // Zero out the lowest kObjectIdIndexSize bits of the first byte of the
  // object ID.
  uint64_t bitmask = static_cast<uint64_t>(-1) << kObjectIdIndexSize;
  *first_bytes = *first_bytes & (bitmask);
  return task_id;
}

int64_t ComputeObjectIndex(const ObjectID &object_id) {
  const int64_t *first_bytes = reinterpret_cast<const int64_t *>(&object_id);
  uint64_t bitmask = static_cast<uint64_t>(-1) << kObjectIdIndexSize;
  int64_t index = *first_bytes & (~bitmask);
  index <<= (8 * sizeof(int64_t) - kObjectIdIndexSize);
  index >>= (8 * sizeof(int64_t) - kObjectIdIndexSize);
  return index;
}

}  // namespace ray
