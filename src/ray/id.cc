#include "ray/id.h"

#include <limits.h>

#include <chrono>
#include <mutex>
#include <random>

#include "ray/constants.h"
#include "ray/status.h"

extern "C" {
#include "thirdparty/sha256.h"
}

// Definitions for computing hash digests.
#define DIGEST_SIZE SHA256_BLOCK_SIZE

namespace ray {

std::mt19937 RandomlySeededMersenneTwister() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 seeded_engine(seed);
  return seeded_engine;
}

uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

plasma::UniqueID UniqueID::to_plasma_id() const {
  plasma::UniqueID result;
  std::memcpy(result.mutable_data(), &id_, kUniqueIDSize);
  return result;
}

UniqueID::UniqueID(const plasma::UniqueID &from) {
  std::memcpy(&id_, from.data(), kUniqueIDSize);
}

// This code is from https://sites.google.com/site/murmurhash/
// and is public domain.
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t *data = reinterpret_cast<const uint64_t *>(key);
  const uint64_t *end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char *data2 = reinterpret_cast<const unsigned char *>(data);

  switch (len & 7) {
  case 7:
    h ^= uint64_t(data2[6]) << 48;
  case 6:
    h ^= uint64_t(data2[5]) << 40;
  case 5:
    h ^= uint64_t(data2[4]) << 32;
  case 4:
    h ^= uint64_t(data2[3]) << 24;
  case 3:
    h ^= uint64_t(data2[2]) << 16;
  case 2:
    h ^= uint64_t(data2[1]) << 8;
  case 1:
    h ^= uint64_t(data2[0]);
    h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

size_t UniqueID::hash() const {
  // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
  // default value 0)
  if (!hash_) {
    hash_ = MurmurHash64A(&id_[0], kUniqueIDSize, 0);
  }
  return hash_;
}

std::ostream &operator<<(std::ostream &os, const UniqueID &id) {
  if (id.is_nil()) {
    os << "NIL_ID";
  } else {
    os << id.hex();
  }
  return os;
}

const ObjectID ComputeObjectId(const TaskID &task_id, int64_t object_index) {
  RAY_CHECK(object_index <= kMaxTaskReturns && object_index >= -kMaxTaskPuts);
  ObjectID return_id = ObjectID(task_id);
  int64_t *first_bytes = reinterpret_cast<int64_t *>(&return_id);
  // Zero out the lowest kObjectIdIndexSize bits of the first byte of the
  // object ID.
  uint64_t bitmask = static_cast<uint64_t>(-1) << kObjectIdIndexSize;
  *first_bytes = *first_bytes & (bitmask);
  // OR the first byte of the object ID with the return index.
  *first_bytes = *first_bytes | (object_index & ~bitmask);
  return return_id;
}

const TaskID FinishTaskId(const TaskID &task_id) {
  return TaskID(ComputeObjectId(task_id, 0));
}

const ObjectID ComputeReturnId(const TaskID &task_id, int64_t return_index) {
  RAY_CHECK(return_index >= 1 && return_index <= kMaxTaskReturns);
  return ComputeObjectId(task_id, return_index);
}

const ObjectID ComputePutId(const TaskID &task_id, int64_t put_index) {
  RAY_CHECK(put_index >= 1 && put_index <= kMaxTaskPuts);
  // We multiply put_index by -1 to distinguish from return_index.
  return ComputeObjectId(task_id, -1 * put_index);
}

const TaskID ComputeTaskId(const ObjectID &object_id) {
  TaskID task_id = TaskID(object_id);
  int64_t *first_bytes = reinterpret_cast<int64_t *>(&task_id);
  // Zero out the lowest kObjectIdIndexSize bits of the first byte of the
  // object ID.
  uint64_t bitmask = static_cast<uint64_t>(-1) << kObjectIdIndexSize;
  *first_bytes = *first_bytes & (bitmask);
  return task_id;
}

const TaskID GenerateTaskId(const DriverID &driver_id, const TaskID &parent_task_id,
                            int parent_task_counter) {
  // Compute hashes.
  SHA256_CTX ctx;
  sha256_init(&ctx);
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(driver_id.data()), driver_id.size());
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(parent_task_id.data()),
                parent_task_id.size());
  sha256_update(&ctx, (const BYTE *)&parent_task_counter, sizeof(parent_task_counter));

  // Compute the final task ID from the hash.
  BYTE buff[DIGEST_SIZE];
  sha256_final(&ctx, buff);
  return FinishTaskId(TaskID::from_binary(std::string(buff, buff + kUniqueIDSize)));
}

int64_t ComputeObjectIndex(const ObjectID &object_id) {
  const int64_t *first_bytes = reinterpret_cast<const int64_t *>(&object_id);
  uint64_t bitmask = static_cast<uint64_t>(-1) << kObjectIdIndexSize;
  int64_t index = *first_bytes & (~bitmask);
  index <<= (8 * sizeof(int64_t) - kObjectIdIndexSize);
  index >>= (8 * sizeof(int64_t) - kObjectIdIndexSize);
  return index;
}

template class BaseId<UniqueID>;

}  // namespace ray
