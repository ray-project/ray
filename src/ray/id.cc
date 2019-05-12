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

plasma::UniqueID ObjectID::to_plasma_id() const {
  plasma::UniqueID result;
  std::memcpy(result.mutable_data(), this, kUniqueIDSize);
  return result;
}

ObjectID::ObjectID(const plasma::UniqueID &from) {
  std::memcpy(this, from.data(), kUniqueIDSize);
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

std::ostream &operator<<(std::ostream &os, const TaskID &id) {
  if (id.is_nil()) {
    os << "NIL_ID";
  } else {
    os << id.hex();
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, const ObjectID &id) {
  if (id.is_nil()) {
    os << "NIL_ID";
  } else {
    os << id.hex();
  }
  return os;
}

size_t ObjectID::hash() const {
  // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
  // default value 0)
  if (!hash_) {
    hash_ = MurmurHash64A(this, ObjectID::size(), 0);
  }
  return hash_;
}

size_t TaskID::hash() const {
  return MurmurHash64A(this, TaskID::size(), 0);
}

TaskID TaskID::GetDriverTaskID(const DriverID &driver_id) {
  std::string driver_id_str = driver_id.binary();
  driver_id_str.resize(size());
  return TaskID::from_binary(driver_id_str);
}

ObjectID ObjectID::build(const TaskID &task_id, bool is_put, int64_t index) {
  RAY_CHECK(index >= 1) << "index=" << index;
  ObjectID object_id;
  object_id.task_id_ = task_id;
  if (is_put) {
    RAY_CHECK(index <= kMaxTaskPuts) << "index=" << index;
    object_id.index_ = -index;
  } else {
    RAY_CHECK(index <= kMaxTaskReturns) << "index=" << index;
    object_id.index_ = index;
  }
  return object_id;
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
  return TaskID::from_binary(std::string(buff, buff + TaskID::size()));
}

}  // namespace ray
