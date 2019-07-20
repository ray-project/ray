#include "ray/common/id.h"

#include <limits.h>

#include <chrono>
#include <mutex>
#include <random>

#include "ray/common/constants.h"
#include "ray/common/status.h"

extern "C" {
#include "ray/thirdparty/sha256.h"
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

WorkerID ComputeDriverIdFromJob(const JobID &job_id) {
  std::vector<uint8_t> data(WorkerID::Size(), 0);
  std::memcpy(data.data(), job_id.Data(), JobID::Size());
  std::fill_n(data.data() + JobID::Size(), WorkerID::Size() - JobID::Size(), 0xFF);
  return WorkerID::FromBinary(
      std::string(reinterpret_cast<const char *>(data.data()), data.size()));
}

plasma::UniqueID ObjectID::ToPlasmaId() const {
  plasma::UniqueID result;
  std::memcpy(result.mutable_data(), Data(), kUniqueIDSize);
  return result;
}

ObjectID::ObjectID(const plasma::UniqueID &from) {
  std::memcpy(this->MutableData(), from.data(), kUniqueIDSize);
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

TaskID TaskID::ComputeDriverTaskId(const WorkerID &driver_id) {
  std::string driver_id_str = driver_id.Binary();
  driver_id_str.resize(Size());
  return TaskID::FromBinary(driver_id_str);
}

TaskID ObjectID::TaskId() const {
  return TaskID::FromBinary(
      std::string(reinterpret_cast<const char *>(id_), TaskID::Size()));
}

ObjectID ObjectID::ForPut(const TaskID &task_id, int64_t put_index) {
  RAY_CHECK(put_index >= 1 && put_index <= kMaxTaskPuts) << "index=" << put_index;
  ObjectID object_id;
  std::memcpy(object_id.id_, task_id.Binary().c_str(), task_id.Size());
  object_id.index_ = -put_index;
  return object_id;
}

ObjectID ObjectID::ForTaskReturn(const TaskID &task_id, int64_t return_index) {
  RAY_CHECK(return_index >= 1 && return_index <= kMaxTaskReturns)
      << "index=" << return_index;
  ObjectID object_id;
  std::memcpy(object_id.id_, task_id.Binary().c_str(), task_id.Size());
  object_id.index_ = return_index;
  return object_id;
}

const TaskID GenerateTaskId(const JobID &job_id, const TaskID &parent_task_id,
                            int parent_task_counter) {
  // Compute hashes.
  SHA256_CTX ctx;
  sha256_init(&ctx);
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(job_id.Data()), job_id.Size());
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(parent_task_id.Data()),
                parent_task_id.Size());
  sha256_update(&ctx, (const BYTE *)&parent_task_counter, sizeof(parent_task_counter));

  // Compute the final task ID from the hash.
  BYTE buff[DIGEST_SIZE];
  sha256_final(&ctx, buff);
  return TaskID::FromBinary(std::string(buff, buff + TaskID::Size()));
}

const ActorHandleID ComputeNextActorHandleId(const ActorHandleID &actor_handle_id,
                                             int64_t num_forks) {
  // Compute hashes.
  SHA256_CTX ctx;
  sha256_init(&ctx);
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(actor_handle_id.Data()),
                actor_handle_id.Size());
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(&num_forks), sizeof(num_forks));

  // Compute the final actor handle ID from the hash.
  BYTE buff[DIGEST_SIZE];
  sha256_final(&ctx, buff);
  RAY_CHECK(DIGEST_SIZE >= ActorHandleID::Size());
  return ActorHandleID::FromBinary(std::string(buff, buff + ActorHandleID::Size()));
}

JobID JobID::FromInt(uint32_t value) {
  std::vector<uint8_t> data(JobID::Size(), 0);
  std::memcpy(data.data(), &value, JobID::Size());
  return JobID::FromBinary(
      std::string(reinterpret_cast<const char *>(data.data()), data.size()));
}

#define ID_OSTREAM_OPERATOR(id_type)                              \
  std::ostream &operator<<(std::ostream &os, const id_type &id) { \
    if (id.IsNil()) {                                             \
      os << "NIL_ID";                                             \
    } else {                                                      \
      os << id.Hex();                                             \
    }                                                             \
    return os;                                                    \
  }

ID_OSTREAM_OPERATOR(UniqueID);
ID_OSTREAM_OPERATOR(JobID);
ID_OSTREAM_OPERATOR(TaskID);
ID_OSTREAM_OPERATOR(ObjectID);

}  // namespace ray
