#include "ray/common/id.h"

#include <limits.h>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <random>

#include "ray/common/constants.h"
#include "ray/common/status.h"
#include "ray/util/util.h"

extern "C" {
#include "ray/thirdparty/sha256.h"
}

// Definitions for computing hash digests.
#define DIGEST_SIZE SHA256_BLOCK_SIZE

namespace ray {

uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

/// A helper function to generate the unique bytes by hash.
std::string GenerateUniqueBytes(const JobID &job_id, const TaskID &parent_task_id,
                                size_t parent_task_counter, size_t length) {
  RAY_CHECK(length <= DIGEST_SIZE);
  SHA256_CTX ctx;
  sha256_init(&ctx);
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(job_id.Data()), job_id.Size());
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(parent_task_id.Data()),
                parent_task_id.Size());
  sha256_update(&ctx, (const BYTE *)&parent_task_counter, sizeof(parent_task_counter));

  BYTE buff[DIGEST_SIZE];
  sha256_final(&ctx, buff);
  return std::string(buff, buff + length);
}

namespace {

/// The bit offset of the flag `CreatedByTask` in a flags bytes.
constexpr uint8_t kCreatedByTaskBitsOffset = 15;

/// The bit offset of the flag `ObjectType` in a flags bytes.
constexpr uint8_t kObjectTypeBitsOffset = 14;

/// The bit offset of the flag `TransportType` in a flags bytes.
constexpr uint8_t kTransportTypeBitsOffset = 11;

/// The mask that is used to mask the flag `CreatedByTask`.
constexpr ObjectIDFlagsType kCreatedByTaskFlagBitMask = 0x1 << kCreatedByTaskBitsOffset;

/// The mask that is used to mask a bit to indicates the type of this object.
/// So it can represent for 2 types.
constexpr ObjectIDFlagsType kObjectTypeFlagBitMask = 0x1 << kObjectTypeBitsOffset;

/// The mask that is used to mask 3 bits to indicate the type of transport.
constexpr ObjectIDFlagsType kTransportTypeFlagBitMask = 0x7 << kTransportTypeBitsOffset;

/// The implementations of helper functions.
inline void SetCreatedByTaskFlag(bool created_by_task, ObjectIDFlagsType *flags) {
  const ObjectIDFlagsType object_type_bits =
      static_cast<ObjectIDFlagsType>(created_by_task) << kCreatedByTaskBitsOffset;
  *flags = (*flags bitor object_type_bits);
}

inline void SetObjectTypeFlag(ObjectType object_type, ObjectIDFlagsType *flags) {
  const ObjectIDFlagsType object_type_bits = static_cast<ObjectIDFlagsType>(object_type)
                                             << kObjectTypeBitsOffset;
  *flags = (*flags bitor object_type_bits);
}

inline void SetTransportTypeFlag(uint8_t transport_type, ObjectIDFlagsType *flags) {
  const ObjectIDFlagsType transport_type_bits =
      static_cast<ObjectIDFlagsType>(transport_type) << kTransportTypeBitsOffset;
  *flags = (*flags bitor transport_type_bits);
}

inline bool CreatedByTask(ObjectIDFlagsType flags) {
  return ((flags bitand kCreatedByTaskFlagBitMask) >> kCreatedByTaskBitsOffset) != 0x0;
}

inline ObjectType GetObjectType(ObjectIDFlagsType flags) {
  const ObjectIDFlagsType object_type =
      (flags bitand kObjectTypeFlagBitMask) >> kObjectTypeBitsOffset;
  return static_cast<ObjectType>(object_type);
}

inline uint8_t GetTransportType(ObjectIDFlagsType flags) {
  const ObjectIDFlagsType transport_type =
      (flags bitand kTransportTypeFlagBitMask) >> kTransportTypeBitsOffset;
  return static_cast<uint8_t>(transport_type);
}

}  // namespace

template <typename T>
void FillNil(T *data) {
  RAY_CHECK(data != nullptr);
  for (size_t i = 0; i < data->size(); i++) {
    (*data)[i] = static_cast<uint8_t>(0xFF);
  }
}

WorkerID ComputeDriverIdFromJob(const JobID &job_id) {
  std::vector<uint8_t> data(WorkerID::Size(), 0);
  std::memcpy(data.data(), job_id.Data(), JobID::Size());
  std::fill_n(data.data() + JobID::Size(), WorkerID::Size() - JobID::Size(), 0xFF);
  return WorkerID::FromBinary(
      std::string(reinterpret_cast<const char *>(data.data()), data.size()));
}

ObjectID ObjectID::FromPlasmaIdBinary(const std::string &from) {
  RAY_CHECK(from.size() == kPlasmaIdSize);
  return ObjectID::FromBinary(from.substr(0, ObjectID::kLength));
}

plasma::UniqueID ObjectID::ToPlasmaId() const {
  static_assert(ObjectID::kLength <= kPlasmaIdSize,
                "Currently length of ObjectID must be shorter than plasma's.");

  plasma::UniqueID result;
  std::memcpy(result.mutable_data(), Data(), ObjectID::Size());
  std::fill_n(result.mutable_data() + ObjectID::Size(), kPlasmaIdSize - ObjectID::kLength,
              0xFF);
  return result;
}

ObjectID::ObjectID(const plasma::UniqueID &from) {
  RAY_CHECK(from.size() <= static_cast<int64_t>(ObjectID::Size())) << "Out of size.";
  std::memcpy(this->MutableData(), from.data(), ObjectID::Size());
}

ObjectIDFlagsType ObjectID::GetFlags() const {
  ObjectIDFlagsType flags;
  std::memcpy(&flags, id_ + TaskID::kLength, sizeof(flags));
  return flags;
}
bool ObjectID::CreatedByTask() const { return ::ray::CreatedByTask(this->GetFlags()); }

bool ObjectID::IsPutObject() const {
  return ::ray::GetObjectType(this->GetFlags()) == ObjectType::PUT_OBJECT;
}

bool ObjectID::IsReturnObject() const {
  return ::ray::GetObjectType(this->GetFlags()) == ObjectType::RETURN_OBJECT;
}

uint8_t ObjectID::GetTransportType() const {
  return ::ray::GetTransportType(this->GetFlags());
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

ActorID ActorID::Of(const JobID &job_id, const TaskID &parent_task_id,
                    const size_t parent_task_counter) {
  auto data = GenerateUniqueBytes(job_id, parent_task_id, parent_task_counter,
                                  ActorID::kUniqueBytesLength);
  std::copy_n(job_id.Data(), JobID::kLength, std::back_inserter(data));
  RAY_CHECK(data.size() == kLength);
  return ActorID::FromBinary(data);
}

ActorID ActorID::NilFromJob(const JobID &job_id) {
  std::string data(kUniqueBytesLength, 0);
  FillNil(&data);
  std::copy_n(job_id.Data(), JobID::kLength, std::back_inserter(data));
  RAY_CHECK(data.size() == kLength);
  return ActorID::FromBinary(data);
}

JobID ActorID::JobId() const {
  RAY_CHECK(!IsNil());
  return JobID::FromBinary(std::string(
      reinterpret_cast<const char *>(this->Data() + kUniqueBytesLength), JobID::kLength));
}

TaskID TaskID::ForDriverTask(const JobID &job_id) {
  std::string data(kUniqueBytesLength, 0);
  FillNil(&data);
  const auto dummy_actor_id = ActorID::NilFromJob(job_id);
  std::copy_n(dummy_actor_id.Data(), ActorID::kLength, std::back_inserter(data));
  RAY_CHECK(data.size() == TaskID::kLength);
  return TaskID::FromBinary(data);
}

TaskID TaskID::ForFakeTask() {
  std::string data(kLength, 0);
  FillRandom(&data);
  return TaskID::FromBinary(data);
}

TaskID TaskID::ForActorCreationTask(const ActorID &actor_id) {
  std::string data(kUniqueBytesLength, 0);
  FillNil(&data);
  std::copy_n(actor_id.Data(), ActorID::kLength, std::back_inserter(data));
  RAY_CHECK(data.size() == TaskID::kLength);
  return TaskID::FromBinary(data);
}

TaskID TaskID::ForActorTask(const JobID &job_id, const TaskID &parent_task_id,
                            size_t parent_task_counter, const ActorID &actor_id) {
  std::string data = GenerateUniqueBytes(job_id, parent_task_id, parent_task_counter,
                                         TaskID::kUniqueBytesLength);
  std::copy_n(actor_id.Data(), ActorID::kLength, std::back_inserter(data));
  RAY_CHECK(data.size() == TaskID::kLength);
  return TaskID::FromBinary(data);
}

TaskID TaskID::ForNormalTask(const JobID &job_id, const TaskID &parent_task_id,
                             size_t parent_task_counter) {
  std::string data = GenerateUniqueBytes(job_id, parent_task_id, parent_task_counter,
                                         TaskID::kUniqueBytesLength);
  const auto dummy_actor_id = ActorID::NilFromJob(job_id);
  std::copy_n(dummy_actor_id.Data(), ActorID::kLength, std::back_inserter(data));
  RAY_CHECK(data.size() == TaskID::kLength);
  return TaskID::FromBinary(data);
}

ActorID TaskID::ActorId() const {
  return ActorID::FromBinary(std::string(
      reinterpret_cast<const char *>(id_ + kUniqueBytesLength), ActorID::Size()));
}

JobID TaskID::JobId() const { return ActorId().JobId(); }

TaskID TaskID::ComputeDriverTaskId(const WorkerID &driver_id) {
  std::string driver_id_str = driver_id.Binary();
  driver_id_str.resize(Size());
  return TaskID::FromBinary(driver_id_str);
}

TaskID ObjectID::TaskId() const {
  if (!CreatedByTask()) {
    // TODO(qwang): Should be RAY_CHECK here.
    RAY_LOG(WARNING) << "Shouldn't call this on a non-task object id: " << this->Hex();
  }
  return TaskID::FromBinary(
      std::string(reinterpret_cast<const char *>(id_), TaskID::Size()));
}

ObjectID ObjectID::ForPut(const TaskID &task_id, ObjectIDIndexType put_index,
                          uint8_t transport_type) {
  RAY_CHECK(put_index >= 1 && put_index <= kMaxObjectIndex) << "index=" << put_index;

  ObjectIDFlagsType flags = 0x0000;
  SetCreatedByTaskFlag(true, &flags);
  SetObjectTypeFlag(ObjectType::PUT_OBJECT, &flags);

  SetTransportTypeFlag(transport_type, &flags);

  return GenerateObjectId(task_id.Binary(), flags, put_index);
}

ObjectIDIndexType ObjectID::ObjectIndex() const {
  ObjectIDIndexType index;
  std::memcpy(&index, id_ + TaskID::kLength + kFlagsBytesLength, sizeof(index));
  return index;
}

ObjectID ObjectID::ForTaskReturn(const TaskID &task_id, ObjectIDIndexType return_index,
                                 uint8_t transport_type) {
  RAY_CHECK(return_index >= 1 && return_index <= kMaxObjectIndex)
      << "index=" << return_index;

  ObjectIDFlagsType flags = 0x0000;
  SetCreatedByTaskFlag(true, &flags);
  SetObjectTypeFlag(ObjectType::RETURN_OBJECT, &flags);
  SetTransportTypeFlag(transport_type, &flags);

  return GenerateObjectId(task_id.Binary(), flags, return_index);
}

ObjectID ObjectID::FromRandom() {
  ObjectIDFlagsType flags = 0x0000;
  SetCreatedByTaskFlag(false, &flags);
  // No need to set transport type for a random object id.
  // No need to assign put_index/return_index bytes.
  std::vector<uint8_t> task_id_bytes(TaskID::kLength, 0x0);
  FillRandom(&task_id_bytes);

  return GenerateObjectId(
      std::string(reinterpret_cast<const char *>(task_id_bytes.data()),
                  task_id_bytes.size()),
      flags);
}

ObjectID ObjectID::GenerateObjectId(const std::string &task_id_binary,
                                    ObjectIDFlagsType flags,
                                    ObjectIDIndexType object_index) {
  RAY_CHECK(task_id_binary.size() == TaskID::Size());
  ObjectID ret = ObjectID::Nil();
  std::memcpy(ret.id_, task_id_binary.c_str(), TaskID::kLength);
  std::memcpy(ret.id_ + TaskID::kLength, &flags, sizeof(flags));
  std::memcpy(ret.id_ + TaskID::kLength + kFlagsBytesLength, &object_index,
              sizeof(object_index));
  return ret;
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
ID_OSTREAM_OPERATOR(ActorID);
ID_OSTREAM_OPERATOR(TaskID);
ID_OSTREAM_OPERATOR(ObjectID);

}  // namespace ray
