// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/id.h"

#include <limits.h>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <random>
#include "absl/time/clock.h"

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
                                size_t parent_task_counter, size_t extra_bytes,
                                size_t length) {
  RAY_CHECK(length <= DIGEST_SIZE);
  SHA256_CTX ctx;
  sha256_init(&ctx);
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(job_id.Data()), job_id.Size());
  sha256_update(&ctx, reinterpret_cast<const BYTE *>(parent_task_id.Data()),
                parent_task_id.Size());
  sha256_update(&ctx, (const BYTE *)&parent_task_counter, sizeof(parent_task_counter));
  if (extra_bytes > 0) {
    sha256_update(&ctx, (const BYTE *)&extra_bytes, sizeof(extra_bytes));
  }

  BYTE buff[DIGEST_SIZE];
  sha256_final(&ctx, buff);
  return std::string(buff, buff + length);
}

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
  // NOTE(swang): Include the current time in the hash for the actor ID so that
  // we avoid duplicating a previous actor ID, which is not allowed by the GCS.
  // See https://github.com/ray-project/ray/issues/10481.
  auto data =
      GenerateUniqueBytes(job_id, parent_task_id, parent_task_counter,
                          absl::GetCurrentTimeNanos(), ActorID::kUniqueBytesLength);
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
  std::string data = GenerateUniqueBytes(job_id, parent_task_id, parent_task_counter, 0,
                                         TaskID::kUniqueBytesLength);
  std::copy_n(actor_id.Data(), ActorID::kLength, std::back_inserter(data));
  RAY_CHECK(data.size() == TaskID::kLength);
  return TaskID::FromBinary(data);
}

TaskID TaskID::ForNormalTask(const JobID &job_id, const TaskID &parent_task_id,
                             size_t parent_task_counter) {
  std::string data = GenerateUniqueBytes(job_id, parent_task_id, parent_task_counter, 0,
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
  return TaskID::FromBinary(
      std::string(reinterpret_cast<const char *>(id_), TaskID::Size()));
}

ObjectIDIndexType ObjectID::ObjectIndex() const {
  ObjectIDIndexType index;
  std::memcpy(&index, id_ + TaskID::kLength, sizeof(index));
  return index;
}

ObjectID ObjectID::FromIndex(const TaskID &task_id, ObjectIDIndexType index) {
  RAY_CHECK(index >= 1 && index <= kMaxObjectIndex) << "index=" << index;

  return GenerateObjectId(task_id.Binary(), index);
}

ObjectID ObjectID::FromRandom() {
  std::vector<uint8_t> task_id_bytes(TaskID::kLength, 0x0);
  FillRandom(&task_id_bytes);

  return GenerateObjectId(std::string(
      reinterpret_cast<const char *>(task_id_bytes.data()), task_id_bytes.size()));
}

ObjectID ObjectID::ForActorHandle(const ActorID &actor_id) {
  return ObjectID::FromIndex(TaskID::ForActorCreationTask(actor_id),
                             /*return_index=*/1);
}

ObjectID ObjectID::GenerateObjectId(const std::string &task_id_binary,
                                    ObjectIDIndexType object_index) {
  RAY_CHECK(task_id_binary.size() == TaskID::Size());
  ObjectID ret;
  std::memcpy(ret.id_, task_id_binary.c_str(), TaskID::kLength);
  std::memcpy(ret.id_ + TaskID::kLength, &object_index, sizeof(object_index));
  return ret;
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
ID_OSTREAM_OPERATOR(PlacementGroupID);

NodeID GenerateNodeIdFromIpAddress(const std::string &ip_address) {
  // check IP version 4 address.
  boost::system::error_code err;
  boost::asio::ip::address_v4 ip_v4 = boost::asio::ip::make_address_v4(ip_address, err);
  if (err) {
    RAY_LOG(WARNING) << "Inavlid IP address: " << ip_address
                     << ", error message: " << err.message()
                     << ". Falling back to a random NodeID.";
    return NodeID::FromRandom();
  }

  // Format the IP version 4 address to a 12-digits hex string, and add 1 for the
  // null terminator.
  char ip_buffer[13];
  int split_bit = 8;
  int mask = (1 << split_bit) - 1;
  unsigned int uint_ip_v4 = ip_v4.to_uint();
  sprintf(ip_buffer, "%03d%03d%03d%03d", (uint_ip_v4 >> 3 * split_bit) & mask,
          (uint_ip_v4 >> 2 * split_bit) & mask, (uint_ip_v4 >> 1 * split_bit) & mask,
          (uint_ip_v4 >> 0 * split_bit) & mask);

  // Convert hex-format `ip_buffer` to binary.
  std::string ip_prefix(6, 0);
  for (unsigned int index = 0; index < ip_prefix.size(); index++) {
    int number = ((ip_buffer[index * 2] - '0') << 4) + (ip_buffer[index * 2 + 1] - '0');
    ip_prefix[index] = static_cast<uint8_t>(number);
  }

  // Filling the remaining bits with random data.
  std::string random_suffix(NodeID::Size() - ip_prefix.size(), 0);
  FillRandom(&random_suffix);

  return NodeID::FromBinary(ip_prefix + random_suffix);
}

}  // namespace ray
