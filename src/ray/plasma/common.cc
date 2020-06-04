// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "plasma/common.h"

#include <limits>
#include <utility>

#include "arrow/util/ubsan.h"

#include "plasma/plasma_generated.h"

namespace fb = plasma::flatbuf;

namespace plasma {

namespace {

const char kErrorDetailTypeId[] = "plasma::PlasmaStatusDetail";

class PlasmaStatusDetail : public arrow::StatusDetail {
 public:
  explicit PlasmaStatusDetail(PlasmaErrorCode code) : code_(code) {}
  const char* type_id() const override { return kErrorDetailTypeId; }
  std::string ToString() const override {
    const char* type;
    switch (code()) {
      case PlasmaErrorCode::PlasmaObjectExists:
        type = "Plasma object exists";
        break;
      case PlasmaErrorCode::PlasmaObjectNonexistent:
        type = "Plasma object is nonexistent";
        break;
      case PlasmaErrorCode::PlasmaStoreFull:
        type = "Plasma store is full";
        break;
      case PlasmaErrorCode::PlasmaObjectAlreadySealed:
        type = "Plasma object is already sealed";
        break;
      default:
        type = "Unknown plasma error";
        break;
    }
    return std::string(type);
  }
  PlasmaErrorCode code() const { return code_; }

 private:
  PlasmaErrorCode code_;
};

bool IsPlasmaStatus(const arrow::Status& status, PlasmaErrorCode code) {
  if (status.ok()) {
    return false;
  }
  auto* detail = status.detail().get();
  return detail != nullptr && detail->type_id() == kErrorDetailTypeId &&
         static_cast<PlasmaStatusDetail*>(detail)->code() == code;
}

}  // namespace

using arrow::Status;

arrow::Status MakePlasmaError(PlasmaErrorCode code, std::string message) {
  arrow::StatusCode arrow_code = arrow::StatusCode::UnknownError;
  switch (code) {
    case PlasmaErrorCode::PlasmaObjectExists:
      arrow_code = arrow::StatusCode::AlreadyExists;
      break;
    case PlasmaErrorCode::PlasmaObjectNonexistent:
      arrow_code = arrow::StatusCode::KeyError;
      break;
    case PlasmaErrorCode::PlasmaStoreFull:
      arrow_code = arrow::StatusCode::CapacityError;
      break;
    case PlasmaErrorCode::PlasmaObjectAlreadySealed:
      // Maybe a stretch?
      arrow_code = arrow::StatusCode::TypeError;
      break;
  }
  return arrow::Status(arrow_code, std::move(message),
                       std::make_shared<PlasmaStatusDetail>(code));
}

bool IsPlasmaObjectExists(const arrow::Status& status) {
  return IsPlasmaStatus(status, PlasmaErrorCode::PlasmaObjectExists);
}
bool IsPlasmaObjectNonexistent(const arrow::Status& status) {
  return IsPlasmaStatus(status, PlasmaErrorCode::PlasmaObjectNonexistent);
}
bool IsPlasmaObjectAlreadySealed(const arrow::Status& status) {
  return IsPlasmaStatus(status, PlasmaErrorCode::PlasmaObjectAlreadySealed);
}
bool IsPlasmaStoreFull(const arrow::Status& status) {
  return IsPlasmaStatus(status, PlasmaErrorCode::PlasmaStoreFull);
}

UniqueID UniqueID::from_binary(const std::string& binary) {
  UniqueID id;
  std::memcpy(&id, binary.data(), sizeof(id));
  return id;
}

const uint8_t* UniqueID::data() const { return id_; }

uint8_t* UniqueID::mutable_data() { return id_; }

std::string UniqueID::binary() const {
  return std::string(reinterpret_cast<const char*>(id_), kUniqueIDSize);
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

// This code is from https://sites.google.com/site/murmurhash/
// and is public domain.
uint64_t MurmurHash64A(const void* key, int len, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t* data = reinterpret_cast<const uint64_t*>(key);
  const uint64_t* end = data + (len / 8);

  while (data != end) {
    uint64_t k = arrow::util::SafeLoad(data++);

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char* data2 = reinterpret_cast<const unsigned char*>(data);

  switch (len & 7) {
    case 7:
      h ^= uint64_t(data2[6]) << 48;  // fall through
    case 6:
      h ^= uint64_t(data2[5]) << 40;  // fall through
    case 5:
      h ^= uint64_t(data2[4]) << 32;  // fall through
    case 4:
      h ^= uint64_t(data2[3]) << 24;  // fall through
    case 3:
      h ^= uint64_t(data2[2]) << 16;  // fall through
    case 2:
      h ^= uint64_t(data2[1]) << 8;  // fall through
    case 1:
      h ^= uint64_t(data2[0]);
      h *= m;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

size_t UniqueID::hash() const { return MurmurHash64A(&id_[0], kUniqueIDSize, 0); }

bool UniqueID::operator==(const UniqueID& rhs) const {
  return std::memcmp(data(), rhs.data(), kUniqueIDSize) == 0;
}

const PlasmaStoreInfo* plasma_config;

}  // namespace plasma
