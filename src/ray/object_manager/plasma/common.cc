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

#include "ray/object_manager/plasma/common.h"

#include <limits>
#include <utility>

#include "arrow/util/ubsan.h"

#include "ray/object_manager/plasma/plasma_generated.h"

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

const PlasmaStoreInfo* plasma_config;

}  // namespace plasma
