#include "plasma_common.h"

#include <random>

#include "format/plasma_generated.h"

using arrow::Status;

UniqueID UniqueID::from_random() {
  UniqueID id;
  uint8_t *data = id.mutable_data();
  std::random_device engine;
  for (int i = 0; i < kUniqueIDSize; i++) {
    data[i] = engine();
  }
  return id;
}

UniqueID UniqueID::from_binary(const std::string& binary) {
  UniqueID id;
  std::memcpy(&id, binary.data(), sizeof(id));
  return id;
}

const uint8_t *UniqueID::data() const {
  return id_;
}

uint8_t *UniqueID::mutable_data() {
  return id_;
}

std::string UniqueID::binary() const {
  return std::string(reinterpret_cast<const char *>(id_), kUniqueIDSize);
}

std::string UniqueID::sha1() const {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (int i = 0; i < sizeof(UniqueID); i++) {
    unsigned int val = id_[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

bool UniqueID::operator==(const UniqueID &rhs) const {
  return std::memcmp(data(), rhs.data(), kUniqueIDSize) == 0;
}

Status plasma_error_status(int plasma_error) {
  switch (plasma_error) {
    case PlasmaError_OK:
      return Status::OK();
    case PlasmaError_ObjectExists:
      return Status::PlasmaObjectExists("object already exists in the plasma store");
    case PlasmaError_ObjectNonexistent:
      return Status::PlasmaObjectNonexistent("object does not exist in the plasma store");
    case PlasmaError_OutOfMemory:
      return Status::PlasmaStoreFull("object does not fit in the plasma store");
    default:
      ARROW_LOG(FATAL) << "unknown plasma error code " << plasma_error;
  }
}
