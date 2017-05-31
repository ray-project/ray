#ifndef PLASMA_COMMON_H
#define PLASMA_COMMON_H

#include <cstring>
#include <string>

#include "logging.h"
#include "status.h"

constexpr int64_t kUniqueIDSize = 20;

class UniqueID {
 public:
  static UniqueID from_random();
  static UniqueID from_binary(const std::string &binary);
  bool operator==(const UniqueID &rhs) const;
  const uint8_t *data() const;
  uint8_t *mutable_data();
  std::string binary() const;
  std::string hex() const;

 private:
  uint8_t id_[kUniqueIDSize];
};

static_assert(std::is_pod<UniqueID>::value, "UniqueID must be plain old data");

struct UniqueIDHasher {
  /* ObjectID hashing function. */
  size_t operator()(const UniqueID &id) const {
    size_t result;
    std::memcpy(&result, id.data(), sizeof(size_t));
    return result;
  }
};

typedef UniqueID ObjectID;

arrow::Status plasma_error_status(int plasma_error);

#endif  // PLASMA_COMMON_H
