#ifndef RAY_ID_H_
#define RAY_ID_H_

#include <inttypes.h>

#include <cstring>
#include <string>

#include "plasma/common.h"
#include "ray/constants.h"
#include "ray/util/visibility.h"

namespace ray {

class RAY_EXPORT UniqueID {
 public:
  UniqueID() {}
  UniqueID(const plasma::UniqueID &from);
  static UniqueID from_random();
  static UniqueID from_binary(const std::string &binary);
  static const UniqueID nil();
  bool is_nil() const;
  bool operator==(const UniqueID &rhs) const;
  const uint8_t *data() const;
  uint8_t *mutable_data();
  size_t size() const;
  std::string binary() const;
  std::string hex() const;
  plasma::UniqueID to_plasma_id();

 private:
  uint8_t id_[kUniqueIDSize];
};

static_assert(std::is_standard_layout<UniqueID>::value,
              "UniqueID must be standard");

struct UniqueIDHasher {
  // ID hashing function.
  size_t operator()(const UniqueID &id) const {
    size_t result;
    std::memcpy(&result, id.data(), sizeof(size_t));
    return result;
  }
};

typedef UniqueID TaskID;
typedef UniqueID JobID;
typedef UniqueID ObjectID;
typedef UniqueID FunctionID;
typedef UniqueID ClassID;
typedef UniqueID ActorID;
typedef UniqueID ActorHandleID;
typedef UniqueID WorkerID;
typedef UniqueID DriverID;
typedef UniqueID DBClientID;
typedef UniqueID ConfigID;

}  // namespace ray

#endif  // RAY_ID_H_
