#ifndef RAY_CONSTANTS_H_
#define RAY_CONSTANTS_H_

#include <limits.h>
#include <stdint.h>

/// Length of Ray full-length IDs in bytes.
constexpr size_t kUniqueIDSize = 20;

/// Length of plasma ID in bytes.
constexpr size_t kPlasmaIdSize = 20;

/// An ObjectID's bytes are split into the task ID itself and the index of the
/// object's creation. This is the maximum width of the object index in bits.
constexpr int kObjectIdIndexSize = 32;
static_assert(kObjectIdIndexSize % CHAR_BIT == 0,
              "ObjectID prefix not a multiple of bytes");

/// Prefix for the object table keys in redis.
constexpr char kObjectTablePrefix[] = "ObjectTable";
/// Prefix for the task table keys in redis.
constexpr char kTaskTablePrefix[] = "TaskTable";

constexpr char kWorkerDynamicOptionPlaceholderPrefix[] = "RAY_WORKER_OPTION_";

#endif  // RAY_CONSTANTS_H_
