#ifndef RAY_CONSTANTS_H_
#define RAY_CONSTANTS_H_

/// Length of Ray IDs in bytes.
constexpr int64_t kUniqueIDSize = 20;

/// An ObjectID's bytes are split into the task ID itself and the index of the
/// object's creation. This is the maximum width of the object index in bits.
constexpr int kObjectIdIndexSize = 32;
/// The maximum number of objects that can be returned by a task when finishing
/// execution. An ObjectID's bytes are split into the task ID itself and the
/// index of the object's creation. A positive index indicates an object
/// returned by the task, so the maximum number of objects that a task can
/// return is the maximum positive value for an integer with bit-width
/// `kObjectIdIndexSize`.
constexpr int64_t kMaxTaskReturns = ((int64_t)1 << (kObjectIdIndexSize - 1)) - 1;
/// The maximum number of objects that can be put by a task during execution.
/// An ObjectID's bytes are split into the task ID itself and the index of the
/// object's creation. A negative index indicates an object put by the task
/// during execution, so the maximum number of objects that a task can put is
/// the maximum negative value for an integer with bit-width
/// `kObjectIdIndexSize`.
constexpr int64_t kMaxTaskPuts = ((int64_t)1 << (kObjectIdIndexSize - 1));

/// Prefix for the object table keys in redis.
constexpr char kObjectTablePrefix[] = "ObjectTable";
/// Prefix for the task table keys in redis.
constexpr char kTaskTablePrefix[] = "TaskTable";

#endif  // RAY_CONSTANTS_H_
