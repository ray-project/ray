#ifndef RAY_CONSTANTS_H_
#define RAY_CONSTANTS_H_

/// Length of Ray IDs in bytes.
constexpr int64_t kUniqueIDSize = 20;

/// An ObjectID's bytes are split into the task ID itself and the index of the
/// object's creation. This is the maximum width of the object index.
constexpr int kObjectIdIndexSize = 16;
/// The maximum number of objects that can be returned by a task.
constexpr int64_t kMaxTaskReturns = (1 << (kObjectIdIndexSize - 1)) - 1;
/// The maximum number of objects that can be put by a task.
constexpr int64_t kMaxTaskPuts = (1 << (kObjectIdIndexSize - 1));

/// Prefix for the object table keys in redis.
constexpr char kObjectTablePrefix[] = "ObjectTable";
/// Prefix for the task table keys in redis.
constexpr char kTaskTablePrefix[] = "TaskTable";

#endif  // RAY_CONSTANTS_H_
