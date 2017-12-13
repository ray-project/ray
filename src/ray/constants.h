#ifndef RAY_CONSTANTS_H_
#define RAY_CONSTANTS_H_

/// Length of Ray IDs in bytes.
constexpr int64_t kUniqueIDSize = 20;

/// Prefix for the object table keys in redis.
constexpr char ObjectTablePrefix[] = "ObjectTable";
/// Prefix for the task table keys in redis.
constexpr char TaskTablePrefix[] = "TaskTable";

#endif  // RAY_CONSTANTS_H_
