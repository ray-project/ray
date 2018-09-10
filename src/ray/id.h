#ifndef RAY_ID_H_
#define RAY_ID_H_

#include <inttypes.h>

#include <cstring>
#include <string>

#include "plasma/common.h"
#include "ray/constants.h"
#include "ray/util/visibility.h"

extern "C" {
#include "sha256.h"
}

namespace ray {

class RAY_EXPORT UniqueID {
 public:
  UniqueID() {}
  UniqueID(const plasma::UniqueID &from);
  static UniqueID from_random();
  static UniqueID from_binary(const std::string &binary);
  static const UniqueID nil();
  size_t hash() const;
  bool is_nil() const;
  bool operator==(const UniqueID &rhs) const;
  bool operator!=(const UniqueID &rhs) const;
  const uint8_t *data() const;
  uint8_t *mutable_data();
  size_t size() const;
  std::string binary() const;
  std::string hex() const;
  plasma::UniqueID to_plasma_id() const;

 private:
  uint8_t id_[kUniqueIDSize];
};

static_assert(std::is_standard_layout<UniqueID>::value,
              "UniqueID must be standard");

std::ostream &operator<<(std::ostream &os, const UniqueID &id);

typedef UniqueID TaskID;
typedef UniqueID JobID;
typedef UniqueID ObjectID;
typedef UniqueID FunctionID;
typedef UniqueID ClassID;
typedef UniqueID ActorID;
typedef UniqueID ActorHandleID;
typedef UniqueID WorkerID;
typedef UniqueID DriverID;
typedef UniqueID ConfigID;
typedef UniqueID ClientID;

// TODO(swang): ObjectID and TaskID should derive from UniqueID. Then, we
// can make these methods of the derived classes.
/// Finish computing a task ID. Since objects created by the task share a
/// prefix of the ID, the suffix of the task ID is zeroed out by this function.
///
/// \param task_id A task ID to finish.
/// \return The finished task ID. It may now be used to compute IDs for objects
/// created by the task.
const TaskID FinishTaskId(const TaskID &task_id);

/// Compute the object ID of an object returned by the task.
///
/// \param task_id The task ID of the task that created the object.
/// \param return_index What number return value this object is in the task.
/// \return The computed object ID.
const ObjectID ComputeReturnId(const TaskID &task_id, int64_t return_index);

/// Compute the object ID of an object put by the task.
///
/// \param task_id The task ID of the task that created the object.
/// \param put_index What number put this object was created by in the task.
/// \return The computed object ID.
const ObjectID ComputePutId(const TaskID &task_id, int64_t put_index);

/// Compute the task ID of the task that created the object.
///
/// \param object_id The object ID.
/// \return The task ID of the task that created this object.
const TaskID ComputeTaskId(const ObjectID &object_id);

/// Generate a task ID from the given info.
///
/// \param driver_id The driver that creates the task.
/// \param parent_task_id The parent task of this task.
/// \param parent_task_counter The task index of the worker.
/// \return The task ID generated from the given info.
const TaskID GenerateTaskId(const DriverID &driver_id, const TaskID &parent_task_id,
                            int parent_task_counter);

/// Compute the index of this object in the task that created it.
///
/// \param object_id The object ID.
/// \return The index of object creation according to the task that created
/// this object. This is positive if the task returned the object and negative
/// if created by a put.
int64_t ComputeObjectIndex(const ObjectID &object_id);

}  // namespace ray

namespace std {
template <>
struct hash<::ray::UniqueID> {
  size_t operator()(const ::ray::UniqueID &id) const { return id.hash(); }
};

template <>
struct hash<const ::ray::UniqueID> {
  size_t operator()(const ::ray::UniqueID &id) const { return id.hash(); }
};
}
#endif  // RAY_ID_H_
