#ifndef RAY_ID_H_
#define RAY_ID_H_

#include <inttypes.h>
#include <limits.h>

#include <chrono>
#include <cstring>
#include <mutex>
#include <random>
#include <string>

#include "plasma/common.h"
#include "ray/common/constants.h"
#include "ray/util/logging.h"
#include "ray/util/visibility.h"

namespace ray {

class WorkerID;
class UniqueID;
class JobID;

/// TODO(qwang): These 2 helper functions should be removed
/// once we separated the `WorkerID` from `UniqueID`.
///
/// A helper function that get the `DriverID` of the given job.
WorkerID ComputeDriverIdFromJob(const JobID &job_id);

enum class ObjectType : uint8_t {
  PUT_OBJECT = 0x0,
  RETURN_OBJECT = 0x1,
};

using ObjectIDFlagsType = uint16_t;
using ObjectIDIndexType = uint32_t;

// Declaration.
std::mt19937 RandomlySeededMersenneTwister();
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

// Change the compiler alignment to 1 byte (default is 8).
#pragma pack(push, 1)

template <typename T>
class BaseID {
 public:
  BaseID();
  static T FromRandom();
  static T FromBinary(const std::string &binary);
  static const T &Nil();
  static size_t Size() { return T::Size(); }

  size_t Hash() const;
  bool IsNil() const;
  bool operator==(const BaseID &rhs) const;
  bool operator!=(const BaseID &rhs) const;
  const uint8_t *Data() const;
  std::string Binary() const;
  std::string Hex() const;

 protected:
  BaseID(const std::string &binary) {
    std::memcpy(const_cast<uint8_t *>(this->Data()), binary.data(), T::Size());
  }
  // All IDs are immutable for hash evaluations. MutableData is only allow to use
  // in construction time, so this function is protected.
  uint8_t *MutableData();
  // For lazy evaluation, be careful to have one Id contained in another.
  // This hash code will be duplicated.
  mutable size_t hash_ = 0;
};

class UniqueID : public BaseID<UniqueID> {
 public:
  static size_t Size() { return kUniqueIDSize; }

  UniqueID() : BaseID() {}

 protected:
  UniqueID(const std::string &binary);

 protected:
  uint8_t id_[kUniqueIDSize];
};

class JobID : public BaseID<JobID> {
 public:
  static constexpr int64_t LENGTH = 4;

  static JobID FromInt(uint32_t value);

  static size_t Size() { return LENGTH; }

  static JobID FromRandom() = delete;

  JobID() : BaseID() {}

 private:
  uint8_t id_[LENGTH];
};

class ActorID : public BaseID<ActorID> {
 private:
  static constexpr size_t UNIQUE_BYTES_LENGTH = 4;

 public:
  static constexpr size_t LENGTH = UNIQUE_BYTES_LENGTH + JobID::LENGTH;

  static size_t Size() { return LENGTH; }

  static ActorID FromRandom(const JobID &job_id);

  static ActorID FromRandom() = delete;

  ActorID() : BaseID() {}

  JobID JobId() const;

 private:
  uint8_t id_[LENGTH];
};

class TaskID : public BaseID<TaskID> {
 private:
  static constexpr size_t UNIQUE_BYTES_LENGTH = 6;

 public:
  static constexpr size_t LENGTH = UNIQUE_BYTES_LENGTH + ActorID::LENGTH;

  TaskID() : BaseID() {}

  static size_t Size() { return LENGTH; }

  static TaskID ComputeDriverTaskId(const WorkerID &driver_id);

  /// Generate TaskID randomly.
  /// Note that the ActorID of this task should be NIL.
  static TaskID FromRandom();

  /// Generate TaskID from the given actor id.
  static TaskID FromRandom(const ActorID &actor_id);

  /// Get the id of the actor to which this task belongs.
  ActorID ActorId() const;

 private:
  uint8_t id_[LENGTH];
};

// TODO(qwang): Add complete designing to describe structure of ID.
class ObjectID : public BaseID<ObjectID> {
private:
  static constexpr size_t INDEX_BYTES_LENGTH = sizeof(ObjectIDIndexType);

  static constexpr size_t FLAGS_BYTES_LENGTH = sizeof(ObjectIDFlagsType);

 public:
  /// The maximum number of objects that can be returned or put by a task.
  static constexpr int64_t MAX_OBJECT_INDEX = ((int64_t) 1 << kObjectIdIndexSize) - 1;

  static constexpr size_t LENGTH = INDEX_BYTES_LENGTH + FLAGS_BYTES_LENGTH + TaskID::LENGTH;

  ObjectID() : BaseID() {}

  /// The maximum index of object. It also means the number of objects(put or return)
  /// of one task.
  static uint64_t MaxObjectIndex() { return MAX_OBJECT_INDEX; }

  static size_t Size() { return LENGTH; }

  /// Generate ObjectID by the given binary string of a plasma id.
  static ObjectID FromPlasmaIdBinary(const std::string &from);

  /// Generate an random actor cursor object id by the given actor.
  static ObjectID GenerateActorDummyObjectId(const ActorID &actor_id);

  plasma::ObjectID ToPlasmaId() const;

  ObjectID(const plasma::UniqueID &from);

  /// Get the index of this object in the task that created it.
  ///
  /// \return The index of object creation according to the task that created
  /// this object. This is positive if the task returned the object and negative
  /// if created by a put.
  ObjectIDIndexType ObjectIndex() const;

  /// Compute the task ID of the task that created the object.
  ///
  /// \return The task ID of the task that created this object.
  TaskID TaskId() const;

  /// Whether this object is created by a task.
  ///
  /// \return True if this object is created by a task, otherwise false.
  bool CreatedByTask() const;

  /// Whether this object was created through `ray.put`.
  bool IsPutObject() const;

  /// Whether this object was created as a return object of a task.
  bool IsReturnObject() const;

  /// Get the transport type of this object.
  uint8_t GetTransportType() const;

  /// Compute the object ID of an object put by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param index What index of the object put in the task.
  ///
  /// \return The computed object ID.
  static ObjectID ForPut(const TaskID &task_id, ObjectIDIndexType put_index);

  /// Compute the object ID of an object returned by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param return_index What index of the object returned by in the task.
  /// \param transport_type Which type of the transport that is used to
  ///        transfer this object.
  ///
  /// \return The computed object ID.
  static ObjectID ForTaskReturn(const TaskID &task_id, ObjectIDIndexType return_index,
                                uint8_t transport_type = 0);

  /// Create an object id randomly.
  ///
  /// \param transport_type Which type of the transport that is used to
  ///        transfer this object.
  ///
  /// \return A random object id.
  static ObjectID FromRandom();

 private:
  /// A helper method to generate an ObjectID.
  static ObjectID GenerateObjectId(const std::string &task_id_binary, ObjectIDFlagsType flags, ObjectIDIndexType object_index = 0);

 /// Get the flags out of this object id.
 ObjectIDFlagsType GetFlags() const;

 private:
  uint8_t id_[LENGTH];
};

static_assert(sizeof(JobID) == JobID::LENGTH + sizeof(size_t),
              "JobID size is not as expected");
static_assert(sizeof(ActorID) == ActorID::LENGTH + sizeof(size_t),
              "ActorID size is not as expected");
static_assert(sizeof(TaskID) == TaskID::LENGTH + sizeof(size_t),
              "TaskID size is not as expected");
static_assert(sizeof(ObjectID) == ObjectID::LENGTH + sizeof(size_t),
              "ObjectID size is not as expected");

std::ostream &operator<<(std::ostream &os, const UniqueID &id);
std::ostream &operator<<(std::ostream &os, const JobID &id);
std::ostream &operator<<(std::ostream &os, const ActorID &id);
std::ostream &operator<<(std::ostream &os, const TaskID &id);
std::ostream &operator<<(std::ostream &os, const ObjectID &id);

#define DEFINE_UNIQUE_ID(type)                                                 \
  class RAY_EXPORT type : public UniqueID {                                    \
   public:                                                                     \
    explicit type(const UniqueID &from) {                                      \
      std::memcpy(&id_, from.Data(), kUniqueIDSize);                           \
    }                                                                          \
    type() : UniqueID() {}                                                     \
    static type FromRandom() { return type(UniqueID::FromRandom()); }          \
    static type FromBinary(const std::string &binary) { return type(binary); } \
    static type Nil() { return type(UniqueID::Nil()); }                        \
    static size_t Size() { return kUniqueIDSize; }                             \
                                                                               \
   private:                                                                    \
    explicit type(const std::string &binary) {                                 \
      std::memcpy(&id_, binary.data(), kUniqueIDSize);                         \
    }                                                                          \
  };

#include "id_def.h"

#undef DEFINE_UNIQUE_ID

// Restore the compiler alignment to defult (8 bytes).
#pragma pack(pop)

/// Generate a task ID from the given info.
///
/// \param job_id The job that creates the task.
/// \param parent_task_id The parent task of this task.
/// \param parent_task_counter The task index of the worker.
/// \return The task ID generated from the given info.
const TaskID GenerateTaskId(const JobID &job_id, const TaskID &parent_task_id,
                            int parent_task_counter);

/// Compute the next actor handle ID of a new actor handle during a fork operation.
///
/// \param actor_handle_id The actor handle ID of original actor.
/// \param num_forks The count of forks of original actor.
/// \return The next actor handle ID generated from the given info.
const ActorHandleID ComputeNextActorHandleId(const ActorHandleID &actor_handle_id,
                                             int64_t num_forks);

template <typename T>
BaseID<T>::BaseID() {
  // Using const_cast to directly change data is dangerous. The cached
  // hash may not be changed. This is used in construction time.
  std::fill_n(this->MutableData(), T::Size(), 0xff);
}

template <typename T>
T BaseID<T>::FromRandom() {
  std::string data(T::Size(), 0);
  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex random_engine_mutex;
  std::lock_guard<std::mutex> lock(random_engine_mutex);
  static std::mt19937 generator = RandomlySeededMersenneTwister();
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (int i = 0; i < T::Size(); i++) {
    data[i] = static_cast<uint8_t>(dist(generator));
  }
  return T::FromBinary(data);
}

template <typename T>
T BaseID<T>::FromBinary(const std::string &binary) {
  RAY_CHECK(binary.size() == T::Size()) << "expected size is "
                                        << T::Size() << ", but got " << binary.size();
  T t = T::Nil();
  std::memcpy(t.MutableData(), binary.data(), T::Size());
  return t;
}

template <typename T>
const T &BaseID<T>::Nil() {
  static const T nil_id;
  return nil_id;
}

template <typename T>
bool BaseID<T>::IsNil() const {
  static T nil_id = T::Nil();
  return *this == nil_id;
}

template <typename T>
size_t BaseID<T>::Hash() const {
  // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
  // default value 0)
  if (!hash_) {
    hash_ = MurmurHash64A(Data(), T::Size(), 0);
  }
  return hash_;
}

template <typename T>
bool BaseID<T>::operator==(const BaseID &rhs) const {
  return std::memcmp(Data(), rhs.Data(), T::Size()) == 0;
}

template <typename T>
bool BaseID<T>::operator!=(const BaseID &rhs) const {
  return !(*this == rhs);
}

template <typename T>
uint8_t *BaseID<T>::MutableData() {
  return reinterpret_cast<uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
const uint8_t *BaseID<T>::Data() const {
  return reinterpret_cast<const uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
std::string BaseID<T>::Binary() const {
  return std::string(reinterpret_cast<const char *>(Data()), T::Size());
}

template <typename T>
std::string BaseID<T>::Hex() const {
  constexpr char hex[] = "0123456789abcdef";
  const uint8_t *id = Data();
  std::string result;
  for (int i = 0; i < T::Size(); i++) {
    unsigned int val = id[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

}  // namespace ray

namespace std {

#define DEFINE_UNIQUE_ID(type)                                           \
  template <>                                                            \
  struct hash<::ray::type> {                                             \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); } \
  };                                                                     \
  template <>                                                            \
  struct hash<const ::ray::type> {                                       \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); } \
  };

DEFINE_UNIQUE_ID(UniqueID);
DEFINE_UNIQUE_ID(JobID);
DEFINE_UNIQUE_ID(ActorID);
DEFINE_UNIQUE_ID(TaskID);
DEFINE_UNIQUE_ID(ObjectID);
#include "id_def.h"

#undef DEFINE_UNIQUE_ID
}  // namespace std
#endif  // RAY_ID_H_
