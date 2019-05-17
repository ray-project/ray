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
#include "ray/constants.h"
#include "ray/util/logging.h"
#include "ray/util/visibility.h"

namespace ray {

class DriverID;
class UniqueID;

// Declaration.
std::mt19937 RandomlySeededMersenneTwister();
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

// Change the compiler alignment to 1 byte (default is 8).
#pragma pack(push, 1)

template <typename T>
class BaseID {
 public:
  BaseID();
  static T from_random();
  static T from_binary(const std::string &binary);
  static const T &nil();
  static size_t size() { return T::size(); }

  size_t hash() const;
  bool is_nil() const;
  bool operator==(const BaseID &rhs) const;
  bool operator!=(const BaseID &rhs) const;
  const uint8_t *data() const;
  std::string binary() const;
  std::string hex() const;

 protected:
  BaseID(const std::string &binary) {
    std::memcpy(const_cast<uint8_t *>(this->data()), binary.data(), T::size());
  }
  // All IDs are immutable for hash evaluations. mutable_data is only allow to use
  // in construction time, so this function is protected.
  uint8_t *mutable_data();
  // For lazy evaluation, be careful to have one Id contained in another.
  // This hash code will be duplicated.
  mutable size_t hash_ = 0;
};

class UniqueID : public BaseID<UniqueID> {
 public:
  UniqueID() : BaseID(){};
  static size_t size() { return kUniqueIDSize; }

 protected:
  UniqueID(const std::string &binary);

 protected:
  uint8_t id_[kUniqueIDSize];
};

class TaskID : public BaseID<TaskID> {
 public:
  TaskID() : BaseID() {}
  static size_t size() { return kTaskIDSize; }
  static TaskID GetDriverTaskID(const DriverID &driver_id);

 private:
  uint8_t id_[kTaskIDSize];
};

class ObjectID : public BaseID<ObjectID> {
 public:
  ObjectID() : BaseID() {}
  static size_t size() { return kUniqueIDSize; }
  plasma::ObjectID to_plasma_id() const;
  ObjectID(const plasma::UniqueID &from);

  /// Get the index of this object in the task that created it.
  ///
  /// \return The index of object creation according to the task that created
  /// this object. This is positive if the task returned the object and negative
  /// if created by a put.
  int32_t object_index() const { return index_; }

  /// Compute the task ID of the task that created the object.
  ///
  /// \return The task ID of the task that created this object.
  TaskID task_id() const;

  /// Compute the object ID of an object put by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param index What index of the object put in the task.
  /// \return The computed object ID.
  static ObjectID for_put(const TaskID &task_id, int64_t put_index);

  /// Compute the object ID of an object returned by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param return_index What index of the object returned by in the task.
  /// \return The computed object ID.
  static ObjectID for_task_return(const TaskID &task_id, int64_t return_index);

 private:
  uint8_t id_[kTaskIDSize];
  int32_t index_;
};

static_assert(sizeof(TaskID) == kTaskIDSize + sizeof(size_t),
              "TaskID size is not as expected");
static_assert(sizeof(ObjectID) == sizeof(int32_t) + sizeof(TaskID),
              "ObjectID size is not as expected");

std::ostream &operator<<(std::ostream &os, const UniqueID &id);
std::ostream &operator<<(std::ostream &os, const TaskID &id);
std::ostream &operator<<(std::ostream &os, const ObjectID &id);

#define DEFINE_UNIQUE_ID(type)                                                  \
  class RAY_EXPORT type : public UniqueID {                                     \
   public:                                                                      \
    explicit type(const UniqueID &from) {                                       \
      std::memcpy(&id_, from.data(), kUniqueIDSize);                            \
    }                                                                           \
    type() : UniqueID() {}                                                      \
    static type from_random() { return type(UniqueID::from_random()); }         \
    static type from_binary(const std::string &binary) { return type(binary); } \
    static type nil() { return type(UniqueID::nil()); }                         \
    static size_t size() { return kUniqueIDSize; }                              \
                                                                                \
   private:                                                                     \
    explicit type(const std::string &binary) {                                  \
      std::memcpy(&id_, binary.data(), kUniqueIDSize);                          \
    }                                                                           \
  };

#include "id_def.h"

#undef DEFINE_UNIQUE_ID

// Restore the compiler alignment to defult (8 bytes).
#pragma pack(pop)

/// Generate a task ID from the given info.
///
/// \param driver_id The driver that creates the task.
/// \param parent_task_id The parent task of this task.
/// \param parent_task_counter The task index of the worker.
/// \return The task ID generated from the given info.
const TaskID GenerateTaskId(const DriverID &driver_id, const TaskID &parent_task_id,
                            int parent_task_counter);

template <typename T>
BaseID<T>::BaseID() {
  // Using const_cast to directly change data is dangerous. The cached
  // hash may not be changed. This is used in construction time.
  std::fill_n(this->mutable_data(), T::size(), 0xff);
}

template <typename T>
T BaseID<T>::from_random() {
  std::string data(T::size(), 0);
  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex random_engine_mutex;
  std::lock_guard<std::mutex> lock(random_engine_mutex);
  static std::mt19937 generator = RandomlySeededMersenneTwister();
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (int i = 0; i < T::size(); i++) {
    data[i] = static_cast<uint8_t>(dist(generator));
  }
  return T::from_binary(data);
}

template <typename T>
T BaseID<T>::from_binary(const std::string &binary) {
  T t = T::nil();
  std::memcpy(t.mutable_data(), binary.data(), T::size());
  return t;
}

template <typename T>
const T &BaseID<T>::nil() {
  static const T nil_id;
  return nil_id;
}

template <typename T>
bool BaseID<T>::is_nil() const {
  static T nil_id = T::nil();
  return *this == nil_id;
}

template <typename T>
size_t BaseID<T>::hash() const {
  // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
  // default value 0)
  if (!hash_) {
    hash_ = MurmurHash64A(data(), T::size(), 0);
  }
  return hash_;
}

template <typename T>
bool BaseID<T>::operator==(const BaseID &rhs) const {
  return std::memcmp(data(), rhs.data(), T::size()) == 0;
}

template <typename T>
bool BaseID<T>::operator!=(const BaseID &rhs) const {
  return !(*this == rhs);
}

template <typename T>
uint8_t *BaseID<T>::mutable_data() {
  return reinterpret_cast<uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
const uint8_t *BaseID<T>::data() const {
  return reinterpret_cast<const uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
std::string BaseID<T>::binary() const {
  return std::string(reinterpret_cast<const char *>(data()), T::size());
}

template <typename T>
std::string BaseID<T>::hex() const {
  constexpr char hex[] = "0123456789abcdef";
  const uint8_t *id = data();
  std::string result;
  for (int i = 0; i < T::size(); i++) {
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
    size_t operator()(const ::ray::type &id) const { return id.hash(); } \
  };                                                                     \
  template <>                                                            \
  struct hash<const ::ray::type> {                                       \
    size_t operator()(const ::ray::type &id) const { return id.hash(); } \
  };

DEFINE_UNIQUE_ID(UniqueID);
DEFINE_UNIQUE_ID(TaskID);
DEFINE_UNIQUE_ID(ObjectID);
#include "id_def.h"

#undef DEFINE_UNIQUE_ID
}  // namespace std
#endif  // RAY_ID_H_
