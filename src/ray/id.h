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
#include "ray/util/visibility.h"
#include "ray/util/logging.h"

namespace ray {

class DriverID;
class UniqueID;

std::mt19937 RandomlySeededMersenneTwister();

template<typename T>
class BaseID {
 public:
  BaseID() {
    std::fill_n(reinterpret_cast<uint8_t*>(this), T::size(), 0xff);
  }

  static T from_random();
  static T from_binary(const std::string &binary);
  static const T &nil();
  static size_t size() { return sizeof(T); }

  size_t hash() const;
  bool is_nil() const;
  bool operator==(const BaseID &rhs) const;
  bool operator!=(const BaseID &rhs) const;
  const uint8_t *data() const;
  std::string binary() const;
  std::string hex() const;

 protected:
  BaseID(const std::string &binary) {
    std::memcpy(reinterpret_cast<uint8_t*>(this), binary.data(), T::size());
  }
};

#pragma pack(push, 1)

class UniqueID : public BaseID<UniqueID> {
 public:
  UniqueID() : BaseID() {};
  size_t hash() const;
  static size_t size()  { return kUniqueIDSize; }

 private:
  UniqueID(const std::string &binary);

 protected:
  uint8_t id_[kUniqueIDSize];
  mutable size_t hash_ = 0;
};

static_assert(std::is_standard_layout<UniqueID>::value, "UniqueID must be standard");

class TaskID : public BaseID<TaskID> {
 public:
  TaskID() : BaseID() {}
  size_t hash() const;
  static size_t size()  {
    return kUniqueIDSize - sizeof(int32_t);
  }
  static TaskID GetDriverTaskID(const DriverID &driver_id);

 protected:
  uint8_t id_[kUniqueIDSize - sizeof(int32_t)];
};


class ObjectID : public BaseID<ObjectID> {
 public:
  ObjectID() : BaseID() {}
  size_t hash() const;
  static size_t size()  {
    return kUniqueIDSize;
  }
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
  const TaskID &task_id() const { return task_id_; }

  /// Compute the object ID of an object put or returned by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param index What number the object was created by in the task.
  /// \return The computed object ID.
  static ObjectID build(const TaskID &task_id, bool is_put, int64_t index);

 protected:
  TaskID task_id_;
  int32_t index_;
  mutable size_t hash_ = 0;
};

static_assert(std::is_standard_layout<ObjectID>::value, "ObjectID must be standard");
static_assert(sizeof(ObjectID) == sizeof(size_t) + kUniqueIDSize, "ObjectID size is not as expected");
static_assert(sizeof(TaskID) == kUniqueIDSize - kObjectIdIndexSize/8, "TaskID size is not as expected");

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

#pragma pack(pop)

/// Generate a task ID from the given info.
///
/// \param driver_id The driver that creates the task.
/// \param parent_task_id The parent task of this task.
/// \param parent_task_counter The task index of the worker.
/// \return The task ID generated from the given info.
const TaskID GenerateTaskId(const DriverID &driver_id, const TaskID &parent_task_id,
                            int parent_task_counter);


template<typename T>
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

template<typename T>
T BaseID<T>::from_binary(const std::string &binary) {
  T t = T::nil();
  std::memcpy(reinterpret_cast<uint8_t*>(&t), binary.data(), T::size());
  return t;
}

template<typename T>
const T &BaseID<T>::nil(){
  static const T nil_id;
  return nil_id;
}

template<typename T>
bool BaseID<T>::is_nil() const {
  const uint8_t *d = data();
  for (int i = 0; i < T::size(); ++i) {
    if (d[i] != 0xff) {
      return false;
    }
  }
  return true;
}

template<typename T>
bool BaseID<T>::operator==(const BaseID &rhs) const {
  return std::memcmp(data(), rhs.data(), T::size()) == 0;
}

template<typename T>
bool BaseID<T>::operator!=(const BaseID &rhs) const {
  return !(*this == rhs);
}

template<typename T>
const uint8_t *BaseID<T>::data() const {
  return reinterpret_cast<const uint8_t*>(this);
}

template<typename T>
std::string BaseID<T>::binary() const {
  return std::string(reinterpret_cast<const char *>(this), T::size());
}

template<typename T>
std::string BaseID<T>::hex() const {
  constexpr char hex[] = "0123456789abcdef";
  const uint8_t *id = reinterpret_cast<const uint8_t *>(this);
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
