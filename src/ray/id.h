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
class BaseId {
 public:
  BaseId() {
    std::fill_n(reinterpret_cast<uint8_t*>(this), T::size(), 0xff);
  }

  static T from_random();

  static T from_binary(const std::string &binary);

  static const T &nil();
  
  size_t hash() const;

  bool is_nil() const;
  bool operator==(const BaseId &rhs) const;
  bool operator!=(const BaseId &rhs) const;
  const uint8_t *data() const;
  static size_t size() {
    return sizeof(T);
  }
  std::string binary() const;
  std::string hex() const;

  //plasma::UniqueID to_plasma_id() const;
 protected:
  BaseId(const std::string &binary) {
    std::memcpy(reinterpret_cast<uint8_t*>(this), binary.data(), T::size());
  }
 private:
};

class UniqueID : public BaseId<UniqueID> {
 public:
  UniqueID() : BaseId() {};
  size_t hash() const;
  static size_t size()  {
    return kUniqueIDSize;
  }

 private:
  UniqueID(const std::string &binary);

 protected:
  uint8_t id_[kUniqueIDSize];
  mutable size_t hash_ = 0;
};

static_assert(std::is_standard_layout<UniqueID>::value, "UniqueID must be standard");

#pragma pack(push, 1)

class TaskID : public BaseId<TaskID> {
 public:
  TaskID() : BaseId() {}
  size_t hash() const;
  static size_t size()  {
    return kUniqueIDSize - sizeof(int64_t);
  }
  static TaskID GetDriverTaskID(const DriverID &driver_id);

 protected:
  uint8_t id_[kUniqueIDSize - sizeof(int64_t)];
};


class ObjectID : public BaseId<ObjectID> {
 public:
  ObjectID() : BaseId() {}
  size_t hash() const;
  static size_t size()  {
    return kUniqueIDSize;
  }
  plasma::ObjectID to_plasma_id() const;
  ObjectID(const plasma::UniqueID &from);

  bool is_put() const {
    return index_ < 0;
  }

  int64_t index() const {
    return index_;
  }

  TaskID task_id() const {
    return task_id_;
  }

  static ObjectID build(const TaskID &task_id, bool is_put, int64_t index);

 protected:
  TaskID task_id_;
  int64_t index_;
  mutable size_t hash_ = 0;
};

static_assert(std::is_standard_layout<ObjectID>::value, "ObjectID must be standard");
static_assert(sizeof(ObjectID) == sizeof(size_t) + kUniqueIDSize, "ObjectID size is not as expected");

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


template<typename T>
T BaseId<T>::from_random() {
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
T BaseId<T>::from_binary(const std::string &binary) {
  T t = T::nil();
  std::memcpy(reinterpret_cast<uint8_t*>(&t), binary.data(), T::size());
  return t;
}

template<typename T>
const T &BaseId<T>::nil(){
  static const T nil_id;
  return nil_id;
}

template<typename T>
bool BaseId<T>::is_nil() const {
  const uint8_t *d = data();
  for (int i = 0; i < T::size(); ++i) {
    if (d[i] != 0xff) {
      return false;
    }
  }
  return true;
}

template<typename T>
bool BaseId<T>::operator==(const BaseId &rhs) const {
  return std::memcmp(data(), rhs.data(), T::size()) == 0;
}

template<typename T>
bool BaseId<T>::operator!=(const BaseId &rhs) const {
  return !(*this == rhs);
}

template<typename T>
const uint8_t *BaseId<T>::data() const {
  return reinterpret_cast<const uint8_t*>(this);
}

template<typename T>
std::string BaseId<T>::binary() const {
  return std::string(reinterpret_cast<const char *>(this), T::size());
}

template<typename T>
std::string BaseId<T>::hex() const {
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
