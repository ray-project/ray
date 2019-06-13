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

class DriverID;
class UniqueID;

// Declaration.
std::mt19937 RandomlySeededMersenneTwister();
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

// Change the compiler alignment to 1 byte (default is 8).
#pragma pack(push, 1)

template <typename T, size_t N>
class BaseID {
 public:
  BaseID();
  static T FromRandom();
  static T FromBinary(const std::string &binary);
  static const T &Nil();
  static constexpr size_t Size() { return N; }

  size_t Hash() const;
  bool IsNil() const;
  bool operator==(const BaseID &rhs) const;
  bool operator!=(const BaseID &rhs) const;
  const uint8_t *Data() const;
  std::string Binary() const;
  std::string Hex() const;

 protected:
  // All IDs are immutable for hash evaluations. MutableData is only allow to use
  // in construction time, so this function is protected.
  uint8_t *MutableData();
  // For lazy evaluation, be careful to have one Id contained in another.
  // This hash code will be duplicated.
  mutable size_t hash_ = 0;
};

class UniqueID : public BaseID<UniqueID, kUniqueIDSize> {
 public:
  UniqueID() : BaseID(){};

 protected:
  UniqueID(const std::string &binary);

 protected:
  uint8_t id_[kUniqueIDSize];
};

class ActorID : public BaseID<ActorID, kActorIdSize> {
 public:
  ActorID() : BaseID() {}

 private:
  uint8_t id_[kActorIdSize];
};

class TaskID : public BaseID<TaskID, kTaskIDSize + kActorIdSize> {
 public:
  TaskID() : BaseID() {}
  ActorID ActorId() const {
    return ActorID::FromBinary(
        std::string(reinterpret_cast<const char *>(actor_id_), ActorID::Size()));
  }
  static TaskID GetDriverTaskID(const DriverID &driver_id);

 private:
  uint8_t id_[kTaskIDSize];
  uint8_t actor_id_[kActorIdSize];
};

class ObjectID : public BaseID<ObjectID, kUniqueIDSize> {
 public:
  ObjectID() : BaseID() {}
  plasma::ObjectID ToPlasmaId() const;
  ObjectID(const plasma::UniqueID &from);

  /// Get the index of this object in the task that created it.
  ///
  /// \return The index of object creation according to the task that created
  /// this object. This is positive if the task returned the object and negative
  /// if created by a put.
  int32_t ObjectIndex() const { return index_; }

  /// Compute the task ID of the task that created the object.
  ///
  /// \return The task ID of the task that created this object.
  TaskID TaskId() const;

  /// Compute the object ID of an object put by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param index What index of the object put in the task.
  /// \return The computed object ID.
  static ObjectID ForPut(const TaskID &task_id, int64_t put_index);

  /// Compute the object ID of an object returned by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param return_index What index of the object returned by in the task.
  /// \return The computed object ID.
  static ObjectID ForTaskReturn(const TaskID &task_id, int64_t return_index);

 private:
  uint8_t task_id_[kTaskIDSize];
  uint8_t actor_id_[kActorIdSize];
  int32_t index_;
};

std::ostream &operator<<(std::ostream &os, const UniqueID &id);
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
    static constexpr size_t Size() { return kUniqueIDSize; }                   \
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
/// \param driver_id The driver that creates the task.
/// \param parent_task_id The parent task of this task.
/// \param parent_task_counter The task index of the worker.
/// \param actor_id The actor that this task is destined for, if any.
/// \return The task ID generated from the given info.
const TaskID GenerateTaskId(const DriverID &driver_id, const TaskID &parent_task_id,
                            int parent_task_counter, const ActorID &actor_id);

template <typename T, size_t N>
BaseID<T, N>::BaseID() {
  // Using const_cast to directly change data is dangerous. The cached
  // hash may not be changed. This is used in construction time.
  std::fill_n(this->MutableData(), N, 0xff);
}

template <typename T, size_t N>
T BaseID<T, N>::FromRandom() {
  std::string data(N, 0);
  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex random_engine_mutex;
  std::lock_guard<std::mutex> lock(random_engine_mutex);
  static std::mt19937 generator = RandomlySeededMersenneTwister();
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (int i = 0; i < N; i++) {
    data[i] = static_cast<uint8_t>(dist(generator));
  }
  return T::FromBinary(data);
}

template <typename T, size_t N>
T BaseID<T, N>::FromBinary(const std::string &binary) {
  RAY_CHECK(binary.size() == N) << "String has size " << binary.size() << " but expected "
                                << N;
  T t = T::Nil();
  std::memcpy(t.MutableData(), binary.data(), N);
  return t;
}

template <typename T, size_t N>
const T &BaseID<T, N>::Nil() {
  static const T nil_id;
  return nil_id;
}

template <typename T, size_t N>
bool BaseID<T, N>::IsNil() const {
  static T nil_id = T::Nil();
  return *this == nil_id;
}

template <typename T, size_t N>
size_t BaseID<T, N>::Hash() const {
  // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
  // default value 0)
  if (!hash_) {
    hash_ = MurmurHash64A(Data(), N, 0);
  }
  return hash_;
}

template <typename T, size_t N>
bool BaseID<T, N>::operator==(const BaseID &rhs) const {
  return std::memcmp(Data(), rhs.Data(), N) == 0;
}

template <typename T, size_t N>
bool BaseID<T, N>::operator!=(const BaseID &rhs) const {
  return !(*this == rhs);
}

template <typename T, size_t N>
uint8_t *BaseID<T, N>::MutableData() {
  return reinterpret_cast<uint8_t *>(this) + sizeof(hash_);
}

template <typename T, size_t N>
const uint8_t *BaseID<T, N>::Data() const {
  return reinterpret_cast<const uint8_t *>(this) + sizeof(hash_);
}

template <typename T, size_t N>
std::string BaseID<T, N>::Binary() const {
  return std::string(reinterpret_cast<const char *>(Data()), N);
}

template <typename T, size_t N>
std::string BaseID<T, N>::Hex() const {
  constexpr char hex[] = "0123456789abcdef";
  const uint8_t *id = Data();
  std::string result;
  for (int i = 0; i < N; i++) {
    unsigned int val = id[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

}  // namespace ray

namespace std {

#define DEFINE_UNIQUE_ID(type)                                               \
  template <>                                                                \
  struct hash<::ray::type> {                                                 \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); }     \
  };                                                                         \
  template <>                                                                \
  struct hash<const ::ray::type> {                                           \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); }     \
  };                                                                         \
  static_assert(sizeof(::ray::type) == sizeof(size_t) + ::ray::type::Size(), \
                #type " size is not as expected.");

DEFINE_UNIQUE_ID(UniqueID);
DEFINE_UNIQUE_ID(ActorID);
DEFINE_UNIQUE_ID(TaskID);
DEFINE_UNIQUE_ID(ObjectID);
#include "id_def.h"

#undef DEFINE_UNIQUE_ID
}  // namespace std
#endif  // RAY_ID_H_
