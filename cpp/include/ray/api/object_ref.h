
#pragma once

#include <ray/api/logging.h>
#include <ray/api/ray_config.h>
#include <ray/api/ray_runtime_holder.h>
#include <ray/api/serializer.h>

#include <memory>
#include <msgpack.hpp>
#include <utility>

namespace ray {
namespace api {

template <typename T>
class ObjectRef;

/// Common helper functions used by ObjectRef<T> and ObjectRef<void>;
inline void CheckResult(const std::shared_ptr<msgpack::sbuffer> &packed_object) {
  bool has_error = Serializer::HasError(packed_object->data(), packed_object->size());
  if (has_error) {
    auto tp = Serializer::Deserialize<std::tuple<int, std::string>>(
        packed_object->data(), packed_object->size(), 1);
    std::string err_msg = std::get<1>(tp);
    throw RayException(err_msg);
  }
}

inline void CopyAndAddReference(std::string &dest_id, const std::string &id) {
  dest_id = id;
  ray::internal::RayRuntime()->AddLocalReference(id);
}

inline void SubReference(const std::string &id) {
  ray::internal::RayRuntime()->RemoveLocalReference(id);
}

/// Represents an object in the object store..
/// \param T The type of object.
template <typename T>
class ObjectRef {
 public:
  ObjectRef();
  ~ObjectRef();

  ObjectRef(const ObjectRef &rhs) { CopyAndAddReference(id_, rhs.id_); }

  ObjectRef &operator=(const ObjectRef &rhs) {
    CopyAndAddReference(id_, rhs.id_);
    return *this;
  }

  ObjectRef(const std::string &id);

  bool operator==(const ObjectRef<T> &object) const;

  /// Get a untyped ID of the object
  const std::string &ID() const;

  /// Get the object from the object store.
  /// This method will be blocked until the object is ready.
  ///
  /// \return shared pointer of the result.
  std::shared_ptr<T> Get() const;

  /// Make ObjectRef serializable
  MSGPACK_DEFINE(id_);

 private:
  std::string id_;
};

// ---------- implementation ----------
template <typename T>
inline static std::shared_ptr<T> GetFromRuntime(const ObjectRef<T> &object) {
  auto packed_object = internal::RayRuntime()->Get(object.ID());
  CheckResult(packed_object);

  return Serializer::Deserialize<std::shared_ptr<T>>(packed_object->data(),
                                                     packed_object->size());
}

template <typename T>
ObjectRef<T>::ObjectRef() {}

template <typename T>
ObjectRef<T>::ObjectRef(const std::string &id) {
  CopyAndAddReference(id_, id);
}

template <typename T>
ObjectRef<T>::~ObjectRef() {
  SubReference(id_);
}

template <typename T>
inline bool ObjectRef<T>::operator==(const ObjectRef<T> &object) const {
  return id_ == object.id_;
}

template <typename T>
const std::string &ObjectRef<T>::ID() const {
  return id_;
}

template <typename T>
inline std::shared_ptr<T> ObjectRef<T>::Get() const {
  return GetFromRuntime(*this);
}

template <>
class ObjectRef<void> {
 public:
  ObjectRef() = default;
  ~ObjectRef() { SubReference(id_); }

  ObjectRef(const ObjectRef &rhs) { CopyAndAddReference(id_, rhs.id_); }

  ObjectRef &operator=(const ObjectRef &rhs) {
    CopyAndAddReference(id_, rhs.id_);
    return *this;
  }

  ObjectRef(const std::string &id) { CopyAndAddReference(id_, id); }

  bool operator==(const ObjectRef<void> &object) const { return id_ == object.id_; }

  /// Get a untyped ID of the object
  const std::string &ID() const { return id_; }

  /// Get the object from the object store.
  /// This method will be blocked until the object is ready.
  ///
  /// \return shared pointer of the result.
  void Get() const {
    auto packed_object = internal::RayRuntime()->Get(id_);
    CheckResult(packed_object);
  }

  /// Make ObjectRef serializable
  MSGPACK_DEFINE(id_);

 private:
  std::string id_;
};
}  // namespace api
}  // namespace ray