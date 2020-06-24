
#pragma once

#include <memory>
#include <utility>

#include <msgpack.hpp>

#include "ray/core.h"

namespace ray {
namespace api {

/// Represents an object in the object store..
/// \param T The type of object.
template <typename T>
class ObjectRef {
 public:
  ObjectRef();

  ObjectRef(const ObjectID &id);

  bool operator==(const ObjectRef<T> &object) const;

  /// Get a untyped ID of the object
  const ObjectID &ID() const;

  /// Get the object from the object store.
  /// This method will be blocked until the object is ready.
  ///
  /// \return shared pointer of the result.
  std::shared_ptr<T> Get() const;

  /// Make ObjectRef serializable
  MSGPACK_DEFINE(id_);

 private:
  ObjectID id_;
};

// ---------- implementation ----------
#include <ray/api.h>

template <typename T>
ObjectRef<T>::ObjectRef() {}

template <typename T>
ObjectRef<T>::ObjectRef(const ObjectID &id) {
  id_ = id;
}

template <typename T>
inline bool ObjectRef<T>::operator==(const ObjectRef<T> &object) const {
  return id_ == object.id_;
}

template <typename T>
const ObjectID &ObjectRef<T>::ID() const {
  return id_;
}

template <typename T>
inline std::shared_ptr<T> ObjectRef<T>::Get() const {
  return Ray::Get(*this);
}
}  // namespace api
}  // namespace ray