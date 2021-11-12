// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <ray/api/ray_runtime_holder.h>
#include <ray/api/serializer.h>

#include <memory>
#include <msgpack.hpp>
#include <utility>

namespace ray {

template <typename T>
class ObjectRef;

/// Common helper functions used by ObjectRef<T> and ObjectRef<void>;
inline void CheckResult(const std::shared_ptr<msgpack::sbuffer> &packed_object) {
  bool has_error =
      ray::internal::Serializer::HasError(packed_object->data(), packed_object->size());
  if (has_error) {
    auto tp = ray::internal::Serializer::Deserialize<std::tuple<int, std::string>>(
        packed_object->data(), packed_object->size(), 1);
    std::string err_msg = std::get<1>(tp);
    throw ray::internal::RayTaskException(err_msg);
  }
}

inline void AddReference(const std::string &id) {
  ray::internal::GetRayRuntime()->AddLocalReference(id);
}

inline void SubReference(const std::string &id) {
  ray::internal::GetRayRuntime()->RemoveLocalReference(id);
}

/// This base class used to reduce duplicate code.
class ObjectRefBase {
 public:
  ObjectRefBase() = default;
  virtual ~ObjectRefBase() { SubReference(id_); }

  ObjectRefBase(const std::string &id) {
    id_ = id;
    AddReference(id);
  }

  /// Get a untyped ID of the object
  const std::string &ID() const { return id_; }

  void ReadExternal() const {
    ray::internal::GetRayRuntime()->RegisterOwnershipInfoAndResolveFuture(id_, "",
                                                                          owner_address_);
  }

  void WriteExternal() {
    owner_address_ = ray::internal::GetRayRuntime()->GetOwnershipInfo(id_);
  }

  // Used to identify its type.
  static bool IsObjectRef() { return true; }
  bool operator==(const ObjectRefBase &object) const { return id_ == object.id_; }

 protected:
  ObjectRefBase(ObjectRefBase &&rhs) { Move(std::move(rhs)); }

  ObjectRefBase &operator=(ObjectRefBase &&rhs) {
    if (rhs == *this) {
      return *this;
    }

    SubReference(id_);
    Move(std::move(rhs));
    return *this;
  }

  ObjectRefBase(const ObjectRefBase &rhs) { Copy(rhs); }

  ObjectRefBase &operator=(const ObjectRefBase &rhs) {
    SubReference(id_);
    Copy(rhs);
    return *this;
  }

  template <typename T>
  void Copy(const T &rhs) {
    id_ = rhs.id_;
    owner_address_ = rhs.owner_address_;
    AddReference(rhs.id_);
  }

  template <typename T>
  void Move(T &&rhs) {
    id_ = std::move(rhs.id_);
    owner_address_ = std::move(rhs.owner_address_);
    SubReference(rhs.id_);
    AddReference(rhs.id_);
  }

  std::string id_;
  std::string owner_address_;
};

/// Represents an object in the object store..
/// \param T The type of object.
template <typename T>
class ObjectRef : public ObjectRefBase {
 public:
  ObjectRef() = default;

  ObjectRef(ObjectRef &&rhs) : ObjectRefBase(std::move(rhs)) {}

  ObjectRef &operator=(ObjectRef &&rhs) {
    ObjectRefBase::operator=(std::move(rhs));
    return *this;
  }

  ObjectRef(const ObjectRef &rhs) : ObjectRefBase(rhs) {}

  ObjectRef &operator=(const ObjectRef &rhs) {
    ObjectRefBase::operator=(rhs);
    return *this;
  }

  ObjectRef(const std::string &id) : ObjectRefBase(id) {}

  /// Get the object from the object store.
  /// This method will be blocked until the object is ready.
  ///
  /// \return shared pointer of the result.
  std::shared_ptr<T> Get() const { return GetFromRuntime(*this); }

  /// Make ObjectRef serializable
  MSGPACK_DEFINE(id_, owner_address_);
};

// ---------- implementation ----------
template <typename T>
inline static std::shared_ptr<T> GetFromRuntime(const ObjectRef<T> &object) {
  auto packed_object = internal::GetRayRuntime()->Get(object.ID());
  CheckResult(packed_object);

  return ray::internal::Serializer::Deserialize<std::shared_ptr<T>>(
      packed_object->data(), packed_object->size());
}

template <>
class ObjectRef<void> : public ObjectRefBase {
 public:
  ObjectRef() = default;

  ObjectRef(ObjectRef &&rhs) : ObjectRefBase(std::move(rhs)) {}

  ObjectRef &operator=(ObjectRef &&rhs) {
    ObjectRefBase::operator=(std::move(rhs));
    return *this;
  }

  ObjectRef(const ObjectRef &rhs) : ObjectRefBase(rhs) {}

  ObjectRef &operator=(const ObjectRef &rhs) {
    ObjectRefBase::operator=(rhs);
    return *this;
  }

  ObjectRef(const std::string &id) : ObjectRefBase(id) {}

  /// Get the object from the object store.
  /// This method will be blocked until the object is ready.
  ///
  /// \return shared pointer of the result.
  void Get() const {
    auto packed_object = internal::GetRayRuntime()->Get(id_);
    CheckResult(packed_object);
  }

  /// Make ObjectRef serializable
  MSGPACK_DEFINE(id_, owner_address_);
};

}  // namespace ray