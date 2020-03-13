
#pragma once

#include <memory>
#include <utility>

#include <msgpack.hpp>

#include <ray/core.h>

namespace ray {
namespace api {

template <typename T>
class RayObject {
 public:
  RayObject();

  RayObject(const ObjectID &id);

  RayObject(const ObjectID &&id);

  const ObjectID &ID() const;

  std::shared_ptr<T> Get() const;

  bool operator==(const RayObject<T> &object) const;

  MSGPACK_DEFINE(_id);

 private:
  ObjectID _id;

  template <typename TO>
  std::shared_ptr<TO> DoGet() const;
};

}  // namespace api
}  // namespace ray

// ---------- implementation ----------
#include <ray/api.h>

namespace ray {
namespace api {

template <typename T>
RayObject<T>::RayObject() {}

template <typename T>
RayObject<T>::RayObject(const ObjectID &id) {
  _id = id;
}

template <typename T>
RayObject<T>::RayObject(const ObjectID &&id) {
  _id = std::move(id);
}

template <typename T>
const ObjectID &RayObject<T>::ID() const {
  return _id;
}

template <typename T>
inline std::shared_ptr<T> RayObject<T>::Get() const {
  return DoGet<T>();
}

template <typename T>
template <typename TO>
inline std::shared_ptr<TO> RayObject<T>::DoGet() const {
  return Ray::Get(*this);
}

template <typename T>
inline bool RayObject<T>::operator==(const RayObject<T> &object) const {
  if (_id == object.ID()) {
    return true;
  } else {
    return false;
  }
}

}  // namespace api
}  // namespace ray