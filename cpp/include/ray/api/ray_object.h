
#pragma once

#include <memory>
#include <utility>

#include <msgpack.hpp>

#include <ray/core.h>

namespace ray { namespace api {

template <typename T>
class RayObject {
 public:
  RayObject();

  RayObject(const ObjectID &id);

  RayObject(const ObjectID &&id);

  void assign(const ObjectID &id);

  void assign(ObjectID &&id);

  const ObjectID &id() const;

  std::shared_ptr<T> get() const;

  bool operator==(const RayObject<T> &object) const;

  MSGPACK_DEFINE(_id);

 private:
  ObjectID _id;

  template <typename TO>
  std::shared_ptr<TO> doGet() const;
};

}  }// namespace ray::api

// ---------- implementation ----------
#include <ray/api.h>

namespace ray { namespace api {

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
void RayObject<T>::assign(const ObjectID &id) {
  _id = id;
}

template <typename T>
void RayObject<T>::assign(ObjectID &&id) {
  _id = std::move(id);
}

template <typename T>
const ObjectID &RayObject<T>::id() const {
  return _id;
}

template <typename T>
inline std::shared_ptr<T> RayObject<T>::get() const {
  return doGet<T>();
}

template <typename T>
template <typename TO>
inline std::shared_ptr<TO> RayObject<T>::doGet() const {
  return Ray::get(*this);
}

template <typename T>
inline bool RayObject<T>::operator==(const RayObject<T> &object) const {
  if (_id == object.id()) {
    return true;
  } else {
    return false;
  }
}

}  }// namespace ray::api