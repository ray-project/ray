
#pragma once

#include <memory>
#include <utility>

#include <ray/api/uniqueId.h>
#include <msgpack.hpp>

namespace ray {

template <typename T>
class RayObject {
 public:
  RayObject();

  RayObject(const UniqueId &id);

  RayObject(const UniqueId &&id);

  void assign(const UniqueId &id);

  void assign(UniqueId &&id);

  const UniqueId &id() const;

  std::shared_ptr<T> get() const;

  bool operator==(const RayObject<T> &object) const;

  MSGPACK_DEFINE(_id);

 private:
  UniqueId _id;

  template <typename TO>
  std::shared_ptr<TO> doGet() const;
};

}  // namespace ray

// ---------- implementation ----------
#include <ray/api.h>

namespace ray {

template <typename T>
RayObject<T>::RayObject() {}

template <typename T>
RayObject<T>::RayObject(const UniqueId &id) {
  _id = id;
}

template <typename T>
RayObject<T>::RayObject(const UniqueId &&id) {
  _id = std::move(id);
}

template <typename T>
void RayObject<T>::assign(const UniqueId &id) {
  _id = id;
}

template <typename T>
void RayObject<T>::assign(UniqueId &&id) {
  _id = std::move(id);
}

template <typename T>
const UniqueId &RayObject<T>::id() const {
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

}  // namespace ray