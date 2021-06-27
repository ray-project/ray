// Copyright 2021 The Google Research Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <new>
#include <string>
#include <type_traits>
#include <utility>

#include "ray/common/status.h"

namespace ray {

template <typename T>
class StatusOr {
  template <typename U>
  friend class StatusOr;

 public:
  // Construct a new StatusOr with Status::UNKNOWN status
  StatusOr();

  // Construct a new StatusOr with the given non-ok status. After calling
  // this constructor, calls to value() will crash.
  //
  // NOTE: Not explicit - we want to use StatusOr<T> as a return
  // value, so it is convenient and sensible to be able to do 'return
  // Status()' when the return type is StatusOr<T>.
  //
  // REQUIRES: status != StatusCode::kOk. This requirement is RAY_CHECKed.
  StatusOr(const Status &status);

  // Construct a new StatusOr with the given value. If T is a plain pointer,
  // value must not be NULL. After calling this constructor, calls to
  // ValueOrDie() will succeed, and calls to status() will return OK.
  //
  // NOTE: Not explicit - we want to use StatusOr<T> as a return type
  // so it is convenient and sensible to be able to do 'return T()'
  // when when the return type is StatusOr<T>.
  //
  // REQUIRES: if T is a plain pointer, value != nullptr. This requirement is
  // RAY_CHECKED.
  StatusOr(const T &value);

  // Copy constructor.
  StatusOr(const StatusOr &other) = default;

  // Conversion copy constructor, T must be copy constructible from U
  template <typename U>
  StatusOr(const StatusOr<U> &other);

  // Assignment operator.
  StatusOr &operator=(const StatusOr &other) = default;

  // Conversion assignment operator, T must be assignable from U
  template <typename U>
  StatusOr &operator=(const StatusOr<U> &other);

  // Move constructor and move-assignment operator.
  StatusOr(StatusOr &&other) = default;
  StatusOr &operator=(StatusOr &&other) = default;

  // Rvalue-reference overloads of the other constructors and assignment
  // operators, to support move-only types and avoid unnecessary copying.
  StatusOr(T &&value);
  template <typename U>
  StatusOr(StatusOr<U> &&other);
  template <typename U>
  StatusOr &operator=(StatusOr<U> &&other);

  // Returns a reference to our status. If this contains a T, then
  // returns StatusCode::kOk.
  const Status &status() const;

  // Returns this->status().ok()
  bool ok() const;

  // StatusOr<T>:: operator*()
  //
  // Returns a reference to the current value.
  //
  // REQUIRES: `this->ok() == true`, otherwise it crashes.
  const T &value() const &;
  T &value() &;
  const T &&value() const &&;
  T &&value() &&;

  // StatusOr<T>:: operator*()
  //
  // Returns a pointer to the current value.
  //
  // REQUIRES: `this->ok() == true`, otherwise it crashes.
  const T &operator*() const &;
  T &operator*() &;
  const T &&operator*() const &&;
  T &&operator*() &&;

  // StatusOr<T>::operator->()
  //
  // Returns a pointer to the current value.
  //
  // REQUIRES: `this->ok() == true`, otherwise it crashes.
  const T *operator->() const;
  T *operator->();

 private:
  void EnsureOK() const;
  Status status_;
  T value_;
};

////////////////////////////////////////////////////////////////////////////////
// Implementation details for StatusOr<T>

namespace internal {

class StatusOrHelper {
 public:
  // Customized behavior for StatusOr<T> vs. StatusOr<T*>
  template <typename T>
  struct Specialize;
};

template <typename T>
struct StatusOrHelper::Specialize {
  // For non-pointer T, a reference can never be NULL.
  static inline bool IsValueNull(const T &t) { return false; }
};

template <typename T>
struct StatusOrHelper::Specialize<T *> {
  static inline bool IsValueNull(const T *t) { return t == nullptr; }
};

}  // namespace internal

template <typename T>
inline StatusOr<T>::StatusOr() : status_(Status::UnknownError("")), value_() {}

template <typename T>
inline StatusOr<T>::StatusOr(const Status &status) : status_(status), value_() {
  RAY_CHECK(!status_.ok())
      << "Status::OK is not a valid constructor argument to StatusOr<T>";
}

template <typename T>
inline StatusOr<T>::StatusOr(const T &value) : status_(Status::OK()), value_(value) {
  RAY_CHECK(!internal::StatusOrHelper::Specialize<T>::IsValueNull(value_))
      << "nullptr is not a valid constructor argument to StatusOr<T*>";
}

template <typename T>
template <typename U>
inline StatusOr<T>::StatusOr(const StatusOr<U> &other)
    : status_(other.status_), value_(other.value_) {}

template <typename T>
template <typename U>
inline StatusOr<T> &StatusOr<T>::operator=(const StatusOr<U> &other) {
  status_ = other.status_;
  value_ = other.value_;
  return *this;
}

template <typename T>
inline StatusOr<T>::StatusOr(T &&value)
    : status_(Status::OK()), value_(std::move(value)) {
  RAY_CHECK(!internal::StatusOrHelper::Specialize<T>::IsValueNull(value_))
      << "nullptr is not a valid constructor argument to StatusOr<T*>";
}

template <typename T>
template <typename U>
inline StatusOr<T>::StatusOr(StatusOr<U> &&other)
    : status_(other.status_), value_(std::move(other.value_)) {}

template <typename T>
template <typename U>
inline StatusOr<T> &StatusOr<T>::operator=(StatusOr<U> &&other) {
  status_ = other.status_;
  value_ = std::move(other.value_);
  return *this;
}

template <typename T>
inline const Status &StatusOr<T>::status() const {
  return status_;
}

template <typename T>
inline bool StatusOr<T>::ok() const {
  return status_.ok();
}

template <typename T>
const T &StatusOr<T>::value() const & {
  this->EnsureOK();
  return this->value_;
}

template <typename T>
T &StatusOr<T>::value() & {
  this->EnsureOK();
  return this->value_;
}

template <typename T>
const T &&StatusOr<T>::value() const && {
  this->EnsureOK();
  return std::move(this->value_);
}

template <typename T>
T &&StatusOr<T>::value() && {
  this->EnsureOK();
  return std::move(this->value_);
}

template <typename T>
const T &StatusOr<T>::operator*() const & {
  this->EnsureOK();
  return this->value_;
}

template <typename T>
T &StatusOr<T>::operator*() & {
  this->EnsureOK();
  return this->value_;
}

template <typename T>
const T &&StatusOr<T>::operator*() const && {
  this->EnsureOK();
  return std::move(this->value_);
}

template <typename T>
T &&StatusOr<T>::operator*() && {
  this->EnsureOK();
  return std::move(this->value_);
}

template <typename T>
const T *StatusOr<T>::operator->() const {
  this->EnsureOK();
  return &this->value_;
}

template <typename T>
T *StatusOr<T>::operator->() {
  this->EnsureOK();
  return &this->value_;
}

template <typename T>
void StatusOr<T>::EnsureOK() const {
  RAY_CHECK(status_.ok()) << "Attempting to fetch value instead of handling error status";
}

}  // namespace ray