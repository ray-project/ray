/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>

namespace tensorpipe {

// Base class for actual errors.
class BaseError {
 public:
  virtual ~BaseError() = default;

  // Returns an explanatory string.
  // Like `std::exception` but returns a `std::string`.
  virtual std::string what() const = 0;
};

// Wrapper class for errors.
//
// Background: we wish to not use exceptions yet need an error
// representation that can propagate across function and thread
// boundaries. This representation must be copyable (so we can store
// and return it at a later point in time) and retain downstream type
// information. This implies a heap allocation because it's the
// easiest way to deal with variable size objects (barring a union of
// all downstream error classes and a lot of custom code). Instead of
// passing a shared_ptr around directly, we use this wrapper class to
// keep implementation details hidden from calling code.
//
class Error final {
 public:
  // Constant instance that indicates success.
  static const Error kSuccess;

  // Default constructor for error that is not an error.
  Error() {}

  Error(std::shared_ptr<BaseError> error, std::string file, int line)
      : error_(std::move(error)), file_(std::move(file)), line_(line) {}

  virtual ~Error() = default;

  // Converting to boolean means checking if there is an error. This
  // means we don't need to use an `std::optional` and allows for a
  // snippet like the following:
  //
  //   if (error) {
  //     // Deal with it.
  //   }
  //
  operator bool() const {
    return static_cast<bool>(error_);
  }

  template <typename T>
  std::shared_ptr<T> castToType() const {
    return std::dynamic_pointer_cast<T>(error_);
  }

  template <typename T>
  bool isOfType() const {
    return castToType<T>() != nullptr;
  }

  // Like `std::exception` but returns a `std::string`.
  std::string what() const;

 private:
  std::shared_ptr<BaseError> error_;
  std::string file_;
  int line_;
};

class SystemError final : public BaseError {
 public:
  explicit SystemError(const char* syscall, int error)
      : syscall_(syscall), error_(error) {}

  std::string what() const override;

  int errorCode() const;

 private:
  const char* syscall_;
  const int error_;
};

class ShortReadError final : public BaseError {
 public:
  ShortReadError(ssize_t expected, ssize_t actual)
      : expected_(expected), actual_(actual) {}

  std::string what() const override;

 private:
  const ssize_t expected_;
  const ssize_t actual_;
};

class ShortWriteError final : public BaseError {
 public:
  ShortWriteError(ssize_t expected, ssize_t actual)
      : expected_(expected), actual_(actual) {}

  std::string what() const override;

 private:
  const ssize_t expected_;
  const ssize_t actual_;
};

class EOFError final : public BaseError {
 public:
  EOFError() {}

  std::string what() const override;
};

} // namespace tensorpipe
