/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <tensorpipe/common/error.h>

namespace tensorpipe {
namespace channel {

class ContextClosedError final : public BaseError {
 public:
  ContextClosedError() {}

  std::string what() const override;
};

class ChannelClosedError final : public BaseError {
 public:
  ChannelClosedError() {}

  std::string what() const override;
};

class ContextNotViableError final : public BaseError {
 public:
  ContextNotViableError() {}

  std::string what() const override;
};

} // namespace channel
} // namespace tensorpipe
