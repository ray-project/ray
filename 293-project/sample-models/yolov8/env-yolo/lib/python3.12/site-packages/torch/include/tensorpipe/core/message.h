/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include <tensorpipe/common/buffer.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {

// Messages consist of a primary buffer and zero or more separate
// buffers. The primary buffer is always a host-side memory region that
// contains a serialized version of the message we're dealing with. This
// serialized message, in turn, may have references to the separate
// buffers that accompany the primary buffer. These separate buffers may
// point to any type of memory, host-side or device-side.
//
class Message final {
 public:
  std::string metadata;

  struct Payload {
    void* data{nullptr};
    size_t length{0};

    // Users may include arbitrary metadata in the following fields.
    // This may contain allocation hints for the receiver, for example.
    std::string metadata;
  };

  // Holds the payloads that are transferred over the primary connection.
  std::vector<Payload> payloads;

  struct Tensor {
    tensorpipe::Buffer buffer;
    size_t length{0};

    // Users may optionally specify the target device, on which the receiver
    // should allocate memory for this tensor. If left unset, the receiver will
    // choose one at their convenience.
    optional<Device> targetDevice;

    // Users may include arbitrary metadata in the following field.
    // This may contain allocation hints for the receiver, for example.
    std::string metadata;
  };

  // Holds the tensors that are offered to the side channels.
  std::vector<Tensor> tensors;
};

// Descriptors consist of metadata required by the receiver to allocate memory
// for an incoming message.
class Descriptor final {
 public:
  std::string metadata;

  struct Payload {
    size_t length{0};
    std::string metadata;
  };
  std::vector<Payload> payloads;

  struct Tensor {
    size_t length{0};

    // This is the sender-side device from which this tensor is being sent.
    Device sourceDevice;

    // The sender may optionally specify a target device, in which case the
    // receiver must allocate memory for this tensor on the specified device.
    optional<Device> targetDevice;

    std::string metadata;
  };
  std::vector<Tensor> tensors;
};

// Allocations consist of actual memory allocations provided by the receiver for
// an incoming message. They must match the length and target devices specified
// in the corresponding Descriptor.
class Allocation final {
 public:
  struct Payload {
    void* data{nullptr};
  };
  std::vector<Payload> payloads;

  struct Tensor {
    tensorpipe::Buffer buffer;
  };
  std::vector<Tensor> tensors;
};

} // namespace tensorpipe
