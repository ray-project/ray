/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/common/device.h>

namespace tensorpipe {

struct CpuBuffer {
  void* ptr{nullptr};

  Device getDevice() const {
    return Device{kCpuDeviceType, 0};
  }
};

} // namespace tensorpipe
