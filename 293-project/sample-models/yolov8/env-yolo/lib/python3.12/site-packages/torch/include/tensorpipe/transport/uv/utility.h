/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <tuple>

#include <sys/socket.h>

#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::tuple<Error, std::string> lookupAddrForIface(std::string iface);

std::tuple<Error, std::string> lookupAddrForHostname();

// Try to replicate the same logic used by NCCL to find a node's own address.
// Roughly, it returns the "first" usable address it can find, and prioritizes
// the interfaces with an `ib` prefix and de-prioritizes those with a `docker`
// or `lo` prefix. It can optionally only return only IPv4 or IPv4 addresses.
std::tuple<Error, std::string> lookupAddrLikeNccl(
    optional<sa_family_t> familyFilter = nullopt);

} // namespace uv
} // namespace transport
} // namespace tensorpipe
