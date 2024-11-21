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
namespace transport {

class Connection;
class Listener;

class Context {
 public:
  virtual std::shared_ptr<Connection> connect(std::string addr) = 0;

  virtual std::shared_ptr<Listener> listen(std::string addr) = 0;

  // Return whether the context is able to operate correctly.
  //
  // Some transport types may be unable to perform as intended under
  // some circumstances (e.g., specialized hardware unavailable, lack
  // of permissions). They can report it through this method in order
  // for the core context to avoid registering them in the first place.
  //
  virtual bool isViable() const = 0;

  // Return string to describe the domain for this context.
  //
  // Two processes with a context of the same type can connect to each
  // other if one side's domain descriptor is "accepted" by the other
  // one, using the canCommunicateWithRemote method below. That method
  // must be symmetric, and unless overridden defaults to string
  // comparison.
  //
  // For example, for a transport that leverages TCP/IP, this may be
  // as simple as the address family (assuming we can route between
  // any two processes). For a transport that leverages shared memory,
  // this descriptor must uniquely identify the machine, such that
  // only co-located processes generate the same domain descriptor.
  //
  virtual const std::string& domainDescriptor() const = 0;

  // Compare local and remote domain descriptor for compatibility.
  //
  // Determine whether a connection can be opened between this context
  // and a remote one that has the given domain descriptor. This
  // function needs to be symmetric: if we called this method on the
  // remote context with the local descriptor we should get the same
  // answer. Unless overridden it defaults to string comparison.
  //
  virtual bool canCommunicateWithRemote(
      const std::string& remoteDomainDescriptor) const {
    return domainDescriptor() == remoteDomainDescriptor;
  }

  // Tell the context what its identifier is.
  //
  // This is only supposed to be called from the high-level context or from
  // channel contexts. It will only used for logging and debugging purposes.
  virtual void setId(std::string id) = 0;

  virtual void close() = 0;

  virtual void join() = 0;

  virtual ~Context() = default;
};

} // namespace transport
} // namespace tensorpipe
