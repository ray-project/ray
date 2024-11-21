/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <tensorpipe/common/error.h>

namespace tensorpipe {

class ContextImpl;
class ListenerImpl;
class Pipe;

// The listener.
//
// Listeners are used to produce pipes. Depending on the type of the
// context, listeners may use a variety of addresses to listen on. For
// example, for TCP/IP sockets they listen on an IPv4 or IPv6 address,
// for Unix domain sockets they listen on a path, etcetera.
//
// A pipe can only be accepted from this listener after it has been
// fully established. This means that both its connection and all its
// side channels have been established.
//
class Listener final {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  Listener(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      const std::vector<std::string>& urls);

  //
  // Entry points for user code
  //

  using accept_callback_fn =
      std::function<void(const Error&, std::shared_ptr<Pipe>)>;

  void accept(accept_callback_fn fn);

  // Returns map with the materialized address of listeners by transport.
  //
  // If you don't bind a transport listener to a specific port or address, it
  // may generate its address automatically. Then, in order to connect to the
  // listener, the user must use a separate mechanism to communicate the
  // materialized address to whoever wants to connect.
  //
  const std::map<std::string, std::string>& addresses() const;

  // Returns materialized address for specific transport.
  //
  // See `addresses()` for more information.
  //
  const std::string& address(const std::string& transport) const;

  // Returns URL with materialized address for specific transport.
  //
  // See `addresses()` for more information.
  //
  std::string url(const std::string& transport) const;

  // Put the listener in a terminal state, aborting its pending operations and
  // rejecting future ones, and release its resrouces. This may be carried out
  // asynchronously, in background. Since the pipes may occasionally use the
  // listener to open new connections, closing a listener may trigger errors
  // in the pipes.
  void close();

  ~Listener();

 private:
  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  const std::shared_ptr<ListenerImpl> impl_;

  // Allow context to access constructor token.
  friend ContextImpl;
};

} // namespace tensorpipe
