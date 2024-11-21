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
#include <utility>
#include <vector>

#include <tensorpipe/transport/context.h>

#include <tensorpipe/channel/context.h>

namespace tensorpipe {

class ContextImpl;
class Listener;
class Pipe;

class ContextOptions {
 public:
  // The name should be a semantically meaningful description of this context.
  // It will only be used for logging and debugging purposes, to identify the
  // endpoints of a pipe.
  ContextOptions&& name(std::string name) && {
    name_ = std::move(name);
    return std::move(*this);
  }

 private:
  std::string name_;

  friend ContextImpl;
};

class PipeOptions {
 public:
  // The name should be a semantically meaningful description of the context
  // that the pipe is connecting to. It will only be used for logging and
  // debugging purposes, to identify the endpoints of a pipe.
  PipeOptions&& remoteName(std::string remoteName) && {
    remoteName_ = std::move(remoteName);
    return std::move(*this);
  }

 private:
  std::string remoteName_;

  friend ContextImpl;
};

class Context final {
 public:
  explicit Context(ContextOptions opts = ContextOptions());

  void registerTransport(
      int64_t priority,
      std::string transport,
      std::shared_ptr<transport::Context> context);

  void registerChannel(
      int64_t priority,
      std::string channel,
      std::shared_ptr<channel::Context> context);

  std::shared_ptr<Listener> listen(const std::vector<std::string>& urls);

  std::shared_ptr<Pipe> connect(
      const std::string& url,
      PipeOptions opts = PipeOptions());

  // Put the context in a terminal state, in turn closing all of its pipes and
  // listeners, and release its resources. This may be done asynchronously, in
  // background.
  void close();

  // Wait for all resources to be released and all background activity to stop.
  void join();

  ~Context();

 private:
  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it. However, its lifetime is tied to the one
  // of this public object since when the latter is destroyed the implementation
  // is closed and joined.
  const std::shared_ptr<ContextImpl> impl_;
};

} // namespace tensorpipe
