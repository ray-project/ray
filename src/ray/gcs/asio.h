// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Adapted from https://github.com/ryangraham/hiredis-boostasio-adapter
// (Copyright 2018 Ryan Graham)

#ifndef RAY_GCS_ASIO_H
#define RAY_GCS_ASIO_H

#include <stdio.h>
#include <iostream>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "ray/gcs/redis_async_context.h"

class RedisAsioClient {
 public:
  /// Constructor of RedisAsioClient.
  /// Use single-threaded io_service as event loop (because the redis commands
  /// that will run in the event loop are non-thread safe).
  ///
  /// \param io_service The single-threaded event loop for this client.
  /// \param redis_async_context The redis async context used to execute redis commands
  /// for this client.
  RedisAsioClient(boost::asio::io_service &io_service,
                  ray::gcs::RedisAsyncContext &redis_async_context);

  void operate();

  void handle_read(boost::system::error_code ec);
  void handle_write(boost::system::error_code ec);
  void add_read();
  void del_read();
  void add_write();
  void del_write();
  void cleanup();

 private:
  ray::gcs::RedisAsyncContext &redis_async_context_;

  boost::asio::io_service &io_service_;
  boost::asio::ip::tcp::socket socket_;
  // Hiredis wanted to add a read operation to the event loop
  // but the read might not have happened yet
  bool read_requested_;
  // Hiredis wanted to add a write operation to the event loop
  // but the read might not have happened yet
  bool write_requested_;
  // A read is currently in progress
  bool read_in_progress_;
  // A write is currently in progress
  bool write_in_progress_;
};

// C wrappers for class member functions
extern "C" void call_C_addRead(void *private_data);
extern "C" void call_C_delRead(void *private_data);
extern "C" void call_C_addWrite(void *private_data);
extern "C" void call_C_delWrite(void *private_data);
extern "C" void call_C_cleanup(void *private_data);

#endif  // RAY_GCS_ASIO_H
