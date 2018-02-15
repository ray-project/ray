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

#ifndef RAY_GCS_ASIO_H
#define RAY_GCS_ASIO_H

#include "hiredis/hiredis.h"
#include "hiredis/async.h"

#include <iostream>
#include <string>
#include <stdio.h>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

using boost::asio::ip::tcp;

class RedisAsioClient
{
public:
	RedisAsioClient(boost::asio::io_service& io_service,redisAsyncContext *ac);

	void operate();

	void handle_read(boost::system::error_code ec);
	void handle_write(boost::system::error_code ec);
	void add_read();
	void del_read();
	void add_write();
	void del_write();
	void cleanup();

private:
redisAsyncContext *context_;
boost::asio::ip::tcp::socket socket_;
bool read_requested_;
bool write_requested_;
bool read_in_progress_;
bool write_in_progress_;
};


// C wrappers for class member functions
extern "C" void call_C_addRead(void *privdata);
extern "C" void call_C_delRead(void *privdata);
extern "C" void call_C_addWrite(void *privdata);
extern "C" void call_C_delWrite(void *privdata);
extern "C" void call_C_cleanup(void *privdata);


#endif  // RAY_GCS_ASIO_H
