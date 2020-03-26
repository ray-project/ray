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

#include "ray/util/io_service_pool.h"
#include "ray/util/logging.h"

namespace ray {

IOServicePool::IOServicePool(size_t io_service_num) : io_service_num_(io_service_num) {}

IOServicePool::~IOServicePool() {}

void IOServicePool::Run() {
  for (size_t i = 0; i < io_service_num_; ++i) {
    io_services_.emplace_back(
        std::unique_ptr<boost::asio::io_service>(new boost::asio::io_service));
    boost::asio::io_service &io_service = *io_services_[i];
    threads_.emplace_back([&io_service] {
      boost::asio::io_service::work work(io_service);
      io_service.run();
    });
  }

  RAY_LOG(INFO) << "IOServicePool is running with " << io_service_num_ << " io_service.";
}

void IOServicePool::Stop() {
  for (auto &io_service : io_services_) {
    io_service->stop();
  }

  for (auto &thread : threads_) {
    thread.join();
  }

  RAY_LOG(INFO) << "IOServicePool is stopped.";
}

}  // namespace ray
