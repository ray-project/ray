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

#ifndef RAY_UTIL_IO_SERVICE_POOL_H
#define RAY_UTIL_IO_SERVICE_POOL_H

#include <atomic>
#include <boost/asio.hpp>
#include <thread>

namespace ray {

/// \class IOServicePool
/// The io_service pool. Each io_service owns a thread.
/// To get io_service from this pool should call `Run()` first.
/// Before exit, `Stop()` must be called.
class IOServicePool {
 public:
  IOServicePool(size_t io_service_num);

  ~IOServicePool();

  void Run();

  void Stop();

  /// Select io_service by round robin.
  ///
  /// \return io_service
  boost::asio::io_service *Get();

  /// Select io_service by hash.
  ///
  /// \param hash Use this hash to pick a io_service.
  /// The same hash will alway get the same io_service.
  /// \return io_service
  boost::asio::io_service *Get(size_t hash);

  /// Get all io_service.
  /// This is only use for RedisClient::Connect().
  std::vector<boost::asio::io_service *> GetAll();

 private:
  size_t io_service_num_{0};

  std::vector<std::thread> threads_;
  std::vector<std::unique_ptr<boost::asio::io_service>> io_services_;

  std::atomic<size_t> current_index_;
};

inline boost::asio::io_service *IOServicePool::Get() {
  size_t index = ++current_index_ % io_service_num_;
  return io_services_[index].get();
}

inline boost::asio::io_service *IOServicePool::Get(size_t hash) {
  size_t index = hash % io_service_num_;
  return io_services_[index].get();
}

inline std::vector<boost::asio::io_service *> IOServicePool::GetAll() {
  std::vector<boost::asio::io_service *> io_services;
  for (auto &io_service : io_services_) {
    io_services.emplace_back(io_service.get());
  }
  return io_services;
}

}  // namespace ray

#endif  // RAY_UTIL_IO_SERVICE_POOL_H
