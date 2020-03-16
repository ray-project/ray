#include "ray/util/io_service_pool.h"

namespace ray {

IOServicePool::IOServicePool(size_t io_service_num) : io_service_num_(io_service_num) {}

void IOServicePool::Run() {
  for (size_t i = 0; i < io_service_num_; ++i) {
    io_services_.emplace_back(std::move(boost::asio::io_service()));
    boost::asio::io_service &io_service = io_services_.back();
    threads_.emplace_back([io_service] {
      boost::asio::io_service::work(io_service);
      io_service.run();
    });
  }
}

void IOServicePool::Stop() {
  for (auto &io_service : io_services_) {
    io_service.stop();
  }

  for (auto &thread : threads_) {
    thread.join();
  }
}

boost::asio::io_service &IOServicePool::Get() {
  size_t index = ++current_index_ % io_service_num_;
  return io_services_[index];
}

boost::asio::io_service &IOServicePool::Get(size_t hash) {
  size_t index = hash % io_service_num_;
  return io_services_[index];
}

std::vector<boost::asio::io_service &> IOServicePool::GetAll() {
  std::vector<boost::asio::io_service &> io_services;
  for (auto &io_service : io_services_) {
    io_services.emplace_back(io_service);
  }
  return io_services;
}

}  // namespace ray
