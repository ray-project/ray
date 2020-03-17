#include "ray/util/io_service_pool.h"

namespace ray {

IOServicePool::IOServicePool(size_t io_service_num) : io_service_num_(io_service_num) {}

IOServicePool::~IOServicePool() {}

void IOServicePool::Run() {
  for (size_t i = 0; i < io_service_num_; ++i) {
    boost::asio::io_service *io_service = new boost::asio::io_service;
    io_services_.emplace_back(io_service);
    threads_.emplace_back([io_service] {
      boost::asio::io_service::work *worker =
          new boost::asio::io_service::work(*io_service);
      io_service->run();
      delete worker;
    });
  }
}

void IOServicePool::Stop() {
  for (auto io_service : io_services_) {
    io_service->stop();
  }

  for (auto &thread : threads_) {
    thread.join();
  }
  threads_.clear();

  for (auto io_service : io_services_) {
    delete io_service;
  }
  io_services_.clear();
}

}  // namespace ray
