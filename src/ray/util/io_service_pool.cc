#include "ray/util/io_service_pool.h"
#include "ray/util/logging.h"

namespace ray {

IOServicePool::IOServicePool(size_t io_service_num) : io_service_num_(io_service_num) {}

IOServicePool::~IOServicePool() {
  for (auto &thread : threads_) {
    delete thread;
  }
  threads_.clear();

  for (auto io_service : io_services_) {
    delete io_service;
  }
  io_services_.clear();
}

void IOServicePool::Run() {
  for (size_t i = 0; i < io_service_num_; ++i) {
    boost::asio::io_service *io_service = new boost::asio::io_service;
    io_services_.emplace_back(io_service);

    threads_.emplace_back(new std::thread([io_service] {
      boost::asio::io_service::work work(*io_service);
      io_service->run();
    }));
  }

  RAY_LOG(INFO) << "IOServicePool is running.";
}

void IOServicePool::Stop() {
  for (auto io_service : io_services_) {
    io_service->stop();
  }

  for (auto &thread : threads_) {
    thread->join();
  }

  RAY_LOG(INFO) << "IOServicePool is stopped.";
}

}  // namespace ray
