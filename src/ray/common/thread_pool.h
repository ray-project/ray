#pragma once

#include <boost/asio.hpp>
#include <boost/fiber/all.hpp>
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio_round_robin.h"

namespace ray {
namespace thread_pool {

class CPUThreadPool {
 public:
  CPUThreadPool(size_t n) : pool_(n) {}
  template <typename F>
  auto post(F &&f) {
    using R = decltype(f());
    boost::fibers::promise<R> promise;
    auto future = promise.get_future();
    boost::asio::post(pool_,
                      [f = std::move(f), p = std::move(promise)] { p.set_value(f()); });
    return future;
  }
  ~CPUThreadPool() { pool_.join(); }

 private:
  boost::asio::thread_pool pool_;
};

class IOThreadPool {
 public:
  IOThreadPool() : io_service_(std::make_shared<instrumented_io_context>()) {
    boost::fibers::use_scheduling_algorithm<boost::fibers::asio::round_robin>(
        io_service_);
  }

  template <typename F>
  auto post(F &&f) {
    using R = decltype(f());
    boost::fibers::promise<R> promise;
    auto future = promise.get_future();
    io_service_->post([f = std::move(f), p = std::move(promise)]() {
      boost::fibers::fiber _(boost::fibers::launch::dispatch, [&f, &p] () mutable {
        p.set_value(f());
      });
      _.join();
    });
    return future;
  }
  instrumented_io_context &GetIOService() { return *io_service_; }

  bool stopped() {
    return io_service_->stopped();
  }

 private:
  std::shared_ptr<instrumented_io_context> io_service_;
};

extern CPUThreadPool _cpu_pool;
extern IOThreadPool _io_pool;

template <typename F, typename... Ts>
auto io_post(F &&f, Ts &&... args) {
  return _io_pool.post(std::move(f)(std::forward(args)...));
}

template <typename F, typename... Ts>
auto cpu_post(F &&f, Ts &&... args) {
  return _cpu_pool.post(std::move(f)(std::forward(args)...));
}

}  // namespace thread_pool
}  // namespace ray
