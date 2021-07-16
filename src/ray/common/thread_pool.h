#pragma once
#include <type_traits>
#include <memory>
#include <boost/asio.hpp>
#include <boost/fiber/all.hpp>
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio_round_robin.h"

namespace ray {
namespace thread_pool {

namespace {

template <typename F,
          typename R = typename std::result_of<F()>::type,
          typename std::enable_if<!std::is_same<R, void>::value, int>::type = 0>
void _run_job(F&& f, boost::fibers::promise<R>* p) {
  p->set_value(f());
}

template <typename F,
          typename R = typename std::result_of<F()>::type,
          typename std::enable_if<std::is_same<R, void>::value, int>::type = 0>
void _run_job(F&& f, boost::fibers::promise<R>* p) {
  f();
  p->set_value();
}

}



class CPUThreadPool {
 public:
  CPUThreadPool(size_t n) : pool_(n) {}
  template <typename F>
  auto post(F &&f) {
    auto p = std::make_unique<boost::fibers::promise<decltype(f())>>().release();
    auto future = p->get_future();
    boost::asio::post(pool_,
                      [f = std::move(f), p = p] {
                        _run_job(std::move(f), p);
                        delete p;
                      });
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
    auto p = std::make_unique<boost::fibers::promise<decltype(f())>>().release();
    auto future = p->get_future();
    io_service_->post([f = std::move(f), p] {
      boost::fibers::fiber co([f = std::move(f), p]() {
        _run_job(std::move(f), p);
        delete p;
      });
      co.join();
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
  return _io_pool.post(std::bind(std::move(f), std::forward(args)...));
}

template <typename F, typename... Ts>
auto cpu_post(F &&f, Ts &&... args) {
  return _cpu_pool.post(std::bind(std::move(f), std::forward(args)...));
}

}  // namespace thread_pool
}  // namespace ray
