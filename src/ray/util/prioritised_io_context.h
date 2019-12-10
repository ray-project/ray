#pragma once

#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <queue>
#include "logging.h"

namespace ray {
enum Priority { HIGH, LOW, COUNT };

class HandlerPriorityQueue : public boost::asio::execution_context {
 public:
  void add(Priority priority, boost::function<void()> function) {
    handlers_[priority].push(QueuedHandler(priority, std::move(function)));
  }

  void execute_all() {
    for (int i = 0; i < Priority::COUNT; ++i) {
      while (!handlers_[i].empty()) {
        QueuedHandler handler = handlers_[i].front();
        handler.execute();
        handlers_[i].pop();
      }
    }
  }

  class Executor {
   public:
    Executor(HandlerPriorityQueue &q, Priority p) : context_(q), priority_(p) {}

    HandlerPriorityQueue &context() const { return context_; }

    template <typename Function, typename Allocator>
    void dispatch(const Function &f, const Allocator &) const {
      context_.add(priority_, f);
    }

    template <typename Function, typename Allocator>
    void post(const Function &f, const Allocator &) const {
      context_.add(priority_, f);
    }

    template <typename Function, typename Allocator>
    void defer(const Function &f, const Allocator &) const {
      context_.add(priority_, f);
    }

    void on_work_started() const {}
    void on_work_finished() const {}

    bool operator==(const Executor &other) const {
      return &context_ == &other.context_ && priority_ == other.priority_;
    }

    bool operator!=(const Executor &other) const { return !operator==(other); }

   private:
    HandlerPriorityQueue &context_;
    Priority priority_;
  };

  template <typename Handler>
  boost::asio::executor_binder<Handler, Executor> wrap(Priority priority,
                                                       Handler handler) {
    return boost::asio::bind_executor(Executor(*this, priority), handler);
  }

 private:
  class QueuedHandler {
   public:
    QueuedHandler(Priority p, boost::function<void()> &&f)
        : priority_(p), function_(std::move(f)) {}

    void execute() { function_(); }

    Priority priority() const { return priority_; }

   private:
    Priority priority_;
    boost::function<void()> function_;
  };

  std::array<std::queue<QueuedHandler>, Priority::COUNT> handlers_;
};

class IOContext {
 public:
  ~IOContext() {
    stop();
    std::lock_guard<std::mutex> lock(mutex_);
  }

  template <typename Handler>
  bool post(Handler handler, Priority priority = Priority::LOW) {
    RAY_CHECK(priority >= Priority::HIGH && priority <= Priority::LOW)
        << "Invalid priority " << priority;
    boost::asio::post(io_context_, pri_queue_.wrap(priority, handler));
  }

  void run() {
    std::lock_guard<std::mutex> lock(mutex_);
    boost::asio::io_context::work worker(io_context_);
    while (io_context_.run_one()) {
      // The custom invocation hook adds the handlers to the priority queue
      // rather than executing them from within the poll_one() call.
      while (io_context_.poll_one())
        ;
      pri_queue_.execute_all();
    }
  }

  void stop() { io_context_.stop(); }

  template <typename Handler>
  void async_wait_timer(boost::asio::deadline_timer &timer, Handler handler,
                        Priority priority = Priority::LOW) {
    RAY_CHECK(priority >= Priority::HIGH && priority <= Priority::LOW)
        << "Invalid priority " << priority;
    timer.async_wait(pri_queue_.wrap(priority, handler));
  }

  boost::asio::io_context &io_context() { return io_context_; }

 private:
  boost::asio::io_context io_context_;
  HandlerPriorityQueue pri_queue_;
  std::mutex mutex_;
};

}  // namespace ray
