// Copyright 2022 The Ray Authors.
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

#pragma once

#include <boost/fiber/all.hpp>
#include <thread>

namespace ray {
namespace executor {

using namespace boost;

class ITask {
 public:
  virtual void run() = 0;
  virtual ~ITask() {}
};

template<typename F, typename ... Args>
class Task : public ITask {
 public:
  using R = std::invoke_result_t<F, Args...>;

  Task(F&& func, Args&&... args)
      : task(std::bind(std::forward<F>(func), std::forward<Args>(args)...)) {

  }

  void run() override {
    task();
  }
  
  boost::fibers::future<R> get_future() {
    return task.get_future();
  }
 private:
  
  boost::fibers::packaged_task<R()> task;
};

class Executor {
 public:
  explicit Executor(size_t channel_size = 1024, size_t num_thread = 1)
      : channel_(channel_size) {
    for(size_t i = 0; i < num_thread; ++i) {
      threads_.emplace_back(std::make_unique<std::thread>(&Executor::run, this));
    }
  }

  ~Executor() {
    channel_.close();
    for(auto& t : threads_) {
      t->join();
    }
  }

  template<typename F, typename ...Args>
  auto submit(F&& func, Args&&... args) {
    auto task = std::make_unique<Task<F, Args...>>(std::forward<F>(func), std::forward<Args>(args)...);
    auto future = task->get_future();
    channel_.push(std::move(task));
    return future;
  }
  
 private:
  const size_t puller_fibers_ = 16;

  void run() {
    auto puller = [this]() {
      std::unique_ptr<ITask> task ;
      while (boost::fibers::channel_op_status::closed != channel_.pop(task)){
        fibers::fiber([task = std::move(task)] () mutable {
          task->run();
        }).detach();
      }
    };
    
    for(size_t i = 0; i != puller_fibers_ - 1; ++i) {
      fibers::fiber(puller).detach();
    }
    puller();
  }
  
  fibers::buffered_channel<std::unique_ptr<ITask>> channel_;
  std::vector<std::unique_ptr<std::thread>> threads_;
};
}
}
