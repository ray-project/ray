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

#include "ray/util/memory.h"

#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace ray {

uint8_t *pointer_logical_and(const uint8_t *address, uintptr_t bits) {
  uintptr_t value = reinterpret_cast<uintptr_t>(address);
  return reinterpret_cast<uint8_t *>(value & bits);
}

namespace {

struct MemcopyTaskCompletion {
  explicit MemcopyTaskCompletion(size_t total) : remaining(total) {}

  std::mutex mutex;
  std::condition_variable cv;
  size_t remaining;
};

struct MemcopyTask {
  std::function<void()> fn;
  std::shared_ptr<MemcopyTaskCompletion> completion;
};

}  // namespace

class ParallelMemcopyThreadPool {
 public:
  explicit ParallelMemcopyThreadPool(int num_threads);
  ~ParallelMemcopyThreadPool();

  void ParallelMemcpy(uint8_t *dst,
                      const uint8_t *src,
                      int64_t nbytes,
                      uintptr_t block_size,
                      int num_threads);

 private:
  void WorkerLoop();
  void Shutdown();

  std::vector<std::thread> workers_;
  std::deque<MemcopyTask> tasks_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stop_ = false;
};

ParallelMemcopyThreadPool::ParallelMemcopyThreadPool(int num_threads) {
  if (num_threads <= 0) {
    return;
  }
  workers_.reserve(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    workers_.emplace_back([this]() { WorkerLoop(); });
  }
}

ParallelMemcopyThreadPool::~ParallelMemcopyThreadPool() { Shutdown(); }

void ParallelMemcopyThreadPool::Shutdown() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

void ParallelMemcopyThreadPool::WorkerLoop() {
  while (true) {
    MemcopyTask task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this]() { return stop_ || !tasks_.empty(); });
      if (stop_ && tasks_.empty()) {
        return;
      }
      task = std::move(tasks_.front());
      tasks_.pop_front();
    }
    task.fn();
    if (task.completion) {
      std::unique_lock<std::mutex> completion_lock(task.completion->mutex);
      if (--task.completion->remaining == 0) {
        task.completion->cv.notify_one();
      }
    }
  }
}

void ParallelMemcopyThreadPool::ParallelMemcpy(uint8_t *dst,
                                               const uint8_t *src,
                                               int64_t nbytes,
                                               uintptr_t block_size,
                                               int num_threads) {
  const size_t available_threads = workers_.size();
  if (available_threads == 0 || num_threads <= 1) {
    std::memcpy(dst, src, nbytes);
    return;
  }

  size_t requested = static_cast<size_t>(num_threads);
  size_t threads_to_use = std::min(available_threads, requested);
  if (threads_to_use <= 1) {
    std::memcpy(dst, src, nbytes);
    return;
  }

  uint8_t *left = pointer_logical_and(src + block_size - 1, ~(block_size - 1));
  uint8_t *right = pointer_logical_and(src + nbytes, ~(block_size - 1));
  int64_t num_blocks = (right - left) / block_size;
  const int64_t thread_count = static_cast<int64_t>(threads_to_use);
  right = right - (num_blocks % thread_count) * block_size;

  int64_t chunk_size = (right - left) / thread_count;
  int64_t prefix = left - src;
  int64_t suffix = src + nbytes - right;

  auto completion = std::make_shared<MemcopyTaskCompletion>(threads_to_use);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t i = 0; i < threads_to_use; ++i) {
      uint8_t *task_dst = dst + prefix + i * chunk_size;
      const uint8_t *task_src = left + i * chunk_size;
      const int64_t copy_size = chunk_size;
      tasks_.push_back({[task_dst, task_src, copy_size]() {
                          std::memcpy(task_dst, task_src, copy_size);
                        },
                        completion});
    }
  }
  cv_.notify_all();

  std::memcpy(dst, src, prefix);
  std::memcpy(dst + prefix + thread_count * chunk_size, right, suffix);

  std::unique_lock<std::mutex> completion_lock(completion->mutex);
  completion->cv.wait(completion_lock,
                      [&completion]() { return completion->remaining == 0; });
}

void parallel_memcopy(uint8_t *dst,
                      const uint8_t *src,
                      int64_t nbytes,
                      uintptr_t block_size,
                      int num_threads) {
  std::vector<std::thread> threadpool(num_threads);
  uint8_t *left = pointer_logical_and(src + block_size - 1, ~(block_size - 1));
  uint8_t *right = pointer_logical_and(src + nbytes, ~(block_size - 1));
  int64_t num_blocks = (right - left) / block_size;

  // Update right address
  right = right - (num_blocks % num_threads) * block_size;

  // Now we divide these blocks between available threads. The remainder is
  // handled on the main thread.
  int64_t chunk_size = (right - left) / num_threads;
  int64_t prefix = left - src;
  int64_t suffix = src + nbytes - right;
  // Now the data layout is | prefix | k * num_threads * block_size | suffix |.
  // We have chunk_size = k * block_size, therefore the data layout is
  // | prefix | num_threads * chunk_size | suffix |.
  // Each thread gets a "chunk" of k blocks.

  // Start all threads first and handle leftovers while threads run.
  for (int i = 0; i < num_threads; i++) {
    threadpool[i] = std::thread(
        std::memcpy, dst + prefix + i * chunk_size, left + i * chunk_size, chunk_size);
  }

  std::memcpy(dst, src, prefix);
  std::memcpy(dst + prefix + num_threads * chunk_size, right, suffix);

  for (auto &t : threadpool) {
    if (t.joinable()) {
      t.join();
    }
  }
}

ParallelMemcopyThreadPool *CreateParallelMemcopyThreadPool(int num_threads) {
  if (num_threads <= 1) {
    return nullptr;
  }
  return new ParallelMemcopyThreadPool(num_threads);
}

void DestroyParallelMemcopyThreadPool(ParallelMemcopyThreadPool *pool) { delete pool; }

void parallel_memcopy_with_pool(ParallelMemcopyThreadPool *pool,
                                uint8_t *dst,
                                const uint8_t *src,
                                int64_t nbytes,
                                uintptr_t block_size,
                                int num_threads) {
  if (pool == nullptr) {
    parallel_memcopy(dst, src, nbytes, block_size, num_threads);
    return;
  }
  pool->ParallelMemcpy(dst, src, nbytes, block_size, num_threads);
}

}  // namespace ray
