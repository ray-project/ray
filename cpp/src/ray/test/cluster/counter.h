
#pragma once

#include <ray/api.h>
#include <condition_variable>
#include <mutex>

/// a class of user code
class Counter {
 public:
  int count;
  Counter(int init);
  static Counter *FactoryCreate();
  static Counter *FactoryCreate(int init);
  static Counter *FactoryCreate(int init1, int init2);

  int Plus1();
  int Add(int x);
  int Exit();
  uint64_t GetPid();
  void ExceptionFunc() { throw std::invalid_argument("error"); }
  static bool IsProcessAlive(uint64_t pid);
};

class CountDownLatch {
 public:
  explicit CountDownLatch(size_t count) : m_count(count) {}

  void Wait() {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_count > 0) {
      m_cv.wait(lock, [this]() { return m_count == 0; });
    }
  }

  void CountDown() {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_count > 0) {
      m_count--;
      m_cv.notify_all();
    }
  }

 private:
  std::mutex m_mutex;
  std::condition_variable m_cv;
  size_t m_count = 0;
};

class ActorConcurrentCall {
 public:
  static ActorConcurrentCall *FactoryCreate() { return new ActorConcurrentCall(); }

  std::string CountDown() {
    contdown_.CountDown();
    contdown_.Wait();
    return "ok";
  }

 private:
  CountDownLatch contdown_{3};
};