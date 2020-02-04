#ifndef RAY_STREAMING_EVENT_SERVER_H
#define RAY_STREAMING_EVENT_SERVER_H
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

#include "channel.h"
#include "ring_buffer.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {

/// Data writer utilizes what's called an event-driven programming model instead
/// loop forward polling driven that's employed in first simple version.
/// Event driven model includes two important components: event server and event
/// queue. In the process of data transmission, the inputer will first define
/// the processing method of corresponding events. However, by triggering
/// different events in actual operation, these events will be put into the event
/// queue, and finally the event server will schedule the previously registered
/// processing functions ordered by its priority.

/// The event queue inherits from the universal ring queue. It mainly divides
/// time into two different levels: normal event and urgent event, and the
/// total size of the queue is the sum of them.
template <class T>
class EventQueue : public AbstractRingBufferImpl<T> {
 public:
  EventQueue(size_t size) : capacity_(size), is_started_(true) {}

  virtual ~EventQueue() {
    is_started_ = false;
    full_cv_.notify_all();
    empty_cv_.notify_all();
  };

  void Run() { is_started_ = true; }

  void Stop() {
    is_started_ = false;
    empty_cv_.notify_all();
    full_cv_.notify_all();
  }

  void Push(const T &t) {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    while (Size() >= capacity_ && is_started_) {
      STREAMING_LOG(WARNING) << " EventQueue is full, its size:" << Size()
                             << " all_size:" << Size() << " capacity:" << capacity_
                             << " buffer size:" << buffer_.size()
                             << " urgent_buffer size:" << urgent_buffer_.size();
      full_cv_.wait(lock);
      STREAMING_LOG(WARNING) << "event_server's full_sleep be notifyed";
    }
    if (!is_started_) {
      return;
    }
    buffer_.push(t);
    if (1 == Size()) {
      empty_cv_.notify_one();
    }
  }

  void PushToUrgent(const T &t) {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    while (Size() >= capacity_ && is_started_) {
      STREAMING_LOG(WARNING) << " EventQueue is full, its size:" << Size()
                             << " all_size:" << Size() << " capacity:" << capacity_
                             << " buffer size:" << buffer_.size()
                             << " urgent_buffer size:" << urgent_buffer_.size();
      full_cv_.wait(lock);
      STREAMING_LOG(WARNING) << "event_server's full_sleep be notifyed";
    }
    if (!is_started_) {
      return;
    }
    urgent_buffer_.push(t);
    if (Size() == 1) {
      empty_cv_.notify_one();
    }
  }

  void Pop() {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    if (Size() >= capacity_) {
      STREAMING_LOG(WARNING) << "Pop should notify"
                             << " all_Size:" << Size();
    }
    if (urgent_) {
      urgent_buffer_.pop();
    } else {
      buffer_.pop();
    }
    full_cv_.notify_all();
  }

  bool Get(T &evt) {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    while (Empty() && is_started_) {
      empty_cv_.wait(lock);
    }
    if (!is_started_) {
      return false;
    }
    if (!urgent_buffer_.empty()) {
      urgent_ = true;
      evt = urgent_buffer_.front();
    } else {
      urgent_ = false;
      evt = buffer_.front();
    }
    return true;
  }

  T PopAndGet() {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    while (Empty() && is_started_) {
      empty_cv_.wait(lock);
    }
    if (!is_started_) {
      return T();
    }
    if (!urgent_buffer_.empty()) {
      T res = urgent_buffer_.front();
      urgent_buffer_.pop();
      if (Full()) {
        full_cv_.notify_one();
      }
      return res;
    }
    T res = buffer_.front();
    buffer_.pop();
    if (Size() + 1 == capacity_) full_cv_.notify_one();
    return res;
  }

  T &Front() {
    std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
    if (urgent_buffer_.size()) {
      return urgent_buffer_.front();
    }
    return buffer_.front();
  }

  inline bool Empty() { return buffer_.empty() && urgent_buffer_.empty(); }

  inline bool Full() { return buffer_.size() + urgent_buffer_.size() == capacity_; }

  inline size_t Size() { return buffer_.size() + urgent_buffer_.size(); }

  inline size_t UrgentBufferSize() { return urgent_buffer_.size(); }

  inline size_t Capacity() { return capacity_; }

 private:
  std::mutex ring_buffer_mutex_;
  std::condition_variable empty_cv_;
  std::condition_variable full_cv_;
  std::queue<T> buffer_;
  std::queue<T> urgent_buffer_;
  bool urgent_ = false;
  size_t capacity_;
  bool is_started_;
};

enum class EventType : uint8_t {
  UserEvent = 0,
  FlowEvent = 1,
  EmptyEvent = 2,
  FullChannel = 3,
  Reload = 4,
};

struct EnumTypeHash {
  template <typename T>
  std::size_t operator()(const T &t) const {
    return static_cast<std::size_t>(t);
  }
};

struct Event {
  ProducerChannelInfo *channel_info_;
  EventType type_;
  bool urgent_;
};

class EventServer {
 public:
  /// User-define event handle for different types.
  typedef std::function<bool(ProducerChannelInfo *info)> Handle;

  EventServer();

  ~EventServer();

  void Run();

  void Stop();

  bool Register(const EventType &type, const Handle &handle);

  void Push(const Event &event);

  /// A single thread should be invoked to run this loop function, so that
  /// event server can poll and execute registered callback function event
  /// one by one.
  void LoopThd();

  inline size_t EventNums() { return event_queue_->Size(); }

  void RemoveDestroyedChannelEvent(const std::vector<ObjectID> &removed_ids);

 private:
  void Execute(Event &event);

 private:
  std::unordered_map<EventType, Handle, EnumTypeHash> event_handle_map_;
  std::shared_ptr<EventQueue<Event> > event_queue_;
  std::shared_ptr<std::thread> loop_thread_;

  bool stop_flag_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_EVENT_SERVER_H
