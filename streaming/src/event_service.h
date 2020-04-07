#ifndef RAY_STREAMING_EVENT_SERVER_H
#define RAY_STREAMING_EVENT_SERVER_H
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

#include "channel.h"
#include "ray/core_worker/core_worker.h"
#include "ring_buffer.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {

enum class EventType : uint8_t {
  // A message created by user writing.
  UserEvent = 0,
  // Unblock upstream writing when it's under flowcontrol.
  FlowEvent = 1,
  // Trigger an empty message by timer.
  EmptyEvent = 2,
  FullChannel = 3,
  // Recovery at the beginning.
  Reload = 4,
  // Error event if event queue is freezed.
  ErrorEvent = 5
};

struct EnumTypeHash {
  template <typename T>
  std::size_t operator()(const T &t) const {
    return static_cast<std::size_t>(t);
  }
};

struct Event {
  ProducerChannelInfo *channel_info;
  EventType type;
  bool urgent;
};

/// Data writer utilizes what's called an event-driven programming model
/// that includes two important components: event service and event
/// queue. In the process of data transmission, the writer will first define
/// the processing method of corresponding events. However, by triggering
/// different events in actual operation, these events will be put into the event
/// queue, and finally the event server will schedule the previously registered
/// processing functions ordered by its priority.
class EventQueue {
 public:
  EventQueue(size_t size) : urgent_(false), capacity_(size), is_freezed_(true) {}

  virtual ~EventQueue();

  /// Resume event queue to normal model.
  void Unfreeze();

  /// Push is prohibited when event queue is freezed.
  void Freeze();

  void Push(const Event &t);

  void Pop();

  bool Get(Event &evt);

  Event PopAndGet();

  Event &Front();

  inline size_t Capacity() const { return capacity_; }

  /// It mainly divides event into two different levels: normal event and urgent
  /// event, and the total size of the queue is the sum of them.
  inline size_t Size() const { return buffer_.size() + urgent_buffer_.size(); }

 private:
  /// (NOTE:lingxuan.zlx) There is no strict thread-safe when query empty or full,
  /// but it can reduce lock contention. In fact, these functions are thread-safe
  /// when invoked via Push/Pop where buffer size will only be changed in whole process.
  inline bool Empty() const { return buffer_.empty() && urgent_buffer_.empty(); }

  inline bool Full() const { return buffer_.size() + urgent_buffer_.size() == capacity_; }

 private:
  std::mutex ring_buffer_mutex_;
  std::condition_variable no_empty_cv_;
  std::condition_variable no_full_cv_;
  // Normal events wil be pushed into buffer_.
  std::queue<Event> buffer_;
  // This field urgent_buffer_ is used for serving urgent event.
  std::queue<Event> urgent_buffer_;
  // Urgent event will be poped out first if urgent_ flag is true.
  bool urgent_;
  size_t capacity_;
  bool is_freezed_;
};

class EventService {
 public:
  /// User-define event handle for different types.
  typedef std::function<bool(ProducerChannelInfo *info)> Handle;

  EventService(uint32_t event_size = kEventQueueCapacity);

  ~EventService();

  void Run();

  void Stop();

  bool Register(const EventType &type, const Handle &handle);

  void Push(const Event &event);

  inline size_t EventNums() const { return event_queue_->Size(); }

  void RemoveDestroyedChannelEvent(const std::vector<ObjectID> &removed_ids);

 private:
  void Execute(Event &event);

  /// A single thread should be invoked to run this loop function, so that
  /// event server can poll and execute registered callback function event
  /// one by one.
  void LoopThreadHandler();

 private:
  WorkerID worker_id_;
  std::unordered_map<EventType, Handle, EnumTypeHash> event_handle_map_;
  std::shared_ptr<EventQueue> event_queue_;
  std::shared_ptr<std::thread> loop_thread_;

  static constexpr int kEventQueueCapacity = 1000;

  bool stop_flag_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_EVENT_SERVER_H
