#include "event_service.h"

#include <chrono>
#include <unordered_set>

namespace ray {
namespace streaming {

EventQueue::~EventQueue() {
  is_active_ = false;
  no_full_cv_.notify_all();
  no_empty_cv_.notify_all();
};

void EventQueue::Unfreeze() { is_active_ = true; }

void EventQueue::Freeze() {
  is_active_ = false;
  no_empty_cv_.notify_all();
  no_full_cv_.notify_all();
}

void EventQueue::Push(const Event &t) {
  std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
  while (Size() >= capacity_ && is_active_) {
    STREAMING_LOG(WARNING) << " EventQueue is full, its size:" << Size()
                           << " capacity:" << capacity_
                           << " buffer size:" << buffer_.size()
                           << " urgent_buffer size:" << urgent_buffer_.size();
    no_full_cv_.wait(lock);
    STREAMING_LOG(WARNING) << "Event server is full_sleep be notified";
  }
  if (!is_active_) {
    return;
  }
  if (t.urgent) {
    buffer_.push(t);
  } else {
    urgent_buffer_.push(t);
  }
  if (1 == Size()) {
    no_empty_cv_.notify_one();
  }
}

void EventQueue::Pop() {
  std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
  if (Size() >= capacity_) {
    STREAMING_LOG(WARNING) << "Pop should notify"
                           << " size : " << Size();
  }
  if (urgent_) {
    urgent_buffer_.pop();
  } else {
    buffer_.pop();
  }
  no_full_cv_.notify_all();
}

void EventQueue::WaitFor(std::unique_lock<std::mutex> &lock) {
  // To avoid deadlock when EventQueue is empty but is_active is changed in other
  // thread, Event queue should awaken this condtion variable and check it again.
  while (is_active_ && Empty()) {
    int timeout = kConditionTimeoutMs;  // This avoids const & to static (linking error)
    if (!no_empty_cv_.wait_for(lock, std::chrono::milliseconds(timeout),
                               [this]() { return !is_active_ || !Empty(); })) {
      STREAMING_LOG(DEBUG) << "No empty condition variable wait timeout."
                           << " Empty => " << Empty() << ", is active " << is_active_;
    }
  }
}

bool EventQueue::Get(Event &evt) {
  std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
  WaitFor(lock);
  if (!is_active_) {
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

Event EventQueue::PopAndGet() {
  std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
  WaitFor(lock);
  if (!is_active_) {
    // Return error event if queue is active.
    return Event({nullptr, EventType::ErrorEvent, false});
  }
  if (!urgent_buffer_.empty()) {
    Event res = urgent_buffer_.front();
    urgent_buffer_.pop();
    if (Full()) {
      no_full_cv_.notify_one();
    }
    return res;
  }
  Event res = buffer_.front();
  buffer_.pop();
  if (Size() + 1 == capacity_) no_full_cv_.notify_one();
  return res;
}

Event &EventQueue::Front() {
  std::unique_lock<std::mutex> lock(ring_buffer_mutex_);
  if (urgent_buffer_.size()) {
    return urgent_buffer_.front();
  }
  return buffer_.front();
}

EventService::EventService(uint32_t event_size)
    : worker_id_(CoreWorkerProcess::IsInitialized()
                     ? CoreWorkerProcess::GetCoreWorker().GetWorkerID()
                     : WorkerID::Nil()),
      event_queue_(std::make_shared<EventQueue>(event_size)),
      stop_flag_(false) {}
EventService::~EventService() {
  stop_flag_ = true;
  // No need to join if loop thread has never been created.
  if (loop_thread_ && loop_thread_->joinable()) {
    STREAMING_LOG(WARNING) << "Loop Thread Stopped";
    loop_thread_->join();
  }
}

void EventService::Run() {
  stop_flag_ = false;
  event_queue_->Unfreeze();
  loop_thread_ = std::make_shared<std::thread>(&EventService::LoopThreadHandler, this);
  STREAMING_LOG(WARNING) << "event_server run";
}

void EventService::Stop() {
  stop_flag_ = true;
  event_queue_->Freeze();
  if (loop_thread_->joinable()) {
    loop_thread_->join();
  }
  STREAMING_LOG(WARNING) << "event_server stop";
}

bool EventService::Register(const EventType &type, const Handle &handle) {
  if (event_handle_map_.find(type) != event_handle_map_.end()) {
    STREAMING_LOG(WARNING) << "EventType had been registered!";
  }
  event_handle_map_[type] = handle;
  return true;
}

void EventService::Push(const Event &event) { event_queue_->Push(event); }

void EventService::Execute(Event &event) {
  if (event_handle_map_.find(event.type) == event_handle_map_.end()) {
    STREAMING_LOG(WARNING) << "Handle has never been registered yet, type => "
                           << static_cast<int>(event.type);
    return;
  }
  Handle &handle = event_handle_map_[event.type];
  if (handle(event.channel_info)) {
    event_queue_->Pop();
  }
}

void EventService::LoopThreadHandler() {
  if (CoreWorkerProcess::IsInitialized()) {
    CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id_);
  }
  while (true) {
    if (stop_flag_) {
      break;
    }
    Event event;
    if (event_queue_->Get(event)) {
      Execute(event);
    }
  }
}

void EventService::RemoveDestroyedChannelEvent(const std::vector<ObjectID> &removed_ids) {
  // NOTE(lingxuan.zlx): To prevent producing invalid event for removed
  // channels, we pop out all invalid channel related events(push it to
  // original queue if it has no connection with removed channels).
  std::unordered_set<ObjectID> removed_set(removed_ids.begin(), removed_ids.end());
  size_t total_event_nums = EventNums();
  STREAMING_LOG(INFO) << "Remove Destroyed channel event, removed_ids size "
                      << removed_ids.size() << ", total event size " << total_event_nums;
  size_t removed_related_num = 0;
  event_queue_->Unfreeze();
  for (size_t i = 0; i < total_event_nums; ++i) {
    Event event;
    if (!event_queue_->Get(event) || !event.channel_info) {
      STREAMING_LOG(WARNING) << "Fail to get event or channel_info is null, i = " << i;
      continue;
    }
    if (removed_set.find(event.channel_info->channel_id) != removed_set.end()) {
      removed_related_num++;
    } else {
      event_queue_->Push(event);
    }
    event_queue_->Pop();
  }
  event_queue_->Freeze();
  STREAMING_LOG(INFO) << "Total event num => " << total_event_nums
                      << ", removed related num => " << removed_related_num;
}

}  // namespace streaming
}  // namespace ray
