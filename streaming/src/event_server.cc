#include <unordered_set>

#include "event_server.h"
namespace ray {
namespace streaming {
EventServer::EventServer(uint32_t event_size)
    : event_queue_(std::make_shared<EventQueue<Event> >(event_size)), stop_flag_(false) {}
EventServer::~EventServer() {
  stop_flag_ = true;
  // No need to join if loop thread has never been created.
  if (loop_thread_ && loop_thread_->joinable()) {
    STREAMING_LOG(WARNING) << "Loop Thread Stopped";
    loop_thread_->join();
  }
}

void EventServer::Run() {
  stop_flag_ = false;
  event_queue_->Run();
  loop_thread_ = std::make_shared<std::thread>(&EventServer::LoopThreadHandler, this);
  STREAMING_LOG(WARNING) << "event_server run";
}

void EventServer::Stop() {
  stop_flag_ = true;
  event_queue_->Stop();
  if (loop_thread_->joinable()) {
    loop_thread_->join();
  }
  STREAMING_LOG(WARNING) << "event_server stop";
}

bool EventServer::Register(const EventType &type, const Handle &handle) {
  if (event_handle_map_.find(type) != event_handle_map_.end()) {
    STREAMING_LOG(WARNING) << "EventType had been registered!";
  }
  event_handle_map_[type] = handle;
  return true;
}

void EventServer::Push(const Event &event) {
  if (event.urgent_) {
    event_queue_->PushToUrgent(event);
  } else {
    event_queue_->Push(event);
  }
}

void EventServer::Execute(Event &event) {
  if (event_handle_map_.find(event.type_) == event_handle_map_.end()) {
    STREAMING_LOG(WARNING) << "Handle has never been registered yet, type => "
                           << static_cast<int>(event.type_);
    return;
  }
  Handle &handle = event_handle_map_[event.type_];
  if (handle(event.channel_info_)) {
    event_queue_->Pop();
  }
}

void EventServer::LoopThreadHandler() {
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

void EventServer::RemoveDestroyedChannelEvent(const std::vector<ObjectID> &removed_ids) {
  // NOTE(lingxuan.zlx): To prevent producing invalid event for removed
  // channels, we pop out all invalid channel related events(push it to
  // original queue if it has no connection with removed channels).
  std::unordered_set<ObjectID> removed_set(removed_ids.begin(), removed_ids.end());
  size_t total_event_nums = EventNums();
  STREAMING_LOG(INFO) << "Remove Destroyed channel event, removed_ids size "
                      << removed_ids.size() << ", total event size " << total_event_nums;
  size_t removed_related_num = 0;
  event_queue_->Run();
  for (size_t i = 0; i < total_event_nums; ++i) {
    Event event;
    if (!event_queue_->Get(event) || !event.channel_info_) {
      STREAMING_LOG(WARNING) << "Fail to get event or channel_info is null, i = " << i;
      continue;
    }
    if (removed_set.find(event.channel_info_->channel_id) != removed_set.end()) {
      removed_related_num++;
    } else if (event.urgent_) {
      event_queue_->PushToUrgent(event);
    } else {
      event_queue_->Push(event);
    }
    event_queue_->Pop();
  }
  event_queue_->Stop();
  STREAMING_LOG(INFO) << "Total event num => " << total_event_nums
                      << ", removed related num => " << removed_related_num;
}

}  // namespace streaming
}  // namespace ray
