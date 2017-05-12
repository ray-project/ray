#ifndef PLASMA_EVENTS
#define PLASMA_EVENTS

#include <functional>
#include <list>

extern "C" {
#include "ae/ae.h"
}

template<typename T>
class EventLoop {
 public:
  typedef std::function<void(EventLoop &, T &, int, int)> FileCallback;
  typedef std::function<void(EventLoop &, T &, int64_t)> TimerCallback;

  EventLoop(T& context);

  bool add_file_event(int fd, int events, FileCallback callback);

  void remove_file_event(int fd);

  int64_t add_timer(int64_t timeout, TimerCallback callback);

  int remove_timer(int64_t timer_id);

  void run();

 private:

  struct FileCallbackData {
    EventLoop *loop;
    FileCallback callback;
  };

  struct TimerCallbackData {
    EventLoop *loop;
    TimerCallback callback;
  };

  static void file_event_callback(aeEventLoop *loop,
                                  int fd,
                                  void *context,
                                  int events);

  static int timer_event_callback(aeEventLoop *loop,
                                  long long timer_id,
                                  void *context);

  aeEventLoop *loop_;
  T &context_;
  // The following needs to be a list or another STL datastructure that does
  // not reallocate elements (i.e. it cannot be a vector). This is because
  // we are passing around pointers to elements in C and they are stored in
  // internal datastructures of the ae event loop.
  std::list<FileCallbackData> file_callbacks_;
  std::list<TimerCallbackData> timer_callbacks_;
};

template<typename T>
void EventLoop<T>::file_event_callback(aeEventLoop *loop,
                            int fd,
                            void *context,
                            int events) {
  FileCallbackData *data = reinterpret_cast<FileCallbackData*>(context);
  EventLoop &event_loop = *data->loop;
  data->callback(event_loop, event_loop.context_, fd, events);
}

template<typename T>
int EventLoop<T>::timer_event_callback(aeEventLoop *loop,
                                  long long timer_id,
                                  void *context) {
  TimerCallbackData *data = reinterpret_cast<TimerCallbackData*>(context);
  EventLoop &event_loop = *data->loop;
  data->callback(event_loop, event_loop.context_, timer_id);
}

constexpr int kInitialEventLoopSize = 1024;

template<typename T>
EventLoop<T>::EventLoop(T &context)
  : context_(context) {
  loop_ = aeCreateEventLoop(kInitialEventLoopSize);
}

template<typename T>
bool EventLoop<T>::add_file_event(int fd, int events, FileCallback callback) {
  FileCallbackData data;
  data.loop = this;
  data.callback = callback;
  file_callbacks_.push_back(data);
  void *context = reinterpret_cast<void *>(&file_callbacks_.back());
  /* Try to add the file descriptor. */
  int err = aeCreateFileEvent(loop_, fd, events, EventLoop::file_event_callback, context);
  /* If it cannot be added, increase the size of the event loop. */
  if (err == AE_ERR && errno == ERANGE) {
    err = aeResizeSetSize(loop_, 3 * aeGetSetSize(loop_) / 2);
    if (err != AE_OK) {
      return false;
    }
    err = aeCreateFileEvent(loop_, fd, events, EventLoop::file_event_callback, context);
  }
  /* In any case, test if there were errors. */
  return (err == AE_OK);
}

template<typename T>
void EventLoop<T>::remove_file_event(int fd) {
  aeDeleteFileEvent(loop_, fd, AE_READABLE | AE_WRITABLE);
  // XXX TODO: delete context data
}

template<typename T>
void EventLoop<T>::run() {
  aeMain(loop_);
}

template<typename T>
int64_t EventLoop<T>::add_timer(int64_t timeout, TimerCallback callback) {
  TimerCallbackData data;
  data.loop = this;
  data.callback = callback;
  timer_callbacks_.push_back(data);
  void *context = reinterpret_cast<void *>(&timer_callbacks_.back());
  return aeCreateTimeEvent(loop_, timeout, EventLoop::timer_event_callback, context, NULL);
}

template<typename T>
int EventLoop<T>::remove_timer(int64_t timer_id) {
  return aeDeleteTimeEvent(loop_, timer_id);
  // XXX TODO: delete context data
}

#endif /* PLASMA_EVENTS */
