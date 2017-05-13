#ifndef PLASMA_EVENTS
#define PLASMA_EVENTS

#include <functional>
#include <unordered_map>

extern "C" {
#include "ae/ae.h"
}

/** Constant specifying that the timer is done and it will be removed. */
constexpr int kEventLoopTimerDone = AE_NOMORE;

/** Read event on the file descriptor. */
constexpr int kEventLoopRead = AE_READABLE;

/** Write event on the file descriptor. */
constexpr int kEventLoopWrite = AE_WRITABLE;

template <typename T>
class EventLoop {
 public:
  /* Signature of the handler that will be called when there is a new event
   * on the file descriptor that this handler has been registered for.
   *
   * The arguments are as follows: The event loop, the context of the event
   * loop, the file descriptor that is affected by the event and the
   * event flags (read or write).
   */
  typedef std::function<void(EventLoop &, T &, int, int)> FileCallback;

  /* This handler will be called when a timer times out. The id of the timer
   * as well as the context that was specified when registering this handler
   * are passed as arguments. The return is the number of milliseconds the
   * timer shall be reset to or kEventLoopTimerDone if the timer shall
   * not be triggered again. */
  typedef std::function<int(EventLoop &, T &, int64_t)> TimerCallback;

  EventLoop(T &context);

  /**
   * Add a new file event handler to the event loop.
   *
   * @param fd The file descriptor we are listening to.
   * @param events The flags for events we are listening to (read or write).
   * @param callback The callback that will be called when the event happens.
   *
   * @return Returns true if the event handler was added successfully.
   */
  bool add_file_event(int fd, int events, FileCallback callback);

  /**
   * Remove a file event handler from the event loop.
   *
   * @param fd The file descriptor of the event handler.
   *
   * @return Void.
   */
  void remove_file_event(int fd);

  /** Register a handler that will be called after a time slice of
   *  "timeout" milliseconds.
   *
   *  @param timeout The timeout in milliseconds.
   *  @param callback The callback for the timeout.
   *
   *  @return The ID of the newly created timer.
   */
  int64_t add_timer(int64_t timeout, TimerCallback callback);

  /**
   * Remove a timer handler from the event loop.
   *
   * @param timer_id The ID of the timer that is to be removed.
   * @return The ae.c error code. TODO(pcm): needs to be standardized
   */
  int remove_timer(int64_t timer_id);

  /**
   * Run the event loop.
   *
   * @return Void.
   */
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
  std::unordered_map<int, FileCallbackData *> file_callbacks_;
  std::unordered_map<int64_t, TimerCallbackData *> timer_callbacks_;
};

template <typename T>
void EventLoop<T>::file_event_callback(aeEventLoop *loop,
                                       int fd,
                                       void *context,
                                       int events) {
  FileCallbackData *data = reinterpret_cast<FileCallbackData *>(context);
  EventLoop &event_loop = *data->loop;
  data->callback(event_loop, event_loop.context_, fd, events);
}

template <typename T>
int EventLoop<T>::timer_event_callback(aeEventLoop *loop,
                                       long long timer_id,
                                       void *context) {
  TimerCallbackData *data = reinterpret_cast<TimerCallbackData *>(context);
  EventLoop &event_loop = *data->loop;
  data->callback(event_loop, event_loop.context_, timer_id);
}

constexpr int kInitialEventLoopSize = 1024;

template <typename T>
EventLoop<T>::EventLoop(T &context) : context_(context) {
  loop_ = aeCreateEventLoop(kInitialEventLoopSize);
}

template <typename T>
bool EventLoop<T>::add_file_event(int fd, int events, FileCallback callback) {
  if (file_callbacks_.find(fd) != file_callbacks_.end()) {
    return false;
  }
  FileCallbackData *data = new FileCallbackData();
  data->loop = this;
  data->callback = callback;
  void *context = reinterpret_cast<void *>(data);
  /* Try to add the file descriptor. */
  int err = aeCreateFileEvent(loop_, fd, events, EventLoop::file_event_callback,
                              context);
  /* If it cannot be added, increase the size of the event loop. */
  if (err == AE_ERR && errno == ERANGE) {
    err = aeResizeSetSize(loop_, 3 * aeGetSetSize(loop_) / 2);
    if (err != AE_OK) {
      return false;
    }
    err = aeCreateFileEvent(loop_, fd, events, EventLoop::file_event_callback,
                            context);
  }
  /* In any case, test if there were errors. */
  if (err == AE_OK) {
    file_callbacks_[fd] = data;
    return true;
  }
  return false;
}

template <typename T>
void EventLoop<T>::remove_file_event(int fd) {
  aeDeleteFileEvent(loop_, fd, AE_READABLE | AE_WRITABLE);
  delete file_callbacks_[fd];
  file_callbacks_.erase(fd);
}

template <typename T>
void EventLoop<T>::run() {
  aeMain(loop_);
}

template <typename T>
int64_t EventLoop<T>::add_timer(int64_t timeout, TimerCallback callback) {
  TimerCallbackData *data = new TimerCallbackData();
  data->loop = this;
  data->callback = callback;
  void *context = reinterpret_cast<void *>(data);
  int64_t timer_id = aeCreateTimeEvent(
      loop_, timeout, EventLoop::timer_event_callback, context, NULL);
  timer_callbacks_[timer_id] = data;
  return timer_id;
}

template <typename T>
int EventLoop<T>::remove_timer(int64_t timer_id) {
  int err = aeDeleteTimeEvent(loop_, timer_id);
  delete timer_callbacks_[timer_id];
  timer_callbacks_.erase(timer_id);
  return err;
}

#endif /* PLASMA_EVENTS */
