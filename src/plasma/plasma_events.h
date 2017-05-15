#ifndef PLASMA_EVENTS
#define PLASMA_EVENTS

#include <functional>
#include <memory>
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

class EventLoop {
 public:
  /* Signature of the handler that will be called when there is a new event
   * on the file descriptor that this handler has been registered for.
   *
   * The arguments are the event flags (read or write).
   */
  typedef std::function<void(int)> FileCallback;

  /* This handler will be called when a timer times out. The timer id is
   * passed as an argument. The return is the number of milliseconds the timer
   * shall be reset to or kEventLoopTimerDone if the timer shall not be
   * triggered again.
   */
  typedef std::function<int(int64_t)> TimerCallback;

  EventLoop();

  /**
   * Add a new file event handler to the event loop.
   *
   * @param fd The file descriptor we are listening to.
   * @param events The flags for events we are listening to (read or write).
   * @param callback The callback that will be called when the event happens.
   * @return Returns true if the event handler was added successfully.
   */
  bool add_file_event(int fd, int events, FileCallback callback);

  /**
   * Remove a file event handler from the event loop.
   *
   * @param fd The file descriptor of the event handler.
   * @return Void.
   */
  void remove_file_event(int fd);

  /** Register a handler that will be called after a time slice of
   *  "timeout" milliseconds.
   *
   *  @param timeout The timeout in milliseconds.
   *  @param callback The callback for the timeout.
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
  static void file_event_callback(aeEventLoop *loop,
                                  int fd,
                                  void *context,
                                  int events);

  static int timer_event_callback(aeEventLoop *loop,
                                  long long timer_id,
                                  void *context);

  aeEventLoop *loop_;
  std::unordered_map<int, std::unique_ptr<FileCallback>> file_callbacks_;
  std::unordered_map<int64_t, std::unique_ptr<TimerCallback>> timer_callbacks_;
};

#endif /* PLASMA_EVENTS */
