#include <sys/wait.h>
#include <boost/optional.hpp>
#include <functional>
#include <mutex>
#include <thread>

#include "absl/container/flat_hash_map.h"

namespace ray {
/// \class ChildProcessWatchdog
///
/// Used for triggering a callback function when the watched child process is dead.
template <class T>
using ProcessTerminatedCallback = std::function<void(
    pid_t pid, boost::optional<int> signal_no, boost::optional<std::string> signal_name,
    boost::optional<int> exit_code, const T &process_data)>;
template <class T>
class ChildProcessWatchdog {
 public:
  explicit ChildProcessWatchdog(
      const ray::ProcessTerminatedCallback<T> terminated_callback,
      int watchdog_wait_internval_ms = 100)
      : terminated_callback_(terminated_callback),
        watchdog_wait_internval_ms_(watchdog_wait_internval_ms) {
    loop_thread_ = std::thread(&ChildProcessWatchdog<T>::Loop, this);
  }

  void WatchChildProcess(pid_t pid, const T &process_data) {
    absl::MutexLock lock(&mutex_);
    watched_processes_[pid] = process_data;
  }

  void UnWatchChildProcess(pid_t pid) { (void)PopWatchedProcessData(pid); }

  ~ChildProcessWatchdog() {
    pthread_cancel(loop_thread_.native_handle());
    loop_thread_.join();
  }

 private:
  std::thread loop_thread_;
  ray::ProcessTerminatedCallback<T> terminated_callback_;
  int watchdog_wait_internval_ms_;

  // Mutex guarding watched_processes_.
  absl::Mutex mutex_;
  absl::flat_hash_map<pid_t, T> watched_processes_ GUARDED_BY(mutex_);

  void Loop() {
    while (true) {
      int status;
      // Watching all child processes and will block
      pid_t pid = wait(&status);

      // Pid will not be 0, because `WNOHANG` was not set
      // pid < 0 means don't have any Child Process, just ignore
      if (pid < 0) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(watchdog_wait_internval_ms_));
      } else {
        // Because `WUNTRACED` or  `WCONTINUED` was not set, So child process
        // must have been terminated and there are only two possibilities: exited or
        // signaled
        typename boost::optional<T> process_data = PopWatchedProcessData(pid);
        if (!process_data) continue;
        // Only invoke the callback if the process is watched.
        terminated_callback_(
            pid, WIFEXITED(status) ? boost::none : boost::optional<int>(WTERMSIG(status)),
            WIFEXITED(status) ? boost::none
                              : boost::optional<std::string>(strsignal(WTERMSIG(status))),
            WIFEXITED(status) ? boost::optional<int>(WEXITSTATUS(status)) : boost::none,
            *process_data);
      }
    }
  }

  boost::optional<T> PopWatchedProcessData(pid_t pid) {
    absl::MutexLock lock(&mutex_);
    typename absl::flat_hash_map<pid_t, T>::iterator iter = watched_processes_.find(pid);
    if (iter == watched_processes_.end()) {
      return boost::none;
    } else {
      boost::optional<T> process_data(iter->second);
      watched_processes_.erase(iter);
      return process_data;
    }
  }
};

}  // namespace ray