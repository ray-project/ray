#ifndef RAY_UTIL_PROCESS_H
#define RAY_UTIL_PROCESS_H

#ifdef __linux__
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <functional>
#include <memory>
#include <system_error>
#include <utility>

// Boost.Process headers are quite heavyweight, so we avoid including them here
namespace boost {

namespace asio {

class io_context;
typedef io_context io_service;

}  // namespace asio

namespace process {

class child;

}  // namespace process

}  // namespace boost

// We only define operators required by the standard library (==, hash).
// We declare but avoid defining the rest so that they're not used by accident.

namespace ray {

#ifdef _WIN32
typedef int pid_t;
#endif

class Process {
 protected:
  class ProcessFD;
  std::shared_ptr<std::pair<ProcessFD, boost::process::child> > p_;

  explicit Process(pid_t &pid);

 public:
  ~Process();
  /// Creates a null process object. Two null process objects are assumed equal.
  Process();
  Process(const Process &);
  Process(Process &&);
  Process &operator=(Process other);
  /// Creates a new process.
  /// \param[in] argv The command-line of the process to spawn (terminated with NULL).
  /// \param[in] io_service Boost.Asio I/O service (optional but recommended).
  /// \param[in] on_exit Callback to invoke on exit (optional).
  /// \param[in] ec Returns any error that occurred when spawning the process.
  explicit Process(const char *argv[], boost::asio::io_service *io_service,
                   const std::function<void(int, const std::error_code &)> &on_exit,
                   std::error_code &ec);
  void Detach();
  static Process CreateNewDummy();
  static Process FromPid(pid_t pid);
  boost::process::child *Get() const;
  pid_t GetId() const;
  bool IsNull() const;
  bool IsRunning() const;
  bool IsValid() const;
  void Join();
  void Kill();
};

}  // namespace ray

// Define comparators for process handles:
// -   Valid process objects must be distinguished by their IDs.
// - Invalid process objects must be distinguished by their addresses.
namespace std {

template <>
struct equal_to<ray::Process> {
  bool operator()(const ray::Process &x, const ray::Process &y) const;
};

template <>
struct hash<ray::Process> {
  size_t operator()(const ray::Process &value) const;
};

}  // namespace std

#endif
