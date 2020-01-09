#ifndef RAY_UTIL_PROCESS_H
#define RAY_UTIL_PROCESS_H

#include <functional>  // std::equal_to, std::hash, std::less
#include <memory>      // std::shared_ptr

#include <boost/process/args.hpp>
#include <boost/process/child.hpp>

// We only define operators required by the standard library (<, ==, hash).
// We declare but avoid defining the rest so that they're not used by accident.

namespace ray {

typedef boost::process::pid_t pid_t;
typedef boost::process::child Process;

static constexpr boost::process::detail::args_ make_process_args;

/// A managed equivalent of a pid_t (to manage the lifetime of each process).
/// TODO(mehrdadn): This hasn't been a great design, but we play along to
/// minimize the changes needed for Windows compatibility.
/// (We used to represent a worker process by just its pid_t, which carries
/// no ownership/lifetime information.)
/// Once this code is running properly, refactor the data structures to create
/// a better ownership structure between the worker processes and the workers.
class ProcessHandle {
  typedef std::shared_ptr<Process> Ptr;
  Ptr proc_;
public:
  ProcessHandle(const Ptr &proc = Ptr()) : proc_(proc) { }
  Ptr::element_type *get() const { return proc_.get(); }
  explicit operator bool() const { return !!proc_; }
  static ProcessHandle FromPid(pid_t pid) {
    Process temp(pid);
    temp.detach();
    return std::make_shared<Process>(std::move(temp));
  }
};

}  // namespace ray

// Define comparators for process handles:
// -   Valid process objects must be distinguished by their IDs.
// - Invalid process objects must be distinguished by their addresses.
namespace std {

template <>
struct equal_to<ray::ProcessHandle> {
  typedef ray::ProcessHandle argument_type;
  typedef bool result_type;
  result_type operator()(const argument_type &x, const argument_type &y) const {
    const ray::Process *a = x.get(), *b = y.get();
    // See explanation above
    return a ? b ? a->valid()
                       ? b->valid() ? equal_to<ray::pid_t>()(a->id(), b->id()) : false
                       : b->valid() ? false : equal_to<void const *>()(a, b)
                 : false
             : !b;
  }
};

template <>
struct hash<ray::ProcessHandle> {
  typedef ray::ProcessHandle argument_type;
  typedef size_t result_type;
  result_type operator()(const argument_type &value) const {
    const ray::Process *p = value.get();
    // See explanation above
    return p ? p->valid() ? hash<ray::pid_t>()(p->id()) : hash<void const *>()(p)
             : result_type();
  }
};

}  // namespace std

#endif
