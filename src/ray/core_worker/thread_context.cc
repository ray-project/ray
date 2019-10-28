#include "ray/core_worker/thread_context.h"

namespace ray {

/// Per-thread worker context.
static thread_local std::unique_ptr<ThreadContext> thread_context_;

// Flag used to ensure that we only print a warning about multithreading once per
// process.
static bool multithreading_warning_printed_ = false;

ThreadContext &ThreadContext::Get(bool for_main_thread) {
  if (thread_context_ == nullptr) {
    thread_context_ = std::unique_ptr<ThreadContext>(new ThreadContext());
    if (!for_main_thread && !multithreading_warning_printed_) {
      std::cout << "WARNING: "
                << "Calling ray.get or ray.wait in a separate thread "
                << "may lead to deadlock if the main thread blocks on "
                << "this thread and there are not enough resources to "
                << "execute more tasks." << std::endl;
      multithreading_warning_printed_ = true;
    }
  }

  return *thread_context_;
}

}  // namespace ray
