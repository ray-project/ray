#pragma once
#include <functional>
#include <utility>

#include "ray/common/asio/instrumented_io_context.h"

namespace ray {

// Wrapper for a std::function that includes an instrumented_io_context for dispatching.
// On `Dispatch`, the function is called with the provided arguments, dispatched onto the
// provided io_context.
//
// The io_context must outlive the Dispatchable object.
template <typename FuncType>
class Dispatchable {
 public:
  Dispatchable(std::function<FuncType> func, instrumented_io_context &io_context)
      : func_(std::move(func)), io_context_(&io_context) {}

  explicit Dispatchable(nullptr_t) : func_(nullptr), io_context_(nullptr) {}

  template <typename... Args>
  void DispatchIfNonNull(const std::string &name, Args &&...args) const {
    if (func_) {
      RAY_CHECK(io_context_ != nullptr);
      io_context_->dispatch(
          [func = func_,
           args_tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable {
            std::apply(func, std::move(args_tuple));
          },
          name);
    }
  }

  std::function<FuncType> AsDispatchedFunction(const std::string &name) && {
    return [moved = std::move(*this), name](auto &&...args) {
      moved.DispatchIfNonNull(name, std::forward<decltype(args)>(args)...);
    };
  }

  bool operator==(std::nullptr_t) const { return func_ == nullptr; }
  bool operator!=(std::nullptr_t) const { return func_ != nullptr; }

  std::function<FuncType> func_;
  instrumented_io_context *const io_context_;
};

namespace internal {

template <typename FuncType>
struct ToDispatchableHelper;

template <typename FuncType>
struct ToDispatchableHelper<std::function<FuncType>> {
  using type = Dispatchable<FuncType>;
};

}  // namespace internal

// using ToDispatchable<std::function<ret(args)>> = Dispatchable<ret(args)>;
template <typename FuncType>
using ToDispatchable = typename internal::ToDispatchableHelper<FuncType>::type;

}  // namespace ray
