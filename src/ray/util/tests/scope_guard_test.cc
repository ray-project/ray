#include "src/ray/util/scope_guard.h"

#include <gtest/gtest.h>

#include <future>

namespace ray {

namespace {

TEST(ScopeGuardTest, BasicTest) {
  std::promise<void> promise{};
  {
    ScopeGuard wg{[&]() { promise.set_value(); }};
  }
  promise.get_future().get();
}

TEST(ScopeGuardTest, MacroTest) {
  std::promise<void> promise{};
  {
    SCOPE_EXIT { promise.set_value(); };
  }
  promise.get_future().get();
}

}  // namespace

}  // namespace ray
