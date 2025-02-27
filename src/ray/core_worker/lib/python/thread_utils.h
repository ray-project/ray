#pragma once

#include <Python.h>

#include <functional>

namespace ray {
namespace core {

inline std::function<void()> GetThreadReleaser(PyGILState_STATE gstate) {
  return [gstate]() { PyGILState_Release(gstate); };
}

}  // namespace core
}  // namespace ray
