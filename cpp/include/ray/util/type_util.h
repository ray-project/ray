
#pragma once

#include <functional>
#include <memory>

namespace ray {

template <typename T>
using del_unique_ptr = std::unique_ptr<T, std::function<void(T *)> >;

}  // namespace ray
