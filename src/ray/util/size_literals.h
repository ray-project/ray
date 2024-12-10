// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// These user-defined literals makes sizes.

#pragma once

#include <cstdint>

namespace ray::literals {

// Size literal for integer values.
constexpr unsigned long long operator""_B(unsigned long long sz) { return sz; }

constexpr unsigned long long operator""_KiB(unsigned long long sz) { return sz * 1024_B; }
constexpr unsigned long long operator""_KB(unsigned long long sz) { return sz * 1000_B; }

constexpr unsigned long long operator""_MiB(unsigned long long sz) {
  return sz * 1024_KiB;
}
constexpr unsigned long long operator""_MB(unsigned long long sz) { return sz * 1000_KB; }

constexpr unsigned long long operator""_GiB(unsigned long long sz) {
  return sz * 1024_MiB;
}
constexpr unsigned long long operator""_GB(unsigned long long sz) { return sz * 1000_MB; }

constexpr unsigned long long operator""_TiB(unsigned long long sz) {
  return sz * 1024_GiB;
}
constexpr unsigned long long operator""_TB(unsigned long long sz) { return sz * 1000_GB; }

constexpr unsigned long long operator""_PiB(unsigned long long sz) {
  return sz * 1024_TiB;
}
constexpr unsigned long long operator""_PB(unsigned long long sz) { return sz * 1000_TB; }

// Size literals for floating point values.
constexpr unsigned long long operator""_KiB(long double sz) {
  const long double res = sz * 1_KiB;
  return static_cast<unsigned long long>(res);
}
constexpr unsigned long long operator""_KB(long double sz) {
  const long double res = sz * 1_KB;
  return static_cast<unsigned long long>(res);
}

constexpr unsigned long long operator""_MiB(long double sz) {
  const long double res = sz * 1_MiB;
  return static_cast<unsigned long long>(res);
}
constexpr unsigned long long operator""_MB(long double sz) {
  const long double res = sz * 1_MB;
  return static_cast<unsigned long long>(res);
}

constexpr unsigned long long operator""_GiB(long double sz) {
  const long double res = sz * 1_GiB;
  return static_cast<unsigned long long>(res);
}
constexpr unsigned long long operator""_GB(long double sz) {
  const long double res = sz * 1_GB;
  return static_cast<unsigned long long>(res);
}

constexpr unsigned long long operator""_TiB(long double sz) {
  const long double res = sz * 1_TiB;
  return static_cast<unsigned long long>(res);
}
constexpr unsigned long long operator""_TB(long double sz) {
  const long double res = sz * 1_TB;
  return static_cast<unsigned long long>(res);
}

constexpr unsigned long long operator""_PiB(long double sz) {
  const long double res = sz * 1_PiB;
  return static_cast<unsigned long long>(res);
}
constexpr unsigned long long operator""_PB(long double sz) {
  const long double res = sz * 1_PB;
  return static_cast<unsigned long long>(res);
}

}  // namespace ray::literals
