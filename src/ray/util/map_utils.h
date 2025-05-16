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
//
// This file provides utils for hash map related utils.

#pragma once

#include <functional>
#include <utility>

namespace ray::utils::container {

// A hash wrapper made for `std::reference_wrapper`.
template <typename Hash>
struct RefHash : Hash {
  RefHash() = default;
  template <typename H>
  RefHash(H &&h) : Hash(std::forward<H>(h)) {}  // NOLINT

  RefHash(const RefHash &) = default;
  RefHash(RefHash &&) noexcept = default;
  RefHash &operator=(const RefHash &) = default;
  RefHash &operator=(RefHash &&) noexcept = default;

  template <typename T>
  size_t operator()(std::reference_wrapper<const T> val) const {
    return Hash::operator()(val.get());
  }
  template <typename T>
  size_t operator()(const T &val) const {
    return Hash::operator()(val);
  }
};

template <typename Hash>
RefHash(Hash &&) -> RefHash<std::remove_reference_t<Hash>>;

// A hash equal wrapper made for `std::reference_wrapper`.
template <typename Equal>
struct RefEq : Equal {
  RefEq() = default;
  template <typename Eq>
  RefEq(Eq &&eq) : Equal(std::forward<Eq>(eq)) {}  // NOLINT

  RefEq(const RefEq &) = default;
  RefEq(RefEq &&) noexcept = default;
  RefEq &operator=(const RefEq &) = default;
  RefEq &operator=(RefEq &&) noexcept = default;

  template <typename T1, typename T2>
  bool operator()(std::reference_wrapper<const T1> lhs,
                  std::reference_wrapper<const T2> rhs) const {
    return Equal::operator()(lhs.get(), rhs.get());
  }
  template <typename T1, typename T2>
  bool operator()(const T1 &lhs, std::reference_wrapper<const T2> rhs) const {
    return Equal::operator()(lhs, rhs.get());
  }
  template <typename T1, typename T2>
  bool operator()(std::reference_wrapper<const T1> lhs, const T2 &rhs) const {
    return Equal::operator()(lhs.get(), rhs);
  }
  template <typename T1, typename T2>
  bool operator()(const T1 &lhs, const T2 &rhs) const {
    return Equal::operator()(lhs, rhs);
  }
};

template <typename Equal>
RefEq(Equal &&) -> RefEq<std::remove_reference_t<Equal>>;

}  // namespace ray::utils::container
