// Copyright 2020-2021 The Ray Authors.
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

#pragma once

#include <cmath>
#include <cstdint>
#include <iostream>

#define RESOURCE_UNIT_SCALING 10000

/// Fixed point data type.
class FixedPoint {
 private:
  int64_t i_ = 0;

 public:
  FixedPoint() : FixedPoint(0.0) {}
  FixedPoint(double d) { i_ = (uint64_t)(d * RESOURCE_UNIT_SCALING); }  // NOLINT

  FixedPoint(int i) { i_ = (i * RESOURCE_UNIT_SCALING); }  // NOLINT

  FixedPoint(uint32_t i) { i_ = (i * RESOURCE_UNIT_SCALING); }  // NOLINT

  FixedPoint(int64_t i) : FixedPoint((double)i) {}  // NOLINT

  FixedPoint(uint64_t i) : FixedPoint((double)i) {}  // NOLINT

  static FixedPoint Sum(const std::vector<FixedPoint> &list) {
    FixedPoint sum;
    for (auto &value : list) {
      sum += value;
    }
    return sum;
  }

  FixedPoint operator+(FixedPoint const &ru) const {
    FixedPoint res;
    res.i_ = i_ + ru.i_;
    return res;
  }

  FixedPoint &operator+=(FixedPoint const &ru) {
    i_ += ru.i_;
    return *this;
  }

  FixedPoint operator-(FixedPoint const &ru) const {
    FixedPoint res;
    res.i_ = i_ - ru.i_;
    return res;
  }

  FixedPoint &operator-=(FixedPoint const &ru) {
    i_ -= ru.i_;
    return *this;
  }

  FixedPoint operator-() const {
    FixedPoint res;
    res.i_ = -i_;
    return res;
  }

  FixedPoint operator+(double const d) const {
    FixedPoint res;
    res.i_ = i_ + static_cast<int64_t>(d * RESOURCE_UNIT_SCALING);
    return res;
  }

  FixedPoint operator-(double const d) const {
    FixedPoint res;
    res.i_ = i_ + static_cast<int64_t>(d * RESOURCE_UNIT_SCALING);
    return res;
  }

  FixedPoint operator=(double const d) {
    i_ = static_cast<int64_t>(d * RESOURCE_UNIT_SCALING);
    return *this;
  }

  FixedPoint operator+=(double const d) {
    i_ += static_cast<int64_t>(d * RESOURCE_UNIT_SCALING);
    return *this;
  }

  FixedPoint operator+=(int64_t const ru) {
    *this += static_cast<double>(ru);
    return *this;
  }

  bool operator<(FixedPoint const &ru1) const { return (i_ < ru1.i_); };
  bool operator>(FixedPoint const &ru1) const { return (i_ > ru1.i_); };
  bool operator<=(FixedPoint const &ru1) const { return (i_ <= ru1.i_); };
  bool operator>=(FixedPoint const &ru1) const { return (i_ >= ru1.i_); };
  bool operator==(FixedPoint const &ru1) const { return (i_ == ru1.i_); };
  bool operator!=(FixedPoint const &ru1) const { return (i_ != ru1.i_); };

  [[nodiscard]] double Double() const { return round(i_) / RESOURCE_UNIT_SCALING; };

  friend std::ostream &operator<<(std::ostream &out, FixedPoint const &ru1);
};

inline std::ostream &operator<<(std::ostream &out, FixedPoint const &ru1) {
  out << ru1.i_;
  return out;
}
