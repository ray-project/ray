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

#include <cstdint>
#include <iostream>

#define RESOURCE_UNIT_SCALING 10000

/// Fixed point data type.
class FixedPoint {
 private:
  int64_t i_;

 public:
  FixedPoint(double d = 0);
  FixedPoint(int i);
  FixedPoint(uint32_t i);
  FixedPoint(int64_t i);
  FixedPoint(uint64_t i);

  FixedPoint operator+(FixedPoint const &ru) const;

  FixedPoint operator+=(FixedPoint const &ru);

  FixedPoint operator-(FixedPoint const &ru) const;

  FixedPoint operator-=(FixedPoint const &ru);

  FixedPoint operator-() const;

  FixedPoint operator+(double const d) const;

  FixedPoint operator-(double const d) const;

  FixedPoint operator=(double const d);

  FixedPoint operator+=(double const d);

  FixedPoint operator+=(int64_t const ru);

  bool operator<(FixedPoint const &ru1) const;
  bool operator>(FixedPoint const &ru1) const;
  bool operator<=(FixedPoint const &ru1) const;
  bool operator>=(FixedPoint const &ru1) const;
  bool operator==(FixedPoint const &ru1) const;
  bool operator!=(FixedPoint const &ru1) const;

  double Double() const;

  friend std::ostream &operator<<(std::ostream &out, FixedPoint const &ru1);
};
