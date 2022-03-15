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

#include <sstream>

#include "ray/raylet/scheduling/fixed_point.h"

/// Helper function to compare two vectors with FixedPoint values.
bool FixedPointEqualVectors(const std::vector<FixedPoint> &v1, const std::vector<FixedPoint> &v2) {
  return (v1.size() == v2.size() && std::equal(v1.begin(), v1.end(), v2.begin()));
}

/// Convert a vector of doubles to a vector of resource units.
std::vector<FixedPoint> VectorDoubleToVectorFixedPoint(const std::vector<double> &vector) {
  std::vector<FixedPoint> vector_fp(vector.size());
  for (size_t i = 0; i < vector.size(); i++) {
    vector_fp[i] = vector[i];
  }
  return vector_fp;
}

/// Convert a vector of resource units to a vector of doubles.
std::vector<double> VectorFixedPointToVectorDouble(const std::vector<FixedPoint> &vector_fp) {
  std::vector<double> vector(vector_fp.size());
  for (size_t i = 0; i < vector_fp.size(); i++) {
    vector[i] = FixedPoint(vector_fp[i]).Double();
  }
  return vector;
}

std::string FixedPointVectorToString(const std::vector<FixedPoint> &vector) {
  std::stringstream buffer;
  buffer << "[";
  for (size_t i = 0; i < vector.size(); i++) {
    buffer << vector[i];
    if (i < vector.size() - 1) {
      buffer << ", ";
    }
  }
  buffer << "]";
  return buffer.str();
}
