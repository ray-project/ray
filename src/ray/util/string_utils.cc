// Copyright 2025 The Ray Authors.
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

#include "ray/util/string_utils.h"

#include <cstdio>
#include <string>

namespace ray {

std::string StringToHex(const std::string &str) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (size_t i = 0; i < str.size(); i++) {
    unsigned char val = str[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

std::string ScanToken(std::string::const_iterator &c_str, std::string format) {
  int i = 0;
  std::string result;
  format += "%n";
  if (static_cast<size_t>(sscanf(&*c_str, format.c_str(), &i)) <= 1) {
    result.insert(result.end(), c_str, c_str + i);
    c_str += i;
  }
  return result;
}

std::string PrependToEachLine(const std::string &str, const std::string &prefix) {
  std::stringstream ss;
  ss << prefix;
  for (char c : str) {
    ss << c;
    if (c == '\n') {
      ss << prefix;
    }
  }
  return ss.str();
}

}  // namespace ray
