// Copyright 2017 The Ray Authors.
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

#include <iostream>
#include <memory>
#include <string>

namespace ray {
namespace api {

enum class RayLogLevel { DEBUG = -1, INFO = 0, WARNING = 1, ERROR = 2, FATAL = 3 };

#ifdef RAY_LOG
#undef RAY_LOG
#define RAY_LOG(level) \
  if (IsLevelEnabled(RayLogLevel::level)) RAY_LOG_INTERNAL(RayLogLevel::level)
#endif

#ifdef RAY_LOG_INTERNAL
#undef RAY_LOG_INTERNAL
#define RAY_LOG_INTERNAL(level) *CreateRayLog(__FILE__, __LINE__, level)
#endif

// To make the logging lib plugable with other logging libs and make
// the implementation unawared by the user, CppRayLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.
class RayLog {
 public:
  virtual ~RayLog(){};

  // By default, this class is a null log because it return false here.
  virtual bool IsEnabled() const = 0;

  template <typename T>
  RayLog &operator<<(const T &t) {
    if (IsEnabled()) {
      Stream() << t;
    }
    return *this;
  }

 protected:
  virtual std::ostream &Stream() = 0;
};

std::unique_ptr<RayLog> CreateRayLog(const char *file_name, int line_number,
                                     RayLogLevel severity);
bool IsLevelEnabled(RayLogLevel log_level);

}  // namespace api
}  // namespace ray
