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

#include <ray/api/logging.h>
#include <ray/util/logging.h>

namespace ray {

class RayLoggerImpl : public RayLogger, public ray::RayLog {
 public:
  RayLoggerImpl(const char *file_name, int line_number, RayLoggerLevel severity)
      : ray::RayLog(file_name, line_number, (ray::RayLogLevel)severity) {}
  bool IsEnabled() const override { return ray::RayLog::IsEnabled(); }

  std::ostream &Stream() override { return ray::RayLog::Stream(); }
};

std::unique_ptr<RayLogger> CreateRayLogger(const char *file_name,
                                           int line_number,
                                           RayLoggerLevel severity) {
  return std::make_unique<RayLoggerImpl>(file_name, line_number, severity);
}

bool IsLevelEnabled(RayLoggerLevel log_level) {
  return ray::RayLog::IsLevelEnabled((ray::RayLogLevel)log_level);
}

}  // namespace ray
