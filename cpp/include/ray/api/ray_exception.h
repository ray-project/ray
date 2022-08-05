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

#include <exception>
#include <string>

namespace ray {
namespace internal {

class RayException : public std::exception {
 public:
  RayException(const std::string &msg) : msg_(msg){};

  const char *what() const noexcept override { return msg_.c_str(); };

  std::string msg_;
};

class RayActorException : public RayException {
 public:
  RayActorException(const std::string &msg) : RayException(msg){};
};

class RayTaskException : public RayException {
 public:
  RayTaskException(const std::string &msg) : RayException(msg){};
};

class RayWorkerException : public RayException {
 public:
  RayWorkerException(const std::string &msg) : RayException(msg){};
};

class UnreconstructableException : public RayException {
 public:
  UnreconstructableException(const std::string &msg) : RayException(msg){};
};

class RayFunctionNotFound : public RayException {
 public:
  RayFunctionNotFound(const std::string &msg) : RayException(msg){};
};

class RayRuntimeEnvException : public RayException {
 public:
  RayRuntimeEnvException(const std::string &msg) : RayException(msg){};
};
}  // namespace internal
}  // namespace ray