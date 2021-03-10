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

#include "ray/common/runtime_env_manager.h"
#include "ray/raylet/worker_pool.h"

namespace ray {
namespace raylet {

class RuntimeEnvManager : public RuntimeEnvManagerBase {
 public:
  RuntimeEnvManager(IOWorkerPoolInterface& io_worker_pool)
      : io_worker_pool_(io_worker_pool) {}
 protected:
  void DeleteURI(const std::string& uri) override;
 private:
  IOWorkerPoolInterface &io_worker_pool_;
};

}
}
