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

#include "ray/util/process_factory.h"

#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/util/fake_process.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

namespace ray {

std::unique_ptr<ProcessInterface> ProcessFactory::CreateNull() {
  return std::make_unique<Process>();
}

std::unique_ptr<ProcessInterface> ProcessFactory::CreateNewDummy() {
  return std::make_unique<FakeProcess>();
}

std::unique_ptr<ProcessInterface> ProcessFactory::FromPid(pid_t pid) {
  return std::make_unique<Process>(pid);
}

std::unique_ptr<ProcessInterface> ProcessFactory::Create(
    const char *argv[],
    void *io_service,
    std::error_code &ec,
    bool decouple,
    const ProcessEnvironment &env,
    bool pipe_to_stdin,
    std::function<void(const std::string &)> add_to_cgroup_hook,
    bool new_process_group) {
  return std::make_unique<Process>(argv,
                                   io_service,
                                   ec,
                                   decouple,
                                   env,
                                   pipe_to_stdin,
                                   std::move(add_to_cgroup_hook),
                                   new_process_group);
}

std::pair<std::unique_ptr<ProcessInterface>, std::error_code> ProcessFactory::Spawn(
    const std::vector<std::string> &args,
    bool decouple,
    const std::string &pid_file,
    const ProcessEnvironment &env,
    bool new_process_group) {
  std::vector<const char *> argv;
  argv.reserve(args.size() + 1);
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code error;
  std::unique_ptr<ProcessInterface> proc = std::make_unique<Process>(
      &*argv.begin(),
      nullptr,
      error,
      decouple,
      env,
      /*pipe_to_stdin=*/false,
      /*add_to_cgroup*/ [](const std::string &) {},
      new_process_group);
  if (!error && !pid_file.empty()) {
    std::ofstream file(pid_file, std::ios_base::out | std::ios_base::trunc);
    file << proc->GetId() << std::endl;
    RAY_CHECK(file.good());
  }
  return {std::move(proc), error};
}

std::error_code ProcessFactory::Call(const std::vector<std::string> &args,
                                     const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code ec;
  Process proc(&*argv.begin(), nullptr, ec, true, env);
  if (!ec) {
    int return_code = proc.Wait();
    if (return_code != 0) {
      ec = std::error_code(return_code, std::system_category());
    }
  }
  return ec;
}

}  // namespace ray
