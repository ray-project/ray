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

#define BOOST_BIND_NO_PLACEHOLDERS
#include "ray/common/test_util.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

using namespace std::placeholders;

namespace ray {

int64_t prev_seq_no_ = 0;

Status GetWorkerPid(std::vector<std::shared_ptr<RayObject>> *results) {
  // Save the pid of current process to the return object.
  std::string pid_string = std::to_string(static_cast<int>(getpid()));
  auto data = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(pid_string.data()));
  auto memory_buffer = std::make_shared<LocalMemoryBuffer>(data, pid_string.size(), true);
  results->push_back(
      std::make_shared<RayObject>(memory_buffer, nullptr, std::vector<ObjectID>()));
  return Status::OK();
}

Status MergeInputArgsAsOutput(const std::vector<std::shared_ptr<RayObject>> &args,
                              const std::vector<ObjectID> &return_ids,
                              std::vector<std::shared_ptr<RayObject>> *results) {
  // Merge all the content from input args.
  std::vector<uint8_t> buffer;
  for (const auto &arg : args) {
    auto &data = arg->GetData();
    buffer.insert(buffer.end(), data->Data(), data->Data() + data->Size());
  }
  if (buffer.size() >= 8) {
    auto int_arr = reinterpret_cast<int64_t *>(buffer.data());
    if (int_arr[0] == SHOULD_CHECK_MESSAGE_ORDER) {
      auto seq_no = int_arr[1];
      if (seq_no > 0) {
        RAY_CHECK(seq_no == prev_seq_no_ + 1) << seq_no << " vs " << prev_seq_no_;
      }
      prev_seq_no_ = seq_no;
    }
  }
  auto memory_buffer =
      std::make_shared<LocalMemoryBuffer>(buffer.data(), buffer.size(), true);

  // Write the merged content to each of return ids.
  for (size_t i = 0; i < return_ids.size(); i++) {
    results->push_back(
        std::make_shared<RayObject>(memory_buffer, nullptr, std::vector<ObjectID>()));
  }

  return Status::OK();
}

/// A mock C++ worker used by core_worker_test.cc to verify the task submission/execution
/// interfaces in both single node and cross-nodes scenarios. As the raylet client can
/// only
/// be called by a real worker process, core_worker_test.cc has to use this program binary
/// to start the actual worker process, in the test, the task submission interfaces are
/// called
/// in core_worker_test, and task execution interfaces are called in this file, see that
/// test
/// for more details on how this class is used.
Status ExecuteTask(TaskType task_type, const RayFunction &ray_function,
                   const std::unordered_map<std::string, double> &required_resources,
                   const std::vector<std::shared_ptr<RayObject>> &args,
                   const std::vector<ObjectID> &arg_reference_ids,
                   const std::vector<ObjectID> &return_ids,
                   std::vector<std::shared_ptr<RayObject>> *results) {
  // Note that this doesn't include dummy object id.
  const ray::FunctionDescriptor function_descriptor =
      ray_function.GetFunctionDescriptor();
  RAY_CHECK(function_descriptor->Type() ==
            ray::FunctionDescriptorType::kPythonFunctionDescriptor);
  auto typed_descriptor = function_descriptor->As<ray::PythonFunctionDescriptor>();

  if ("actor creation task" == typed_descriptor->ModuleName()) {
    return Status::OK();
  } else if ("GetWorkerPid" == typed_descriptor->ModuleName()) {
    // Get mock worker pid
    return GetWorkerPid(results);
  } else if ("MergeInputArgsAsOutput" == typed_descriptor->ModuleName()) {
    // Merge input args and write the merged content to each of return ids
    return MergeInputArgsAsOutput(args, return_ids, results);
  } else {
    return Status::TypeError("Unknown function descriptor: " +
                             typed_descriptor->ModuleName());
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  RAY_CHECK(argc == 4);
  ray::CoreWorkerOptions options = {
      ray::WorkerType::WORKER,          // worker_type
      ray::Language::PYTHON,            // langauge
      std::string(argv[1]),             // store_socket
      std::string(argv[2]),             // raylet_socket
      JobID::FromInt(1),                // job_id
      {"127.0.0.1", 6379, ""},          // gcs_options
      "",                               // log_dir
      true,                             // install_failure_signal_handler
      "127.0.0.1",                      // node_ip_address
      std::stoi(std::string(argv[3])),  // node_manager_port
      &ray::ExecuteTask,                // task_execution_callback
      nullptr,                          // check_signals
      nullptr,                          // gc_collect
      nullptr,                          // get_lang_stack
      1,                                // ref_counting_enabled
      1,                                // num_workers
  };
  ray::CoreWorkerProcess::Initialize(options);
  ray::CoreWorkerProcess::StartExecutingTasks();
  return 0;
}
