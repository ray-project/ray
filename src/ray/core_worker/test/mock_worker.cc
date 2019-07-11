#define BOOST_BIND_NO_PLACEHOLDERS
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/test/constants.h"

using namespace std::placeholders;

namespace ray {

/// A mock C++ worker used by core_worker_test.cc to verify the task submission/execution
/// interfaces in both single node and cross-nodes scenarios. As the raylet client can
/// only
/// be called by a real worker process, core_worker_test.cc has to use this program binary
/// to start the actual worker process, in the test, the task submission interfaces are
/// called
/// in core_worker_test, and task execution interfaces are called in this file, see that
/// test
/// for more details on how this class is used.
class MockWorker {
 public:
  MockWorker(const std::string &store_socket, const std::string &raylet_socket)
      : worker_(WorkerType::WORKER, Language::PYTHON, store_socket, raylet_socket,
                JobID::FromRandom(),
                std::bind(&MockWorker::ExecuteTask, this, _1, _2, _3, _4, _5)) {}

  void Run() {
    // Start executing tasks.
    worker_.Execution().Run();
  }

 private:
  Status ExecuteTask(const RayFunction &ray_function,
                     const std::vector<std::shared_ptr<RayObject>> &args,
                     const TaskInfo &task_info, int num_returns,
                     std::vector<std::shared_ptr<Buffer>>* results) {
    // Note that this doesn't include dummy object id.
    RAY_CHECK(num_returns >= 0);

    if (ray_function.function_descriptor.size() == 1 &&
        ray_function.function_descriptor[0] == c_actor_creation_function_str) {
      // This is an actor creation task.
      RAY_CHECK(args.size() > 0);

RAY_LOG(INFO) << c_actor_creation_function_str;
      for (const auto &arg : args) {
        std::string object_id_str(reinterpret_cast<char*>(arg->GetData()->Data()),
            arg->GetData()->Size());
        auto object_id = ObjectID::FromBinary(object_id_str);

        std::vector<std::shared_ptr<ray::RayObject>> results;    
        RAY_CHECK_OK(worker_.Objects().Get({ object_id }, 0, &results));
             /* << "  " << results[0]->GetData()->Data()
              << "  " << results[0]->GetData()->Size(); */

        if (results.empty() || results[0] == nullptr) {

RAY_LOG(INFO) << "aaaaa";
          uint8_t array[] = {1};
          auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));
          RAY_CHECK_OK(worker_.Objects().Put(RayObject(buffer, nullptr), object_id));
          break;
        }
      }
    }

    // Merge all the content from input args.
    auto memory_buffer = std::make_shared<AccumulativeBuffer>();
    for (const auto &arg : args) {
      auto &data = arg->GetData();
      memory_buffer->Append(data->Data(), data->Size());
    }

    // Write the merged content to each of return ids.
    for (int i = 0; i < num_returns; i++) {
      results->push_back(memory_buffer);
    }
    return Status::OK();
  }

  CoreWorker worker_;
};

}  // namespace ray

int main(int argc, char **argv) {
  RAY_CHECK(argc == 3);
  auto store_socket = std::string(argv[1]);
  auto raylet_socket = std::string(argv[2]);

  ray::MockWorker worker(store_socket, raylet_socket);
  worker.Run();
  return 0;
}
