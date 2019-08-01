#define BOOST_BIND_NO_PLACEHOLDERS
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/core_worker/task_execution.h"
#include "src/ray/util/test_util.h"

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
  MockWorker(const std::string &store_socket, const std::string &raylet_socket,
             const gcs::GcsClientOptions &gcs_options)
      : worker_(WorkerType::WORKER, Language::PYTHON, store_socket, raylet_socket,
                JobID::FromInt(1), gcs_options,
                std::bind(&MockWorker::ExecuteTask, this, _1, _2, _3, _4, _5)) {}

  void Run() {
    // Start executing tasks.
    worker_.Execution().Run();
  }

 private:
  Status ExecuteTask(const RayFunction &ray_function,
                     const std::vector<std::shared_ptr<RayObject>> &args,
                     const TaskInfo &task_info, int num_returns,
                     std::vector<std::shared_ptr<RayObject>> *results) {
    // Note that this doesn't include dummy object id.
    RAY_CHECK(num_returns >= 0);

    // Merge all the content from input args.
    std::vector<uint8_t> buffer;
    for (const auto &arg : args) {
      auto &data = arg->GetData();
      buffer.insert(buffer.end(), data->Data(), data->Data() + data->Size());
    }
    auto memory_buffer =
        std::make_shared<LocalMemoryBuffer>(buffer.data(), buffer.size(), true);

    // Write the merged content to each of return ids.
    for (int i = 0; i < num_returns; i++) {
      results->push_back(std::make_shared<RayObject>(memory_buffer, nullptr));
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

  ray::gcs::GcsClientOptions gcs_options("127.0.0.1", 6379, "");
  ray::MockWorker worker(store_socket, raylet_socket, gcs_options);
  worker.Run();
  return 0;
}
