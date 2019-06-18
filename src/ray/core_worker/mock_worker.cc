#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/task_execution.h"

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
      : worker_(WorkerType::WORKER, WorkerLanguage::PYTHON, store_socket, raylet_socket,
                DriverID::FromRandom()) {}

  void Run() {
    auto executor_func = [this](const RayFunction &ray_function,
                                const std::vector<std::shared_ptr<Buffer>> &args,
                                const TaskID &task_id, int num_returns) {
      // Note that this doesn't include dummy object id.
      RAY_CHECK(num_returns >= 0);

      // Merge all the content from input args.
      std::vector<uint8_t> buffer;
      for (const auto &arg : args) {
        buffer.insert(buffer.end(), arg->Data(), arg->Data() + arg->Size());
      }

      LocalMemoryBuffer memory_buffer(buffer.data(), buffer.size());

      // Write the merged content to each of return ids.
      for (int i = 0; i < num_returns; i++) {
        ObjectID id = ObjectID::ForTaskReturn(task_id, i + 1);
        RAY_CHECK_OK(worker_.Objects().Put(memory_buffer, id));
      }
      return Status::OK();
    };

    // Start executing tasks.
    worker_.Execution().Run(executor_func);
  }

 private:
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
