#include "task_execution.h"
#include "context.h"
#include "core_worker.h"

namespace ray {

class MockWorker {
 public:
  MockWorker(const std::string &store_socket,
             const std::string &raylet_socket)
    : worker_(WorkerType::WORKER, Language::PYTHON,
              store_socket, raylet_socket,
              DriverID::FromRandom()) {}
  
  void Run() {
    auto executor_func = [this] (const RayFunction &ray_function,
                             const std::vector<std::shared_ptr<Buffer>> &args,
                             int num_returns) {
      RAY_CHECK(num_returns > 0);

      // Merge all the content from input args.
      std::vector<uint8_t> buffer;
      for (const auto & arg : args) {
        buffer.insert(buffer.end(), arg->Data(), arg->Data() + arg->Size());
      }

      LocalMemoryBuffer memory_buffer(buffer.data(), buffer.size());

      // Write the merged content to each of return ids.
      for (int i  = 0; i < num_returns; i++) {
        ObjectID id;
        RAY_CHECK_OK(worker_.Objects().Put(memory_buffer, &id));
      }
      return Status::OK();
    };

    // Start executing tasks.
    worker_.Execution().Start(executor_func);
  }
 private:
  CoreWorker worker_;
};

} // namespace ray

int main(int argc, char **argv) {

  RAY_CHECK(argc == 3);
  auto store_socket = std::string(argv[1]);
  auto raylet_socket = std::string(argv[2]);

  ray::MockWorker worker(store_socket, raylet_socket);
  worker.Run();
  return 0;
}