#define BOOST_BIND_NO_PLACEHOLDERS
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

using namespace std::placeholders;

namespace ray {

namespace api {

class DefaultWorker {
 public:
  DefaultWorker(const std::string &store_socket, const std::string &raylet_socket,
                int node_manager_port, const gcs::GcsClientOptions &gcs_options,
                const std::string &session_dir) {
    CoreWorkerOptions options = {
        WorkerType::WORKER,     // worker_type
        Language::CPP,          // langauge
        store_socket,           // store_socket
        raylet_socket,          // raylet_socket
        JobID::FromInt(1),      // job_id
        gcs_options,            // gcs_options
        true,                   // enable_logging
        session_dir + "/logs",  // log_dir
        true,                   // install_failure_signal_handler
        "127.0.0.1",            // node_ip_address
        node_manager_port,      // node_manager_port
        "127.0.0.1",            // raylet_ip_address
        "",                     // driver_name
        "",                     // stdout_file
        "",                     // stderr_file
        std::bind(&DefaultWorker::ExecuteTask, this, _1, _2, _3, _4, _5, _6, _7,
                  _8),  // task_execution_callback
        nullptr,        // check_signals
        nullptr,        // gc_collect
        nullptr,        // spill_objects
        nullptr,        // restore_spilled_objects
        nullptr,        // get_lang_stack
        nullptr,        // kill_main
        true,           // ref_counting_enabled
        false,          // is_local_mode
        1,              // num_workers
        nullptr,        // terminate_asyncio_thread
        "",             // serialized_job_config
        -1,             // metrics_agent_port
    };

    CoreWorkerProcess::Initialize(options);
  }

  void RunTaskExecutionLoop() { CoreWorkerProcess::RunTaskExecutionLoop(); }

 private:
  Status ExecuteTask(TaskType task_type, const std::string task_name,
                     const RayFunction &ray_function,
                     const std::unordered_map<std::string, double> &required_resources,
                     const std::vector<std::shared_ptr<RayObject>> &args,
                     const std::vector<ObjectID> &arg_reference_ids,
                     const std::vector<ObjectID> &return_ids,
                     std::vector<std::shared_ptr<RayObject>> *results) {
    /// TODO(Guyang Song): Make task execution worked.
    return Status::TypeError("Task executor not implemented");
  }
};
}  // namespace api
}  // namespace ray

int main(int argc, char **argv) {
  RAY_LOG(INFO) << "CPP default worker started";

  RAY_CHECK(argc == 6);
  auto store_socket = std::string(argv[1]);
  auto raylet_socket = std::string(argv[2]);
  auto node_manager_port = std::stoi(std::string(argv[3]));
  auto redis_password = std::string(std::string(argv[4]));
  auto session_dir = std::string(std::string(argv[5]));

  /// TODO(Guyang Song): Delete this hard code and get address from redis.
  ray::gcs::GcsClientOptions gcs_options("127.0.0.1", 6379, redis_password);
  ray::api::DefaultWorker worker(store_socket, raylet_socket, node_manager_port,
                                 gcs_options, session_dir);
  worker.RunTaskExecutionLoop();
  return 0;
}
