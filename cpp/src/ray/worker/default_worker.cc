#define BOOST_BIND_NO_PLACEHOLDERS
#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/util/logging.h>

using namespace std::placeholders;
using namespace ray::api;
namespace ray {

namespace api {

// class DefaultWorker {
//  public:
//   DefaultWorker(const std::string &store_socket, const std::string &raylet_socket,
//                 int node_manager_port, const gcs::GcsClientOptions &gcs_options,
//                 const std::string &session_dir) {
//     CoreWorkerOptions options = {
//         WorkerType::WORKER,     // worker_type
//         Language::CPP,          // langauge
//         store_socket,           // store_socket
//         raylet_socket,          // raylet_socket
//         JobID::FromInt(1),      // job_id
//         gcs_options,            // gcs_options
//         true,                   // enable_logging
//         session_dir + "/logs",  // log_dir
//         true,                   // install_failure_signal_handler
//         "127.0.0.1",            // node_ip_address
//         node_manager_port,      // node_manager_port
//         "127.0.0.1",            // raylet_ip_address
//         "",                     // driver_name
//         "",                     // stdout_file
//         "",                     // stderr_file
//         std::bind(&DefaultWorker::ExecuteTask, this, _1, _2, _3, _4, _5, _6,
//                   _7),  // task_execution_callback
//         nullptr,        // check_signals
//         nullptr,        // gc_collect
//         nullptr,        // get_lang_stack
//         nullptr,        // kill_main
//         true,           // ref_counting_enabled
//         false,          // is_local_mode
//         1,              // num_workers
//     };
//     CoreWorkerProcess::Initialize(options);
//   }

//   void RunTaskExecutionLoop() { CoreWorkerProcess::RunTaskExecutionLoop(); }

//  private:
//   Status ExecuteTask(TaskType task_type, const RayFunction &ray_function,
//                      const std::unordered_map<std::string, double> &required_resources,
//                      const std::vector<std::shared_ptr<RayObject>> &args,
//                      const std::vector<ObjectID> &arg_reference_ids,
//                      const std::vector<ObjectID> &return_ids,
//                      std::vector<std::shared_ptr<RayObject>> *results) {
//     /// TODO(Guyang Song): Make task execution worked.
//     return Status::TypeError("Task executor not implemented");
//   }
// };
}  // namespace api
}  // namespace ray

int main(int argc, char **argv) {
  RAY_LOG(INFO) << "CPP default worker started";
  RAY_CHECK(argc == 7);

  auto config = ray::api::RayConfig::GetInstance();
  config->run_mode = RunMode::CLUSTER;
  config->worker_type = ray::WorkerType::WORKER;
  config->store_socket = std::string(argv[1]);
  config->raylet_socket = std::string(argv[2]);
  config->node_manager_port = std::stoi(std::string(argv[3]));
  std::string redis_address = std::string(std::string(argv[4]));
  auto pos = redis_address.find(':');
  RAY_CHECK(pos != std::string::npos);
  config->redis_ip = redis_address.substr(0, pos);
  config->redis_port = std::stoi(redis_address.substr(pos + 1, redis_address.length()));
  RAY_LOG(INFO) << "redis ip: " << config->redis_ip << ", redis port: " << config->redis_port;
  config->redis_password = std::string(std::string(argv[5]));
  config->session_dir = std::string(std::string(argv[6]));

  Ray::Init();

  ::ray::CoreWorkerProcess::RunTaskExecutionLoop();

  // ray::gcs::GcsClientOptions gcs_options("127.0.0.1", 6379, config->redis_password);
  // ray::api::DefaultWorker worker(config->store_socket, config->raylet_socket, config->node_manager_port,
  //                                gcs_options, config->session_dir);
  // worker.RunTaskExecutionLoop();
  return 0;
}
