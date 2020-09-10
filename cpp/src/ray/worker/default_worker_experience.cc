#define BOOST_BIND_NO_PLACEHOLDERS
#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/util/logging.h>
#include <cstdlib>

using namespace ray::api;

/// general function of user code
int Return1() { return 1; }
int Plus1(int x) { return x + 1; }
int Plus(int x, int y) { return x + y; }

/// a class of user code
class Counter {
 public:
  int count;

  Counter(int init) { count = init; }

  static Counter *FactoryCreate(int init) { return new Counter(init); }
  /// non static function
  int Add(int x) {
    count += x;
    return count;
  }
};

int main(int argc, char **argv) {
  auto is_driver = std::getenv("IS_DRIVER");
  if (is_driver && memcmp(is_driver, "true", strlen(is_driver)) == 0) {
    RAY_LOG(INFO) << "Start cpp worker example";
    RAY_CHECK(argc == 2) << "input your lib path";

    /// initialization to cluster mode
    ray::api::RayConfig::GetInstance()->run_mode = RunMode::CLUSTER;
    /// Set redis ip to connect an existing ray cluster.
    ray::api::RayConfig::GetInstance()->redis_ip = "127.0.0.1";
    ray::api::RayConfig::GetInstance()->lib_name = argv[1];
    Ray::Init();

    /// put and get object
    auto obj = Ray::Put(123);
    auto get_result = *(obj.Get());

    RAY_LOG(INFO) << "Get result: " << get_result;

    auto r4 = Ray::Task(Plus1, 5).Remote();

    int result4 = *(r4.Get());

    std::cout << "Ray::call with reference results: "
              << " " << result4 << " " << std::endl;

    ActorHandle<Counter> actor = Ray::Actor(Counter::FactoryCreate, 1).Remote();
    auto r6 = actor.Task(&Counter::Add, 5).Remote();

    int result6 = *(r6.Get());

    std::cout << "Ray::call with actor results: " << result6 << std::endl;

    Ray::Shutdown();
    return 0;
  }

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
  RAY_LOG(INFO) << "redis ip: " << config->redis_ip
                << ", redis port: " << config->redis_port;
  config->redis_password = std::string(std::string(argv[5]));
  config->session_dir = std::string(std::string(argv[6]));

  Ray::Init();

  ::ray::CoreWorkerProcess::RunTaskExecutionLoop();
  return 0;
}
