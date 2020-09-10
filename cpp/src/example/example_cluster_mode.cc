
/// This is a complete example of writing a distributed program using the C ++ worker API.

/// including the header
#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/util/logging.h>

/// using namespace
using namespace ray::api;

/// general function of user code
int Return1() { return 1; }
int Plus1(int x) { return x + 1; }
int Plus(int x, int y) { return x + y; }

int main(int argc, char **argv) {
  RAY_LOG(INFO) << "Start cpp worker example";

  /// initialization to cluster mode
  ray::api::RayConfig::GetInstance()->run_mode = RunMode::CLUSTER;
  /// Set redis ip to connect an existing ray cluster.
  ray::api::RayConfig::GetInstance()->redis_ip = "127.0.0.1";
  ray::api::RayConfig::GetInstance()->lib_name =
      "/Users/jiulong/arcos/opensource/ant/ray/bazel-bin/cpp/example_cluster_mode.so";
  Ray::Init();

  /// put and get object
  auto obj = Ray::Put(123);
  auto get_result = *(obj.Get());

  RAY_LOG(INFO) << "Get result: " << get_result;

  /// general function remote call（args passed by reference）
  // auto r3 = Ray::Task(Return1).Remote();
  auto r4 = Ray::Task(Plus1, 5).Remote();
  // auto r5 = Ray::Task(Plus, r4, 1).Remote();

  // int result3 = *(r3.Get());
  int result4 = *(r4.Get());
  // int result5 = *(r5.Get());

  std::cout << "Ray::call with reference results: "
            << " " << result4 << " " << std::endl;
  Ray::Shutdown();
}
