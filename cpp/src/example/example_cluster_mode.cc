
/// This is a complete example of writing a distributed program using the C ++ worker API.

/// including the header
#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/util/logging.h>

/// using namespace
using namespace ray::api;

int main(int argc, char **argv) {
  RAY_LOG(INFO) << "Start cpp worker example";

  /// initialization to cluster mode
  ray::api::RayConfig::GetInstance()->run_mode = RunMode::CLUSTER;
  /// Set redis ip to connect an existing ray cluster.
  /// ray::api::RayConfig::GetInstance()->redis_ip = "127.0.0.1";
  Ray::Init();

  /// put and get object
  auto obj = Ray::Put(123);
  auto get_result = *(obj.Get());

  RAY_LOG(INFO) << "Get result: " << get_result;
  Ray::Shutdown();
}
