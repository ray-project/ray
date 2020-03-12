#include "ray/util/test_util.h"

#include <functional>

#include "ray/util/logging.h"

namespace ray {

void RedisServiceManagerForTest::SetUpTestCase() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> random_gen{2000, 7000};
  // Use random port to avoid port conflicts between UTs.
  REDIS_SERVER_PORT = random_gen(gen);

  std::string start_redis_command =
      REDIS_SERVER_EXEC_PATH + " --loglevel warning --loadmodule " +
      REDIS_MODULE_LIBRARY_PATH + " --port " + std::to_string(REDIS_SERVER_PORT) + " &";
  RAY_LOG(INFO) << "Start redis command is: " << start_redis_command;
  RAY_CHECK(system(start_redis_command.c_str()) == 0);
  usleep(200 * 1000);
}

void RedisServiceManagerForTest::TearDownTestCase() {
  std::string stop_redis_command =
      REDIS_CLIENT_EXEC_PATH + " -p " + std::to_string(REDIS_SERVER_PORT) + " shutdown";
  RAY_LOG(INFO) << "Stop redis command is: " << stop_redis_command;
  RAY_CHECK(system(stop_redis_command.c_str()) == 0);
  usleep(100 * 1000);
}

bool WaitForCondition(std::function<bool()> condition, int timeout_ms) {
  int wait_time = 0;
  while (true) {
    if (condition()) {
      return true;
    }

    // sleep 10ms.
    const int wait_interval_ms = 10;
    usleep(wait_interval_ms * 1000);
    wait_time += wait_interval_ms;
    if (wait_time > timeout_ms) {
      break;
    }
  }
  return false;
}

/// Path to redis server executable binary.
std::string REDIS_SERVER_EXEC_PATH;
/// Path to redis client executable binary.
std::string REDIS_CLIENT_EXEC_PATH;
/// Path to redis module library.
std::string REDIS_MODULE_LIBRARY_PATH;
/// Port of redis server.
int REDIS_SERVER_PORT;

}  // namespace ray
