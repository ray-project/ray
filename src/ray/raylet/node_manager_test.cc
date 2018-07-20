#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/node_manager.h"
#include "ray/raylet/task.h"
#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

class NodeManagerTest : public ::testing::Test {

};

TEST_F(NodeManagerTest, TokenBucket) {
  TokenBucket token_bucket;
  // set a time interval twice as shorter as mx heartbeat interval
  // int64_t interval_ms = 1000./(2*RayConfig::instance().token_bucket_max_rate_hz());
  int64_t timeout_ms;
  int64_t last_heartbeat;
  int64_t now;
  uint64_t start_experiment;
  int hb_count = 0;
  int expected_hb_count;
  int error;

  last_heartbeat = start_experiment = current_time_ms();

  for (int i = 0; i < 50; i++) {
    bool send_heartbeat;
    timeout_ms = token_bucket.Timeout(&send_heartbeat);

    if (send_heartbeat) {
      // sent heartbeat
      hb_count++;
      now = current_time_ms();
      last_heartbeat = now;
    }
    usleep(timeout_ms * 1000);
  }
  expected_hb_count = (current_time_ms() - start_experiment)*RayConfig::instance().token_bucket_min_rate_hz()/1000;
  // allow a 10% error
  error = hb_count * 10/100;
  ASSERT_TRUE((hb_count >= expected_hb_count - error) && (hb_count <= expected_hb_count + error));

  hb_count = 0;
  last_heartbeat = start_experiment = current_time_ms();

  for (int i = 0; i < 50; i++) {
    bool send_heartbeat;
    timeout_ms = token_bucket.TimeoutResourceAvailable(&send_heartbeat);

    if (send_heartbeat) {
      // sent heartbeat
      now = current_time_ms();
      last_heartbeat = now;
      hb_count++;
    }
    usleep(timeout_ms * 1000);
  }
  expected_hb_count = RayConfig::instance().token_bucket_max_capacity() +
      (current_time_ms() - start_experiment)*RayConfig::instance().token_bucket_avg_rate_hz()/1000.;
  // allow a 10% error
  error = hb_count * 10/100;
  ASSERT_TRUE((hb_count >= expected_hb_count - error) && (hb_count <= expected_hb_count + error));
};


}  // raylet

}  // ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
