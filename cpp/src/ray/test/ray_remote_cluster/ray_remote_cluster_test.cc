#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/ray_config.h>

using namespace ::ray::api;

/// general function of user code
int Return1() { return 1; }
int Plus1(int x) { return x + 1; }
int Plus(int x, int y) { return x + y; }

RAY_REMOTE(Return1);
RAY_REMOTE(Plus1);
RAY_REMOTE(Plus);

std::string lib_name = "";

std::string redis_ip = "";

TEST(RayClusterModeTest, FullTest) {
  /// initialization to cluster mode
  ray::api::RayConfig::GetInstance()->run_mode = RunMode::CLUSTER;
  ray::api::RayConfig::GetInstance()->lib_name = lib_name;
  ray::api::RayConfig::GetInstance()->redis_ip = redis_ip;
  Ray::Init();

  ray::api::RayConfig::GetInstance()->use_ray_remote = true;

  /// common task without args
  auto task_obj = Ray::Task(Return1).Remote();
  int task_result = *(Ray::Get(task_obj));
  EXPECT_EQ(1, task_result);

  /// common task with args
  task_obj = Ray::Task(Plus1, 5).Remote();
  task_result = *(Ray::Get(task_obj));
  EXPECT_EQ(6, task_result);

  /// general function remote call（args passed by value）
  auto r0 = Ray::Task(Return1).Remote();
  auto r1 = Ray::Task(Plus1, 30).Remote();
  auto r2 = Ray::Task(Plus, 3, 22).Remote();

  int result1 = *(Ray::Get(r1));
  int result0 = *(Ray::Get(r0));
  int result2 = *(Ray::Get(r2));
  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 31);
  EXPECT_EQ(result2, 25);

  /// general function remote call（args passed by reference）
  auto r3 = Ray::Task(Return1).Remote();
  auto r4 = Ray::Task(Plus1, r3).Remote();
  auto r5 = Ray::Task(Plus, r4, r3).Remote();
  auto r6 = Ray::Task(Plus, r4, 10).Remote();

  int result5 = *(Ray::Get(r5));
  int result4 = *(Ray::Get(r4));
  int result6 = *(Ray::Get(r6));
  int result3 = *(Ray::Get(r3));
  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result3, 1);
  EXPECT_EQ(result4, 2);
  EXPECT_EQ(result5, 3);
  EXPECT_EQ(result6, 12);
  ray::api::RayConfig::GetInstance()->use_ray_remote = false;
  Ray::Shutdown();
}

int main(int argc, char **argv) {
  RAY_CHECK(argc == 2 || argc == 3);
  lib_name = std::string(argv[1]);
  if (argc == 3) {
    redis_ip = std::string(argv[2]);
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}