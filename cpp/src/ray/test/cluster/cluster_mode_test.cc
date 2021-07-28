
#include <gtest/gtest.h>
#include <ray/api.h>
#include "../../util/process_helper.h"
#include "counter.h"
#include "gflags/gflags.h"
#include "plus.h"

using namespace ray;

int *cmd_argc = nullptr;
char ***cmd_argv = nullptr;

DEFINE_bool(external_cluster, false, "");
DEFINE_string(redis_password, "12345678", "");
DEFINE_int32(redis_port, 6379, "");

TEST(RayClusterModeTest, FullTest) {
  ray::RayConfig config;
  if (FLAGS_external_cluster) {
    ray::runtime::ProcessHelper::GetInstance().StartRayNode(FLAGS_redis_port,
                                                            FLAGS_redis_password);
    config.address = "127.0.0.1:" + std::to_string(FLAGS_redis_port);
    config.redis_password_ = FLAGS_redis_password;
  }
  ray::Init(config, cmd_argc, cmd_argv);
  /// put and get object
  auto obj = ray::Put(12345);
  auto get_result = *(ray::Get(obj));
  EXPECT_EQ(12345, get_result);

  /// common task without args
  auto task_obj = ray::Task(Return1).Remote();
  int task_result = *(ray::Get(task_obj));
  EXPECT_EQ(1, task_result);

  /// common task with args
  task_obj = ray::Task(Plus1).Remote(5);
  task_result = *(ray::Get(task_obj));
  EXPECT_EQ(6, task_result);

  /// actor task without args
  ActorHandle<Counter> actor1 = ray::Actor(RAY_FUNC(Counter::FactoryCreate)).Remote();
  auto actor_object1 = actor1.Task(&Counter::Plus1).Remote();
  int actor_task_result1 = *(ray::Get(actor_object1));
  EXPECT_EQ(1, actor_task_result1);

  /// actor task with args
  ActorHandle<Counter> actor2 =
      ray::Actor(RAY_FUNC(Counter::FactoryCreate, int)).Remote(1);
  auto actor_object2 = actor2.Task(&Counter::Add).Remote(5);
  int actor_task_result2 = *(ray::Get(actor_object2));
  EXPECT_EQ(6, actor_task_result2);

  /// actor task with args which pass by reference
  ActorHandle<Counter> actor3 =
      ray::Actor(RAY_FUNC(Counter::FactoryCreate, int, int)).Remote(6, 0);
  auto actor_object3 = actor3.Task(&Counter::Add).Remote(actor_object2);
  int actor_task_result3 = *(ray::Get(actor_object3));
  EXPECT_EQ(12, actor_task_result3);

  /// general function remote call（args passed by value）
  auto r0 = ray::Task(Return1).Remote();
  auto r1 = ray::Task(Plus1).Remote(30);
  auto r2 = ray::Task(Plus).Remote(3, 22);

  std::vector<ray::ObjectRef<int>> objects = {r0, r1, r2};
  WaitResult<int> result = ray::Wait(objects, 3, 1000);
  EXPECT_EQ(result.ready.size(), 3);
  EXPECT_EQ(result.unready.size(), 0);

  int result1 = *(ray::Get(r1));
  int result0 = *(ray::Get(r0));
  int result2 = *(ray::Get(r2));
  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 31);
  EXPECT_EQ(result2, 25);

  /// general function remote call（args passed by reference）
  auto r3 = ray::Task(Return1).Remote();
  auto r4 = ray::Task(Plus1).Remote(r3);
  auto r5 = ray::Task(Plus).Remote(r4, r3);
  auto r6 = ray::Task(Plus).Remote(r4, 10);

  int result5 = *(ray::Get(r5));
  int result4 = *(ray::Get(r4));
  int result6 = *(ray::Get(r6));
  int result3 = *(ray::Get(r3));
  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result3, 1);
  EXPECT_EQ(result4, 2);
  EXPECT_EQ(result5, 3);
  EXPECT_EQ(result6, 12);

  /// create actor and actor function remote call with args passed by value
  ActorHandle<Counter> actor4 =
      ray::Actor(RAY_FUNC(Counter::FactoryCreate, int)).Remote(10);
  auto r7 = actor4.Task(&Counter::Add).Remote(5);
  auto r8 = actor4.Task(&Counter::Add).Remote(1);
  auto r9 = actor4.Task(&Counter::Add).Remote(3);
  auto r10 = actor4.Task(&Counter::Add).Remote(8);

  int result7 = *(ray::Get(r7));
  int result8 = *(ray::Get(r8));
  int result9 = *(ray::Get(r9));
  int result10 = *(ray::Get(r10));
  EXPECT_EQ(result7, 15);
  EXPECT_EQ(result8, 16);
  EXPECT_EQ(result9, 19);
  EXPECT_EQ(result10, 27);

  /// create actor and task function remote call with args passed by reference
  ActorHandle<Counter> actor5 =
      ray::Actor(RAY_FUNC(Counter::FactoryCreate, int, int)).Remote(r10, 0);

  auto r11 = actor5.Task(&Counter::Add).Remote(r0);
  auto r12 = actor5.Task(&Counter::Add).Remote(r11);
  auto r13 = actor5.Task(&Counter::Add).Remote(r10);
  auto r14 = actor5.Task(&Counter::Add).Remote(r13);
  auto r15 = ray::Task(Plus).Remote(r0, r11);
  auto r16 = ray::Task(Plus1).Remote(r15);

  int result12 = *(ray::Get(r12));
  int result14 = *(ray::Get(r14));
  int result11 = *(ray::Get(r11));
  int result13 = *(ray::Get(r13));
  int result16 = *(ray::Get(r16));
  int result15 = *(ray::Get(r15));

  EXPECT_EQ(result11, 28);
  EXPECT_EQ(result12, 56);
  EXPECT_EQ(result13, 83);
  EXPECT_EQ(result14, 166);
  EXPECT_EQ(result15, 29);
  EXPECT_EQ(result16, 30);

  ray::Shutdown();

  if (FLAGS_external_cluster) {
    ray::runtime::ProcessHelper::GetInstance().StopRayNode();
  }
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  cmd_argc = &argc;
  cmd_argv = &argv;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
