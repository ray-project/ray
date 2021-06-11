
#include <gtest/gtest.h>
#include <ray/api.h>
#include "../../util/process_helper.h"
#include "gflags/gflags.h"

using namespace ::ray::api;

/// general function of user code
int Return1() { return 1; }
int Plus1(int x) { return x + 1; }
int Plus(int x, int y) { return x + y; }

RAY_REMOTE(Return1, Plus1, Plus);

/// a class of user code
class Counter {
 public:
  int count;

  Counter(int init) { count = init; }

  static Counter *FactoryCreate() { return new Counter(0); }
  static Counter *FactoryCreate(int init) { return new Counter(init); }
  static Counter *FactoryCreate(int init1, int init2) {
    return new Counter(init1 + init2);
  }
  /// non static function
  int Plus1() {
    count += 1;
    return count;
  }
  int Add(int x) {
    count += x;
    return count;
  }
};

RAY_REMOTE(RAY_FUNC(Counter::FactoryCreate), RAY_FUNC(Counter::FactoryCreate, int),
           RAY_FUNC(Counter::FactoryCreate, int, int), &Counter::Plus1, &Counter::Add);

int *cmd_argc = nullptr;
char ***cmd_argv = nullptr;

DEFINE_bool(external_cluster, false, "");
DEFINE_string(redis_password, "12345678", "");
DEFINE_int32(redis_port, 6379, "");
DEFINE_int32(node_manager_port, 62665, "");

TEST(RayClusterModeTest, FullTest) {
  ray::api::RayConfig config;
  if (FLAGS_external_cluster) {
    ProcessHelper::GetInstance().StartRayNode(FLAGS_redis_port, FLAGS_redis_password,
                                              FLAGS_node_manager_port);
    config.address = "127.0.0.1:" + std::to_string(FLAGS_redis_port);
    config.redis_password_ = FLAGS_redis_password;
  }
  Ray::Init(config, cmd_argc, cmd_argv);
  /// put and get object
  auto obj = Ray::Put(12345);
  auto get_result = *(Ray::Get(obj));
  EXPECT_EQ(12345, get_result);

  /// common task without args
  auto task_obj = Ray::Task(Return1).Remote();
  int task_result = *(Ray::Get(task_obj));
  EXPECT_EQ(1, task_result);

  /// common task with args
  task_obj = Ray::Task(Plus1).Remote(5);
  task_result = *(Ray::Get(task_obj));
  EXPECT_EQ(6, task_result);

  /// actor task without args
  ActorHandle<Counter> actor1 = Ray::Actor(RAY_FUNC(Counter::FactoryCreate)).Remote();
  auto actor_object1 = actor1.Task(&Counter::Plus1).Remote();
  int actor_task_result1 = *(Ray::Get(actor_object1));
  EXPECT_EQ(1, actor_task_result1);

  /// actor task with args
  ActorHandle<Counter> actor2 =
      Ray::Actor(RAY_FUNC(Counter::FactoryCreate, int)).Remote(1);
  auto actor_object2 = actor2.Task(&Counter::Add).Remote(5);
  int actor_task_result2 = *(Ray::Get(actor_object2));
  EXPECT_EQ(6, actor_task_result2);

  /// actor task with args which pass by reference
  ActorHandle<Counter> actor3 =
      Ray::Actor(RAY_FUNC(Counter::FactoryCreate, int, int)).Remote(6, 0);
  auto actor_object3 = actor3.Task(&Counter::Add).Remote(actor_object2);
  int actor_task_result3 = *(Ray::Get(actor_object3));
  EXPECT_EQ(12, actor_task_result3);

  /// general function remote call（args passed by value）
  auto r0 = Ray::Task(Return1).Remote();
  auto r1 = Ray::Task(Plus1).Remote(30);
  auto r2 = Ray::Task(Plus).Remote(3, 22);

  std::vector<ObjectRef<int>> objects = {r0, r1, r2};
  WaitResult<int> result = Ray::Wait(objects, 3, 1000);
  EXPECT_EQ(result.ready.size(), 3);
  EXPECT_EQ(result.unready.size(), 0);

  int result1 = *(Ray::Get(r1));
  int result0 = *(Ray::Get(r0));
  int result2 = *(Ray::Get(r2));
  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 31);
  EXPECT_EQ(result2, 25);

  /// general function remote call（args passed by reference）
  auto r3 = Ray::Task(Return1).Remote();
  auto r4 = Ray::Task(Plus1).Remote(r3);
  auto r5 = Ray::Task(Plus).Remote(r4, r3);
  auto r6 = Ray::Task(Plus).Remote(r4, 10);

  int result5 = *(Ray::Get(r5));
  int result4 = *(Ray::Get(r4));
  int result6 = *(Ray::Get(r6));
  int result3 = *(Ray::Get(r3));
  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result3, 1);
  EXPECT_EQ(result4, 2);
  EXPECT_EQ(result5, 3);
  EXPECT_EQ(result6, 12);

  /// create actor and actor function remote call with args passed by value
  ActorHandle<Counter> actor4 =
      Ray::Actor(RAY_FUNC(Counter::FactoryCreate, int)).Remote(10);
  auto r7 = actor4.Task(&Counter::Add).Remote(5);
  auto r8 = actor4.Task(&Counter::Add).Remote(1);
  auto r9 = actor4.Task(&Counter::Add).Remote(3);
  auto r10 = actor4.Task(&Counter::Add).Remote(8);

  int result7 = *(Ray::Get(r7));
  int result8 = *(Ray::Get(r8));
  int result9 = *(Ray::Get(r9));
  int result10 = *(Ray::Get(r10));
  EXPECT_EQ(result7, 15);
  EXPECT_EQ(result8, 16);
  EXPECT_EQ(result9, 19);
  EXPECT_EQ(result10, 27);

  /// create actor and task function remote call with args passed by reference
  ActorHandle<Counter> actor5 =
      Ray::Actor(RAY_FUNC(Counter::FactoryCreate, int, int)).Remote(r10, 0);

  auto r11 = actor5.Task(&Counter::Add).Remote(r0);
  auto r12 = actor5.Task(&Counter::Add).Remote(r11);
  auto r13 = actor5.Task(&Counter::Add).Remote(r10);
  auto r14 = actor5.Task(&Counter::Add).Remote(r13);
  auto r15 = Ray::Task(Plus).Remote(r0, r11);
  auto r16 = Ray::Task(Plus1).Remote(r15);

  int result12 = *(Ray::Get(r12));
  int result14 = *(Ray::Get(r14));
  int result11 = *(Ray::Get(r11));
  int result13 = *(Ray::Get(r13));
  int result16 = *(Ray::Get(r16));
  int result15 = *(Ray::Get(r15));

  EXPECT_EQ(result11, 28);
  EXPECT_EQ(result12, 56);
  EXPECT_EQ(result13, 83);
  EXPECT_EQ(result14, 166);
  EXPECT_EQ(result15, 29);
  EXPECT_EQ(result16, 30);

  Ray::Shutdown();

  if (FLAGS_external_cluster) {
    ProcessHelper::GetInstance().StopRayNode();
  }
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  cmd_argc = &argc;
  cmd_argv = &argv;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
