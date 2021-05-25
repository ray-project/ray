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

class DummyObject {
 public:
  int count;

  MSGPACK_DEFINE(count);
  DummyObject() { count = 0; };
  DummyObject(int init) { count = init; }

  static DummyObject *FactoryCreate(int init) { return new DummyObject(init); }

  int Add(int x) {
    count += x;
    return count;
  }
};
RAY_REMOTE(DummyObject::FactoryCreate);
RAY_REMOTE(&DummyObject::Add);

std::string lib_name = "";

std::string redis_ip = "";

TEST(RayClusterModeTest, FullTest) {
  /// initialization to cluster mode
  ray::api::RayConfig::GetInstance()->run_mode = RunMode::CLUSTER;
  ray::api::RayConfig::GetInstance()->lib_name = lib_name;
  ray::api::RayConfig::GetInstance()->redis_ip = redis_ip;
  Ray::Init();

  /// common task without args
  auto task_obj = Ray::Task(Return1).Remote();
  int task_result = *(Ray::Get(task_obj));
  EXPECT_EQ(1, task_result);

  /// common task with args
  task_obj = Ray::Task(Plus1).Remote(5);
  task_result = *(Ray::Get(task_obj));
  EXPECT_EQ(6, task_result);

  /// general function remote call（args passed by value）
  auto r0 = Ray::Task(Return1).Remote();
  auto r1 = Ray::Task(Plus1).Remote(30);
  auto r2 = Ray::Task(Plus).Remote(3, 22);

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
  ActorHandle<DummyObject> actor4 = Ray::Actor(DummyObject::FactoryCreate).Remote(10);
  auto r7 = actor4.Task(&DummyObject::Add).Remote(5);
  auto r8 = actor4.Task(&DummyObject::Add).Remote(1);
  auto r9 = actor4.Task(&DummyObject::Add).Remote(3);
  auto r10 = actor4.Task(&DummyObject::Add).Remote(8);

  int result7 = *(Ray::Get(r7));
  int result8 = *(Ray::Get(r8));
  int result9 = *(Ray::Get(r9));
  int result10 = *(Ray::Get(r10));
  EXPECT_EQ(result7, 15);
  EXPECT_EQ(result8, 16);
  EXPECT_EQ(result9, 19);
  EXPECT_EQ(result10, 27);

  /// create actor and task function remote call with args passed by reference
  ActorHandle<DummyObject> actor5 = Ray::Actor(DummyObject::FactoryCreate).Remote(r10);

  auto r11 = actor5.Task(&DummyObject::Add).Remote(r0);
  auto r12 = actor5.Task(&DummyObject::Add).Remote(r11);
  auto r13 = actor5.Task(&DummyObject::Add).Remote(r10);
  auto r14 = actor5.Task(&DummyObject::Add).Remote(r13);
  // auto r15 = Ray::Task(Plus).Remote(r0, r11);
  // auto r16 = Ray::Task(Plus1).Remote(r15);

  int result12 = *(Ray::Get(r12));
  int result14 = *(Ray::Get(r14));
  int result11 = *(Ray::Get(r11));
  int result13 = *(Ray::Get(r13));
  // int result16 = *(Ray::Get(r16));
  // int result15 = *(Ray::Get(r15));

  EXPECT_EQ(result11, 28);
  EXPECT_EQ(result12, 56);
  EXPECT_EQ(result13, 83);
  EXPECT_EQ(result14, 166);
  // EXPECT_EQ(result15, 29);
  // EXPECT_EQ(result16, 30);

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