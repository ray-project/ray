
#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/experimental/default_worker.h>

using namespace ray::api;

/// general function of user code
int Plus1(int x) { return x + 1; }

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

TEST(RayClusterModeTest, FullTest) {
  /// initialization to cluster mode
  ray::api::RayConfig::GetInstance()->run_mode = RunMode::CLUSTER;
  /// TODO(Guyang Song): add the dynamic library name
  ray::api::RayConfig::GetInstance()->lib_name = "";
  Ray::Init();

  /// put and get object
  auto obj = Ray::Put(12345);
  auto get_result = *(Ray::Get(obj));
  EXPECT_EQ(12345, get_result);

  auto task_obj = Ray::Task(Plus1, 5).Remote();
  int task_result = *(Ray::Get(task_obj));
  EXPECT_EQ(6, task_result);

  ActorHandle<Counter> actor = Ray::Actor(Counter::FactoryCreate, 1).Remote();
  auto actor_object = actor.Task(&Counter::Add, 5).Remote();
  int actor_task_result = *(Ray::Get(actor_object));
  EXPECT_EQ(6, actor_task_result);

  Ray::Shutdown();
}

/// TODO(Guyang Song): Separate default worker from this test.
/// Currently, we compile `default_worker` and `cluster_mode_test` in one single binary,
/// to work around a symbol conflicting issue.
/// This is the main function of the binary, and we use the `is_default_worker` arg to
/// tell if this binary is used as `default_worker` or `cluster_mode_test`.
int main(int argc, char **argv) {
  const char *default_worker_magic = "is_default_worker";
  /// `is_default_worker` is the last arg of `argv`
  if (argc > 1 &&
      memcmp(argv[argc - 1], default_worker_magic, strlen(default_worker_magic)) == 0) {
    default_worker_main(argc, argv);
    return 0;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}