
/// This is a complete example of writing a distributed program using the C ++ worker API.

/// including the header
#include <ray/api.h>
#include <ray/api/ray_config.h>
#include "gflags/gflags.h"

/// using namespace
using namespace ::ray::api;

/// general function of user code
int Return1() { return 1; }
int Plus1(int x) { return x + 1; }
int Plus(int x, int y) { return x + y; }

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

DEFINE_string(redis_address, "", "The ip address of redis server.");

DEFINE_string(dynamic_library_path, "", "The local path of the dynamic library.");

int main(int argc, char **argv) {
  /// configuration
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string dynamic_library_path = FLAGS_dynamic_library_path;
  const std::string redis_address = FLAGS_redis_address;
  gflags::ShutDownCommandLineFlags();
  RAY_CHECK(!dynamic_library_path.empty())
      << "Please add a local dynamic library by '--dynamic-library-path'";
  ray::api::RayConfig::GetInstance()->lib_name = dynamic_library_path;
  if (!redis_address.empty()) {
    ray::api::RayConfig::GetInstance()->SetRedisAddress(redis_address);
  }
  ::ray::api::RayConfig::GetInstance()->run_mode = RunMode::CLUSTER;

  /// initialization
  Ray::Init();

  /// put and get object
  auto obj = Ray::Put(12345);
  auto get_put_result = *(Ray::Get(obj));
  std::cout << "get_put_result = " << get_put_result << std::endl;

  /// common task without args
  auto task_obj = Ray::Task(Return1).Remote();
  int task_result1 = *(Ray::Get(task_obj));
  std::cout << "task_result1 = " << task_result1 << std::endl;

  /// common task with args
  task_obj = Ray::Task(Plus1, 5).Remote();
  int task_result2 = *(Ray::Get(task_obj));
  std::cout << "task_result2 = " << task_result2 << std::endl;

  /// actor task without args
  ActorHandle<Counter> actor1 = Ray::Actor(Counter::FactoryCreate).Remote();
  auto actor_object1 = actor1.Task(&Counter::Plus1).Remote();
  int actor_result1 = *(Ray::Get(actor_object1));
  std::cout << "actor_result1 = " << actor_result1 << std::endl;

  /// actor task with args
  ActorHandle<Counter> actor2 = Ray::Actor(Counter::FactoryCreate, 1).Remote();
  auto actor_object2 = actor2.Task(&Counter::Add, 5).Remote();
  int actor_result2 = *(Ray::Get(actor_object2));
  std::cout << "actor_result2 = " << actor_result2 << std::endl;

  /// actor task with args which pass by reference
  ActorHandle<Counter> actor3 = Ray::Actor(Counter::FactoryCreate, 6, 0).Remote();
  auto actor_object3 = actor3.Task(&Counter::Add, actor_object2).Remote();
  int actor_result3 = *(Ray::Get(actor_object3));
  std::cout << "actor_result3 = " << actor_result3 << std::endl;

  /// general function remote call（args passed by value）
  auto r0 = Ray::Task(Return1).Remote();
  auto r2 = Ray::Task(Plus, 3, 22).Remote();
  int task_result3 = *(Ray::Get(r2));
  std::cout << "task_result3 = " << task_result3 << std::endl;

  /// general function remote call（args passed by reference）
  auto r3 = Ray::Task(Return1).Remote();
  auto r4 = Ray::Task(Plus1, r3).Remote();
  auto r5 = Ray::Task(Plus, r4, r3).Remote();
  auto r6 = Ray::Task(Plus, r4, 10).Remote();
  int task_result4 = *(Ray::Get(r6));
  int task_result5 = *(Ray::Get(r5));
  std::cout << "task_result4 = " << task_result4 << ", task_result5 = " << task_result5
            << std::endl;

  /// create actor and actor function remote call with args passed by value
  ActorHandle<Counter> actor4 = Ray::Actor(Counter::FactoryCreate, 10).Remote();
  auto r10 = actor4.Task(&Counter::Add, 8).Remote();
  int actor_result4 = *(Ray::Get(r10));
  std::cout << "actor_result4 = " << actor_result4 << std::endl;

  /// create actor and task function remote call with args passed by reference
  ActorHandle<Counter> actor5 = Ray::Actor(Counter::FactoryCreate, r10, 0).Remote();
  auto r11 = actor5.Task(&Counter::Add, r0).Remote();
  auto r12 = actor5.Task(&Counter::Add, r11).Remote();
  auto r13 = actor5.Task(&Counter::Add, r10).Remote();
  auto r14 = actor5.Task(&Counter::Add, r13).Remote();
  auto r15 = Ray::Task(Plus, r0, r11).Remote();
  auto r16 = Ray::Task(Plus1, r15).Remote();
  int result12 = *(Ray::Get(r12));
  int result14 = *(Ray::Get(r14));
  int result11 = *(Ray::Get(r11));
  int result13 = *(Ray::Get(r13));
  int result16 = *(Ray::Get(r16));
  int result15 = *(Ray::Get(r15));
  std::cout << "Final result:" << std::endl;
  std::cout << "result11 = " << result11 << ", result12 = " << result12
            << ", result13 = " << result13 << ", result14 = " << result14
            << ", result15 = " << result15 << ", result16 = " << result16 << std::endl;

  /// shutdown
  Ray::Shutdown();

  return 0;
}
