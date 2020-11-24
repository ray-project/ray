
/// This is a complete example of writing a distributed program using the C ++ worker API.

/// including the header
#include <ray/api.h>

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

  Counter() { count = 0; }

  static Counter *FactoryCreate() { return new Counter(); }
  /// non static function
  int Add(int x) {
    count += x;
    return count;
  }
};

int main() {
  /// initialization
  Ray::Init();

  /// put and get object
  auto obj = Ray::Put(123);
  auto get_result = obj.Get();

  /// general function remote call（args passed by value）
  auto r0 = Ray::Task(Return1).Remote();
  auto r1 = Ray::Task(Plus1, 1).Remote();
  auto r2 = Ray::Task(Plus, 1, 2).Remote();

  int result0 = *(r0.Get());
  int result1 = *(r1.Get());
  int result2 = *(r2.Get());

  std::cout << "Ray::call with value results: " << result0 << " " << result1 << " "
            << result2 << std::endl;

  /// general function remote call（args passed by reference）
  auto r3 = Ray::Task(Return1).Remote();
  auto r4 = Ray::Task(Plus1, r3).Remote();
  auto r5 = Ray::Task(Plus, r4, 1).Remote();

  int result3 = *(r3.Get());
  int result4 = *(r4.Get());
  int result5 = *(r5.Get());

  std::cout << "Ray::call with reference results: " << result3 << " " << result4 << " "
            << result5 << std::endl;

  /// create actor and actor function remote call
  ActorHandle<Counter> actor = Ray::Actor(Counter::FactoryCreate).Remote();
  auto r6 = actor.Task(&Counter::Add, 5).Remote();
  auto r7 = actor.Task(&Counter::Add, 1).Remote();
  auto r8 = actor.Task(&Counter::Add, 1).Remote();
  auto r9 = actor.Task(&Counter::Add, r8).Remote();

  int result6 = *(r6.Get());
  int result7 = *(r7.Get());
  int result8 = *(r8.Get());
  int result9 = *(r9.Get());

  std::cout << "Ray::call with actor results: " << result6 << " " << result7 << " "
            << result8 << " " << result9 << std::endl;
}
