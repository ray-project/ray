
/// This is a complete example of writing a distributed program using the C ++ worker API.

/// including the header
#include <ray/api.h>

/// using namespace
using namespace ray::api;

/// general function of user code
int Return1() { return 1; }
int Plus1(int x) { return x + 1; }
int Plus(int x, int y) { return x + y; }

/// a class of user code
/// Note, The only difference from ordinary classes is that you need to declare the macro
/// definition of MessagePack to make this class serializable.
class Counter {
 public:
  int count;
  /// Make `Counter` serializable
  MSGPACK_DEFINE(count);

  Counter() { count = 0; }

  static Counter *FactoryCreate() {
    Counter *counter = new Counter();
    return counter;
  }
  /// non static function
  int Add(int x) {
    count += x;
    return count;
  }
  /// static function
  static int Plus1S(int x) { return x + 1; }
  static int PlusS(int x, int y) { return x + y; }
};

int main() {
  /// initialization
  Ray::Init();

  /// put and get object
  auto obj = Ray::Put(123);
  auto getRsult = obj.Get();

  /// general function remote call（args passed by value）
  auto r0 = Ray::Call(Return1);
  auto r1 = Ray::Call(Plus1, 1);
  auto r2 = Ray::Call(Plus, 1, 2);
  auto r3 = Ray::Call(Counter::Plus1S, 3);
  auto r4 = Ray::Call(Counter::PlusS, 3, 2);

  int result0 = *(r0.Get());
  int result1 = *(r1.Get());
  int result2 = *(r2.Get());
  int result3 = *(r3.Get());
  int result4 = *(r4.Get());

  std::cout << "Ray::call with value results: " << result0 << " " << result1 << " "
            << result2 << " " << result3 << " " << result4 << " " << std::endl;

  /// general function remote call（args passed by reference）
  auto rt0 = Ray::Call(Return1);
  auto rt1 = Ray::Call(Plus1, rt0);
  auto rt2 = Ray::Call(Plus, rt1, 1);
  auto rt3 = Ray::Call(Counter::Plus1S, 3);
  auto rt4 = Ray::Call(Counter::PlusS, rt0, rt3);

  int return0 = *(rt0.Get());
  int return1 = *(rt1.Get());
  int return2 = *(rt2.Get());
  int return3 = *(rt3.Get());
  int return4 = *(rt4.Get());

  std::cout << "Ray::call with reference results: " << return0 << " " << return1 << " "
            << return2 << " " << return3 << " " << return4 << " " << std::endl;

  /// create actor and actor function remote call
  RayActor<Counter> actor = Ray::CreateActor(Counter::FactoryCreate);
  auto rt5 = actor.Call(&Counter::Add, 5);
  auto rt6 = actor.Call(&Counter::Add, 1);
  auto rt7 = actor.Call(&Counter::Add, 1);
  auto rt8 = actor.Call(&Counter::Add, rt7);

  int return5 = *(rt5.Get());
  int return6 = *(rt6.Get());
  int return7 = *(rt7.Get());
  int return8 = *(rt8.Get());

  std::cout << "Ray::call with actor results: " << return5 << " " << return6 << " "
            << return7 << " " << return8 << std::endl;
}
