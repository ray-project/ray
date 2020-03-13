
#include <gtest/gtest.h>
#include <ray/api.h>
#include <future>
#include <thread>

using namespace ray::api;

int foo0() { return 1; }
int foo(int x) { return x + 1; }

int bar(int x, int y) { return x + y; }

class Foo {
 public:
  int count;

  MSGPACK_DEFINE(count);

  Foo() { count = 0; }

  static Foo *create() {
    Foo *foo = new Foo();
    return foo;
  }

  int foo(int x) { return x + 1; }

  int bar(int x, int y) { return x + y; }

  int add(int x) {
    count += x;
    return count;
  }

  static int foo_s(int x) { return x + 1; }

  static int bar_s(int x, int y) { return x + y; }
};

TEST(ray_api_test_case, Put_test) {
  Ray::Init();

  auto obj1 = Ray::Put(1);
  auto i1 = obj1.Get();
  EXPECT_EQ(1, *i1);
}

TEST(ray_api_test_case, wait_test) {
  Ray::Init();
  RayObject<int> r0 = Ray::Call(foo0);
  RayObject<int> r1 = Ray::Call(foo, 3);
  RayObject<int> r2 = Ray::Call(bar, 2, 3);
  std::vector<ObjectID> vector;
  vector.push_back(r0.ID());
  vector.push_back(r1.ID());
  vector.push_back(r2.ID());
  WaitResult result = Ray::Wait(vector, 3, 1000);
  EXPECT_EQ(result.readys.size(), 3);
  EXPECT_EQ(result.remains.size(), 0);
  std::vector<std::shared_ptr<int>> getResult = Ray::Get<int>(vector);
  EXPECT_EQ(getResult.size(), 3);
  EXPECT_EQ(*getResult[0], 1);
  EXPECT_EQ(*getResult[1], 4);
  EXPECT_EQ(*getResult[2], 5);
}

TEST(ray_api_test_case, Call_with_value_test) {
  auto r0 = Ray::Call(foo0);
  auto r1 = Ray::Call(foo, 3);
  auto r2 = Ray::Call(bar, 2, 3);

  int result0 = *(r0.Get());
  int result1 = *(r1.Get());
  int result2 = *(r2.Get());

  auto r3 = Ray::Call(Foo::foo_s, 3);
  auto r4 = Ray::Call(Foo::bar_s, 3, 4);

  int result3 = *(r3.Get());
  int result4 = *(r4.Get());

  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 4);
  EXPECT_EQ(result2, 5);
  EXPECT_EQ(result3, 4);
  EXPECT_EQ(result4, 7);
}

TEST(ray_api_test_case, Call_with_object_test) {
  auto rt0 = Ray::Call(foo0);
  auto rt1 = Ray::Call(foo, rt0);
  auto rt2 = Ray::Call(bar, rt1, 3);
  auto rt3 = Ray::Call(Foo::foo_s, 3);
  auto rt4 = Ray::Call(Foo::bar_s, rt2, rt3);

  int return0 = *(rt0.Get());
  int return1 = *(rt1.Get());
  int return2 = *(rt2.Get());
  int return3 = *(rt3.Get());
  int return4 = *(rt4.Get());

  EXPECT_EQ(return0, 1);
  EXPECT_EQ(return1, 2);
  EXPECT_EQ(return2, 5);
  EXPECT_EQ(return3, 4);
  EXPECT_EQ(return4, 9);
}

TEST(ray_api_test_case, actor) {
  Ray::Init();
  RayActor<Foo> actor = Ray::CreateActor(Foo::create);
  auto rt1 = actor.Call(&Foo::foo, 3);
  auto rt2 = actor.Call(&Foo::bar, 3, rt1);
  auto rt3 = actor.Call(&Foo::add, 1);
  auto rt4 = actor.Call(&Foo::add, 2);
  auto rt5 = actor.Call(&Foo::add, 3);
  auto rt6 = actor.Call(&Foo::add, rt5);

  int return1 = *(rt1.Get());
  int return2 = *(rt2.Get());
  int return3 = *(rt3.Get());
  int return4 = *(rt4.Get());
  int return5 = *(rt5.Get());
  int return6 = *(rt6.Get());

  EXPECT_EQ(return1, 4);
  EXPECT_EQ(return2, 7);
  EXPECT_EQ(return3, 1);
  EXPECT_EQ(return4, 3);
  EXPECT_EQ(return5, 6);
  EXPECT_EQ(return6, 12);
}

TEST(ray_api_test_case, compare_with_future) {

  // future from a packaged_task
  std::packaged_task<int(int)> task(foo); 
  std::future<int> f1 = task.get_future();
  std::thread t(std::move(task), 1);
  int rt1 = f1.get();

  // future from an async()
  std::future<int> f2 = std::async(std::launch::async, foo, 1);
  int rt2 = f2.get();

  // Ray API
  Ray::Init();
  RayObject<int> f3 = Ray::Call(foo, 1);
  int rt3 = *f3.Get();

  EXPECT_EQ(rt1, 2);
  EXPECT_EQ(rt2, 2);
  EXPECT_EQ(rt3, 2);
  t.join();
}