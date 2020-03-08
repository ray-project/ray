
#include <gtest/gtest.h>
#include <ray/api.h>

using namespace ray;

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

TEST(ray_api_test_case, put_test) {
  Ray::init();

  auto obj1 = Ray::put(1);
  auto i1 = obj1.get();
  EXPECT_EQ(1, *i1);
}

TEST(ray_api_test_case, wait_test) {
  Ray::init();
  auto r0 = Ray::call(foo0);
  auto r1 = Ray::call(foo, 3);
  auto r2 = Ray::call(bar, 2, 3);
  std::vector<RayObject<int>> vector;
  vector.push_back(r0);
  vector.push_back(r1);
  vector.push_back(r2);
  auto result = Ray::wait(vector, 3, 1000);
  EXPECT_EQ(result.readys.size(), 3);
  EXPECT_EQ(result.remains.size(), 0);
  auto getResult = Ray::get(vector);
  EXPECT_EQ(getResult.size(), 3);
  EXPECT_EQ(*getResult[0], 1);
  EXPECT_EQ(*getResult[1], 4);
  EXPECT_EQ(*getResult[2], 5);
}

TEST(ray_api_test_case, call_with_value_test) {
  auto r0 = Ray::call(foo0);
  auto r1 = Ray::call(foo, 3);
  auto r2 = Ray::call(bar, 2, 3);

  int result0 = *(r0.get());
  int result1 = *(r1.get());
  int result2 = *(r2.get());

  auto r3 = Ray::call(Foo::foo_s, 3);
  auto r4 = Ray::call(Foo::bar_s, 3, 4);

  int result3 = *(r3.get());
  int result4 = *(r4.get());

  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 4);
  EXPECT_EQ(result2, 5);
  EXPECT_EQ(result3, 4);
  EXPECT_EQ(result4, 7);
}

TEST(ray_api_test_case, call_with_object_test) {
  auto rt0 = Ray::call(foo0);
  auto rt1 = Ray::call(foo, rt0);
  auto rt2 = Ray::call(bar, rt1, 3);
  auto rt3 = Ray::call(Foo::foo_s, 3);
  auto rt4 = Ray::call(Foo::bar_s, rt2, rt3);

  int return0 = *(rt0.get());
  int return1 = *(rt1.get());
  int return2 = *(rt2.get());
  int return3 = *(rt3.get());
  int return4 = *(rt4.get());

  EXPECT_EQ(return0, 1);
  EXPECT_EQ(return1, 2);
  EXPECT_EQ(return2, 5);
  EXPECT_EQ(return3, 4);
  EXPECT_EQ(return4, 9);
}

TEST(ray_api_test_case, actor) {
  RayActor<Foo> actor = Ray::createActor(Foo::create);
  auto rt1 = actor.call(&Foo::foo, 3);
  auto rt2 = actor.call(&Foo::bar, 3, rt1);
  auto rt3 = actor.call(&Foo::add, 1);
  auto rt4 = actor.call(&Foo::add, 2);
  auto rt5 = actor.call(&Foo::add, 3);
  auto rt6 = actor.call(&Foo::add, rt5);

  int return1 = *(rt1.get());
  int return2 = *(rt2.get());
  int return3 = *(rt3.get());
  int return4 = *(rt4.get());
  int return5 = *(rt5.get());
  int return6 = *(rt6.get());

  EXPECT_EQ(return1, 4);
  EXPECT_EQ(return2, 7);
  EXPECT_EQ(return3, 1);
  EXPECT_EQ(return4, 3);
  EXPECT_EQ(return5, 6);
  EXPECT_EQ(return6, 12);
}