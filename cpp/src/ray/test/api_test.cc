
#include <gtest/gtest.h>
#include <ray/api.h>

using namespace ray;

int foo0() {
  // std::cout << "foo0 returns 1" << std::endl;
  return 1;
}
int foo(int x) {
  // std::cout << "foo returns " << (x + 1) << std::endl;
  return x + 1;
}

int bar(int x, int y) {
  // std::cout << "bar returns " << (x + y) << std::endl;
  return x + y;
}

class Foo {
 public:
  int count;

  Foo() { count = 0; }

  static Foo *create() {
    Foo *foo = new Foo();
    return foo;
  }

  int foo(int x) {
    // std::cout << "foo returns " << (x + 1) << std::endl;
    return x + 1;
  }

  int bar(int x, int y) {
    // std::cout << "bar returns " << (x + y) << std::endl;
    return x + y;
  }

  int add(int x) {
    count += x;
    // std::cout << "add returns " << count << std::endl;
    return count;
  }

  static int foo_s(int x) {
    // std::cout << "foo returns " << (x + 1) << std::endl;
    return x + 1;
  }

  static int bar_s(int x, int y) {
    // std::cout << "bar returns " << (x + y) << std::endl;
    return x + y;
  }
};

namespace ray {

inline void marshall(::ray::binary_writer &writer, const Foo &foo) {
  writer.write((char *)&foo.count, sizeof(int));
}

inline void unmarshall(::ray::binary_reader &reader, Foo &foo) {
  reader.read((char *)&foo.count, sizeof(int));
}

inline void marshall(::ray::binary_writer &writer, Foo *const &foo) {
  writer.write((char *)&foo, sizeof(Foo *));
}

inline void unmarshall(::ray::binary_reader &reader, Foo *&foo) {
  reader.read((char *)&foo, sizeof(Foo *));
}
}  // namespace ray

TEST(ray_api_test_case, put_test) {
  Ray::init();

  auto obj1 = Ray::put(1);
  auto i1 = obj1->get();
  EXPECT_EQ(1, *i1);
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
  RayActor<Foo> fobj = Ray::create(Foo::create);
  auto rt1 = Ray::call(&Foo::foo, fobj, 3);
  auto rt2 = Ray::call(&Foo::bar, fobj, 3, rt1);
  auto rt3 = Ray::call(&Foo::add, fobj, 1);
  auto rt4 = Ray::call(&Foo::add, fobj, 2);
  auto rt5 = Ray::call(&Foo::add, fobj, 3);
  auto rt6 = Ray::call(&Foo::add, fobj, rt5);

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