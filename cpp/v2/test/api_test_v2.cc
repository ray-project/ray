#include <gtest/gtest.h>
#include <iostream>
#include <api_v2.h>

using namespace ray;

void hello(){
  std::cout<<"hello\n";
}

void dummy(){
  std::cout<<"dummy\n";
}

int bar(int a){
  std::cout<<"bar"<<", arg: "<<a<<"\n";
  return a;
}

int add(int a, int b) {
  return a + b;
}

void not_registered_func(){
}

int overload_func(){
  std::cout<<"overload_func no arguments\n";
  return 0;
}
int overload_func(int i){
  std::cout<<"overload_func one argument\n";
  return i;
}
int overload_func(int i, int j){
  std::cout<<"overload_func two arguments\n";
  return i + j;
}

struct Base{
  int bar(int i){
    std::cout<<"bar\n";
    return i;
  }
  int foo(int i){
    std::cout<<"foo\n";
    return i;
  }

  int overload_func(int i){
    std::cout<<"Base::overload_func one argument\n";
    return i;
  }
  int overload_func(int i, int j){
    std::cout<<"Base::overload_func two arguments\n";
    return i + j;
  }
  int overload_func(int i, int j, int k){
    std::cout<<"Base::overload_func two arguments\n";
    return i + j + k;
  }
};

RAY_REGISTER(hello, bar, add, &Base::foo, &Base::bar,
             RayFunc(overload_func),
             RayFunc(overload_func, int),
             RayMemberFunc(&Base::overload_func, cv_none, int),
             RayMemberFunc(&Base::overload_func, cv_none, int, int));

TEST(RayApiTestV2, RayRegister) {
  EXPECT_TRUE(register_func("dummy", dummy));

  // We have already registered hello and bar functions before, duplicate register fucntions will throw exception.
  EXPECT_THROW(register_func("hello", hello), std::logic_error);
  EXPECT_THROW(register_func("bar1", bar), std::logic_error);
}

TEST(RayApiTestV2, GetFunction) {
  EXPECT_EQ(get_function(hello), &hello);
  EXPECT_EQ(get_function(bar), &bar);

  EXPECT_EQ(get_function(nullptr), nullptr);
  EXPECT_EQ(get_function(not_registered_func), nullptr);

  EXPECT_EQ(get_function(RayFunc(overload_func)), RayFunc(overload_func));
  EXPECT_EQ(get_function(RayFunc(overload_func, int)),
            RayFunc(overload_func, int));

  EXPECT_EQ(get_function(&Base::foo), &Base::foo);
  EXPECT_EQ(get_function(&Base::bar), &Base::bar);

  auto expected_func = RayMemberFunc(&Base::overload_func, cv_none, int);
  EXPECT_EQ(get_function(RayMemberFunc(&Base::overload_func, cv_none, int)),
            expected_func);

  auto expected_func1 = RayMemberFunc(&Base::overload_func, cv_none, int, int);
  EXPECT_EQ(
      get_function(RayMemberFunc(&Base::overload_func, cv_none, int, int)),
      expected_func1);
}

TEST(RayApiTestV2, CallFunction) {
  auto f = get_function(hello);
  call_func(f);

  auto f1 = get_function(bar);
  EXPECT_EQ(call_func(f1, 1), 1);

  auto f2 = get_function(RayFunc(overload_func, int));
  EXPECT_EQ(call_func(f2, 1), 1);

  auto f3 = get_function(RayFunc(overload_func));
  EXPECT_EQ(call_func(f3), 0);

  auto f4 = get_function(RayFunc(overload_func, int, int));
  // The function was not registered, call_func with nullptr will throw std::invalid_argument.
  EXPECT_THROW(call_func(f4, 1, 2), std::invalid_argument);

  Base base{};
  auto f5 = get_function(&Base::bar);
  EXPECT_EQ(call_func(base, f5, 1), 1);

  auto f6 = get_function(RayMemberFunc(&Base::overload_func, cv_none, int));
  EXPECT_EQ(call_func(base, f6, 1), 1);

  auto f7 = get_function(RayMemberFunc(&Base::overload_func, cv_none, int, int, int));
  EXPECT_THROW(call_func(base, f7, 1, 2, 3), std::invalid_argument);
}

TEST(RayApiTestV2, Init) {
  // Default arguments.
  EXPECT_TRUE(ray::Init(num_cpus = 8));
  EXPECT_TRUE(ray::Init(num_cpus = 8, object_store_memory = 400));

  // No order arguments.
  EXPECT_TRUE(ray::Init(object_store_memory = 400, num_gpus = 8));
  EXPECT_TRUE(ray::Init(num_cpus = 8, ray_address = "auto"));
}

TEST(RayApiTestV2, NormalTask) {
  auto obj = ray::Task(hello).Remote();
  obj.Get();

  auto obj1 = ray::Task(bar).Remote(1);
  EXPECT_EQ(obj1.Get(), 0);

  EXPECT_THROW(ray::Task(not_registered_func).Remote(), std::invalid_argument);

  auto obj2 = ray::Task(RayFunc(overload_func, int)).Remote(1);
  EXPECT_EQ(obj2.Get(), 0);

  EXPECT_EQ(ray::Task(add).Remote(2, 3).Get(), 0);
  EXPECT_EQ(ray::Task(add).Remote(ray::Put(1), 3).Get(), 0);
  EXPECT_EQ(ray::Task(add).Remote(2, ray::Put(1)).Get(), 0);
  EXPECT_EQ(ray::Task(add).Remote(ray::Put(1), ray::Put(2)).Get(), 0);
}
