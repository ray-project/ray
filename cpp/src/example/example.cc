#include "example.h"
#include <ray/api.h>
#include <iostream>
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <chrono>
#include <thread>

using namespace ray;

void handler(int sig) {
  void *array[10];
  size_t size;
  size = backtrace(array, 10);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

int foo0() { return 1; }
int foo(int x) { return x + 1; }

int bar(int x, int y) { return x + y; }

int main() {
  signal(SIGSEGV, handler);
  signal(SIGTERM, handler);
  signal(SIGINT, handler);
  signal(SIGILL, handler);
  signal(SIGABRT, handler);
  signal(SIGFPE, handler);
  Ray::init();

  auto obj = Ray::put(123);
  auto getRsult = obj.get();

  auto r0 = Ray::call(foo0);
  auto r1 = Ray::call(foo, 1);
  auto r2 = Ray::call(bar, 1, 2);
  auto r3 = Ray::call(Foo::foo_s, 3);
  auto r4 = Ray::call(Foo::bar_s, 3, 2);

  int result0 = *(r0.get());
  int result1 = *(r1.get());
  int result2 = *(r2.get());
  int result3 = *(r3.get());
  int result4 = *(r4.get());

  std::cout << "Ray::call with value results: " << result0 << " " << result1 << " "
            << result2 << " " << result3 << " " << result4 << " " << std::endl;

  auto rt0 = Ray::call(foo0);
  auto rt1 = Ray::call(foo, rt0);
  auto rt2 = Ray::call(bar, rt1, 1);
  auto rt3 = Ray::call(Foo::foo_s, 3);
  auto rt4 = Ray::call(Foo::bar_s, rt0, rt3);

  int return0 = *(rt0.get());
  int return1 = *(rt1.get());
  int return2 = *(rt2.get());
  int return3 = *(rt3.get());
  int return4 = *(rt4.get());

  std::cout << "Ray::call with reference results: " << return0 << " " << return1 << " "
            << return2 << " " << return3 << " " << return4 << " " << std::endl;

  RayActor<Foo> fobj = Ray::create(Foo::create);
  auto rt5 = fobj.call(&Foo::foo, 1);
  auto rt6 = fobj.call(&Foo::bar, 1, rt5);
  auto rt7 = fobj.call(&Foo::add, 4);
  auto rt8 = fobj.call(&Foo::add, 1);
  auto rt9 = fobj.call(&Foo::add, 1);
  auto rt10 = fobj.call(&Foo::add, rt9);

  int return5 = *(rt5.get());
  int return6 = *(rt6.get());
  int return7 = *(rt7.get());
  int return8 = *(rt8.get());
  int return9 = *(rt9.get());
  int return10 = *(rt10.get());

  std::cout << "Ray::call with actor results: " << return5 << " " << return6 << " "
            << return7 << " " << return8 << " " << return9 << " " << return10 << " "
            << std::endl;
}
