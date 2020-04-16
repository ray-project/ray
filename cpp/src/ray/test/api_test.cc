
#include <gtest/gtest.h>
#include <ray/api.h>
#include <future>
#include <thread>

using namespace ray::api;

int Return1() { return 1; }
int Plus1(int x) { return x + 1; }

int Plus(int x, int y) { return x + y; }

class Counter {
 public:
  int count;

  MSGPACK_DEFINE(count);

  Counter() { count = 0; }

  static Counter *FactoryCreate() {
    Counter *counter = new Counter();
    return counter;
  }

  int Plus1(int x) { return x + 1; }

  int Plus(int x, int y) { return x + y; }

  int Add(int x) {
    count += x;
    return count;
  }
};

TEST(RayApiTest, PutTest) {
  Ray::Init();

  auto obj1 = Ray::Put(1);
  auto i1 = obj1.Get();
  EXPECT_EQ(1, *i1);
}

TEST(RayApiTest, WaitTest) {
  Ray::Init();
  auto r0 = Ray::Call(Return1);
  auto r1 = Ray::Call(Plus1, 3);
  auto r2 = Ray::Call(Plus, 2, 3);
  std::vector<ObjectID> objects = {r0.ID(), r1.ID(), r2.ID()};
  WaitResult result = Ray::Wait(objects, 3, 1000);
  EXPECT_EQ(result.ready.size(), 3);
  EXPECT_EQ(result.unready.size(), 0);
  std::vector<std::shared_ptr<int>> getResult = Ray::Get<int>(objects);
  EXPECT_EQ(getResult.size(), 3);
  EXPECT_EQ(*getResult[0], 1);
  EXPECT_EQ(*getResult[1], 4);
  EXPECT_EQ(*getResult[2], 5);
}

TEST(RayApiTest, CallWithValueTest) {
  auto r0 = Ray::Call(Return1);
  auto r1 = Ray::Call(Plus1, 3);
  auto r2 = Ray::Call(Plus, 2, 3);

  int result0 = *(r0.Get());
  int result1 = *(r1.Get());
  int result2 = *(r2.Get());

  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 4);
  EXPECT_EQ(result2, 5);
}

TEST(RayApiTest, CallWithObjectTest) {
  auto rt0 = Ray::Call(Return1);
  auto rt1 = Ray::Call(Plus1, rt0);
  auto rt2 = Ray::Call(Plus, rt1, 3);
  auto rt3 = Ray::Call(Plus1, 3);
  auto rt4 = Ray::Call(Plus, rt2, rt3);

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

TEST(RayApiTest, ActorTest) {
  Ray::Init();
  RayActor<Counter> actor = Ray::CreateActor(Counter::FactoryCreate);
  auto rt1 = actor.Call(&Counter::Add, 1);
  auto rt2 = actor.Call(&Counter::Add, 2);
  auto rt3 = actor.Call(&Counter::Add, 3);
  auto rt4 = actor.Call(&Counter::Add, rt3);

  int return1 = *(rt1.Get());
  int return2 = *(rt2.Get());
  int return3 = *(rt3.Get());
  int return4 = *(rt4.Get());

  EXPECT_EQ(return1, 1);
  EXPECT_EQ(return2, 3);
  EXPECT_EQ(return3, 6);
  EXPECT_EQ(return4, 12);
}

TEST(RayApiTest, CompareWithFuture) {
  // future from a packaged_task
  std::packaged_task<int(int)> task(Plus1);
  std::future<int> f1 = task.get_future();
  std::thread t(std::move(task), 1);
  int rt1 = f1.get();

  // future from an async()
  std::future<int> f2 = std::async(std::launch::async, Plus1, 1);
  int rt2 = f2.get();

  // Ray API
  Ray::Init();
  auto f3 = Ray::Call(Plus1, 1);
  int rt3 = *f3.Get();

  EXPECT_EQ(rt1, 2);
  EXPECT_EQ(rt2, 2);
  EXPECT_EQ(rt3, 2);
  t.join();
}
