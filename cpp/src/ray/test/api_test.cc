
#include <gtest/gtest.h>
#include <ray/api.h>

#include <future>
#include <thread>

using namespace ray::api;

int Return1() { return 1; }
int Plus1(int x) { return x + 1; }

int Plus(int x, int y) { return x + y; }

int Triple(int x, int y, int z) { return x + y + z; }

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

  int Triple(int x, int y, int z) { return x + y + z; }

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

TEST(RayApiTest, StaticGetTest) {
  Ray::Init();
  /// `Get` member function
  auto obj_ref1 = Ray::Put(100);
  auto res1 = obj_ref1.Get();
  EXPECT_EQ(100, *res1);

  /// `Get` static function
  auto obj_ref2 = Ray::Put(200);
  auto res2 = Ray::Get(obj_ref2);
  EXPECT_EQ(200, *res2);
}

TEST(RayApiTest, WaitTest) {
  Ray::Init();
  auto r0 = Ray::Task(Return1).Remote();
  auto r1 = Ray::Task(Plus1, 3).Remote();
  auto r2 = Ray::Task(Plus, 2, 3).Remote();
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
  auto r0 = Ray::Task(Return1).Remote();
  auto r1 = Ray::Task(Plus1, 3).Remote();
  auto r2 = Ray::Task(Plus, 2, 3).Remote();
  auto r3 = Ray::Task(Triple, 1, 2, 3).Remote();

  int result0 = *(r0.Get());
  int result1 = *(r1.Get());
  int result2 = *(r2.Get());
  int result3 = *(r3.Get());

  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 4);
  EXPECT_EQ(result2, 5);
  EXPECT_EQ(result3, 6);
}

TEST(RayApiTest, CallWithObjectTest) {
  auto rt0 = Ray::Task(Return1).Remote();
  auto rt1 = Ray::Task(Plus1, rt0).Remote();
  auto rt2 = Ray::Task(Plus, rt1, 3).Remote();
  auto rt3 = Ray::Task(Plus1, 3).Remote();
  auto rt4 = Ray::Task(Plus, rt2, rt3).Remote();

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
  ActorHandle<Counter> actor = Ray::Actor(Counter::FactoryCreate).Remote();
  auto rt1 = actor.Task(&Counter::Add, 1).Remote();
  auto rt2 = actor.Task(&Counter::Add, 2).Remote();
  auto rt3 = actor.Task(&Counter::Add, 3).Remote();
  auto rt4 = actor.Task(&Counter::Add, rt3).Remote();
  auto rt5 = actor.Task(&Counter::Triple, 1, 2, 3).Remote();

  int return1 = *(rt1.Get());
  int return2 = *(rt2.Get());
  int return3 = *(rt3.Get());
  int return4 = *(rt4.Get());
  int return5 = *(rt5.Get());

  EXPECT_EQ(return1, 1);
  EXPECT_EQ(return2, 3);
  EXPECT_EQ(return3, 6);
  EXPECT_EQ(return4, 12);
  EXPECT_EQ(return5, 6);
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
  auto f3 = Ray::Task(Plus1, 1).Remote();
  int rt3 = *f3.Get();

  EXPECT_EQ(rt1, 2);
  EXPECT_EQ(rt2, 2);
  EXPECT_EQ(rt3, 2);
  t.join();
}

RAY_REGISTER(Plus1);

TEST(RayApiTest, RayRegister) {
  using namespace ray::api::internal;

  bool r = Router::Instance().RegisterRemoteFunction("Return1", Return1);
  EXPECT_TRUE(r);

  /// Duplicate register
  bool r1 = Router::Instance().RegisterRemoteFunction("Return1", Return1);
  EXPECT_TRUE(!r1);

  bool r2 = Router::Instance().RegisterRemoteFunction("Plus1", Plus1);
  EXPECT_TRUE(!r2);

  /// Find and call the registered function.
  auto args = std::make_tuple("Plus1", 1);
  auto buf = Serializer::Serialize(args);
  auto result_buf = Router::Instance().Route(buf.data(), buf.size());

  /// Deserialize result.
  auto response =
      Serializer::Deserialize<Response<int>>(result_buf.data(), result_buf.size());

  EXPECT_EQ(response.error_code, ErrorCode::OK);
  EXPECT_EQ(response.data, 2);

  /// Void function.
  auto buf1 = Serializer::Serialize(std::make_tuple("Return1"));
  auto result_buf1 = Router::Instance().Route(buf1.data(), buf1.size());
  auto response1 =
      Serializer::Deserialize<VoidResponse>(result_buf.data(), result_buf.size());
  EXPECT_EQ(response1.error_code, ErrorCode::OK);

  /// We should consider the driver so is not same with the worker so, and find the error
  /// reason.

  /// Not exist function.
  auto buf2 = Serializer::Serialize(std::make_tuple("Return11"));
  auto result_buf2 = Router::Instance().Route(buf2.data(), buf2.size());
  auto response2 =
      Serializer::Deserialize<VoidResponse>(result_buf2.data(), result_buf2.size());
  EXPECT_EQ(response2.error_code, ErrorCode::FAIL);
  EXPECT_FALSE(response2.error_msg.empty());

  /// Arguments not match.
  auto buf3 = Serializer::Serialize(std::make_tuple("Plus1", "invalid arguments"));
  auto result_buf3 = Router::Instance().Route(buf3.data(), buf3.size());
  auto response3 =
      Serializer::Deserialize<Response<int>>(result_buf3.data(), result_buf3.size());
  EXPECT_EQ(response3.error_code, ErrorCode::FAIL);
  EXPECT_FALSE(response3.error_msg.empty());
}
