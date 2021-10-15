#include <gtest/gtest.h>
#include <ray/api.h>

#include <chrono>
#include <set>

using namespace ::ray;

void NoReturn() {}
int ReturnVal() { return 1; }

void Input(std::string str) {}

std::string Echo(std::string str) { return str; }

RAY_REMOTE(NoReturn, ReturnVal, Input, Echo);

class Counter {
 public:
  int count;

  Counter(int init) { count = init; }

  static Counter *FactoryCreate() { return new Counter(0); }

  int Add() {
    count += 1;
    return count;
  }

  int Get() const { return count; }
};

RAY_REMOTE(Counter::FactoryCreate, &Counter::Add, &Counter::Get);

template <typename F>
void Execute(F f, std::string func_name, size_t num_repeats) {
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < num_repeats; i++) {
    f();
  }
  auto end = std::chrono::system_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

  std::string out = "Benchmark ";
  out.append(func_name)
      .append(" finished, repeated ")
      .append(std::to_string(num_repeats))
      .append(" times, total duration ")
      .append(std::to_string(duration / 1000000))
      .append(" ms,")
      .append(" average duration ")
      .append(std::to_string(duration / num_repeats))
      .append(" ns.");
  RAYLOG(INFO) << out;

  std::cout << out << std::endl;
}

template <typename R, typename F>
void RunLatencyTest(F f, std::string func_name, size_t num_repeats) {
  std::set<uint64_t> set;
  for (size_t i = 0; i < num_repeats; i++) {
    auto start = std::chrono::system_clock::now();
    ObjectRef<R> object_ref = f();
    object_ref.Get();
    auto end = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    set.emplace(duration);
  }

  std::string out = "Latency ";
  out.append(func_name).append(": \n");

  for (auto &time : set) {
    out.append(std::to_string(time)).append("ns\n");
  }
  RAYLOG(INFO) << out;

  std::cout << out << std::endl;
}

TEST(BenchmarkTest, SimpleLatancyTest) {
  ActorHandle<Counter> actor = ray::Actor(Counter::FactoryCreate).Remote();
  size_t num_repeats = 20;
  RunLatencyTest<int>([&actor] { return actor.Task(&Counter::Add).Remote(); },
                      "&Counter::Add", num_repeats);

  RunLatencyTest<void>([] { return ray::Task(NoReturn).Remote(); }, "NoReturn",
                       num_repeats);

  RunLatencyTest<int>([] { return ray::Task(ReturnVal).Remote(); }, "ReturnVal",
                      num_repeats);
}

TEST(BenchmarkTest, SimpleRemoteTest) {
  ActorHandle<Counter> actor = ray::Actor(Counter::FactoryCreate).Remote();
  size_t num_repeats = 100000;
  Execute([&actor] { actor.Task(&Counter::Add).Remote(); }, "&Counter::Add", num_repeats);

  Execute([] { ray::Task(NoReturn).Remote(); }, "NoReturn", num_repeats);

  Execute([] { ray::Task(ReturnVal).Remote(); }, "ReturnVal", num_repeats);
}

TEST(BenchmarkTest, RemoteWithArgsTest) {
  size_t num_repeats = 100000;
  std::string small_str(100, ' ');
  Execute([&small_str] { ray::Task(Input).Remote(small_str); }, "Input small_str",
          num_repeats);
  Execute([&small_str] { ray::Task(Echo).Remote(small_str); }, "Echo small_str",
          num_repeats);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  ray::RayConfig config;
  ray::Init(config, argc, argv);
  int ret = RUN_ALL_TESTS();
  ray::Shutdown();

  return ret;
}