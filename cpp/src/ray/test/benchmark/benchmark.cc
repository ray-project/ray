#include <gtest/gtest.h>
#include <ray/api.h>
#include <chrono>

using namespace ::ray::api;

class Counter {
 public:
  int count;

  Counter(int init) { count = init; }

  static Counter *FactoryCreate() { return new Counter(0); }

  /// non static function
  int Add() {
    count += 1;
    return count;
  }

  int Get() const { return count; }
};

RAY_REMOTE(Counter::FactoryCreate, &Counter::Add, &Counter::Get);

TEST(BenchmarkTest, Benchmark) {
  ActorHandle<Counter> actor = Ray::Actor(Counter::FactoryCreate).Remote();
  size_t num_repeats = 1000000;
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < num_repeats; i++) {
    actor.Task(&Counter::Add).Remote();
  }
  auto end = std::chrono::system_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

  std::string out = "Benchmark finished, repeated ";
  out.append(std::to_string(num_repeats))
      .append(" times, total duration ")
      .append(std::to_string(duration / 1000000))
      .append(" ms,")
      .append(" average duration ")
      .append(std::to_string(duration / num_repeats))
      .append(" ns.");
  RAYLOG(INFO) << out;

  std::cout << out << std::endl;

  Ray::Shutdown();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  /// configuration
  ray::api::RayConfig config;

  /// initialization
  Ray::Init(config, &argc, &argv);
  return RUN_ALL_TESTS();
}