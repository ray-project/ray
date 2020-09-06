
#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/ray_config.h>

using namespace ray::api;

TEST(RayClusterModeTest, PutTest) {
  ray::api::RayConfig::GetInstance()->run_mode = RunMode::CLUSTER;
  Ray::Init();
  auto obj1 = Ray::Put(12345);
  auto i1 = obj1.Get();
  EXPECT_EQ(12345, *i1);
  Ray::Shutdown();
}