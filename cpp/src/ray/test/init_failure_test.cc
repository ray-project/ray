// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/ray_exception.h>

#include <boost/asio.hpp>
#include <string>

#include "../config_internal.h"
#include "../runtime/abstract_ray_runtime.h"
#include "ray/common/ray_config.h"

namespace {

uint16_t ReserveUnusedLocalPort() {
  boost::asio::io_context io_context;
  boost::asio::ip::tcp::acceptor acceptor(
      io_context,
      boost::asio::ip::tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0));
  uint16_t port = acceptor.local_endpoint().port();
  acceptor.close();
  return port;
}

}  // namespace

TEST(RayInitFailureTest, UnreachableGcsThrowsAndLeavesCleanState) {
  ::RayConfig::instance().initialize(R"({
    "raylet_start_wait_time_s": 1,
    "gcs_server_request_timeout_seconds": 1,
    "gcs_rpc_server_reconnect_timeout_s": 1
  })");

  const uint16_t port = ReserveUnusedLocalPort();
  ray::RayConfig unreachable_config;
  unreachable_config.address = "127.0.0.1:" + std::to_string(port);
  unreachable_config.local_mode = false;

  ASSERT_FALSE(ray::IsInitialized());

  try {
    ray::Init(unreachable_config);
    FAIL() << "Expected ray::Init to throw ray::internal::RayException";
  } catch (const ray::internal::RayException &e) {
    EXPECT_FALSE(std::string(e.what()).empty());
  }

  EXPECT_FALSE(ray::IsInitialized());
  EXPECT_EQ(ray::internal::AbstractRayRuntime::GetInstance(), nullptr);
  EXPECT_TRUE(ray::internal::ConfigInternal::Instance().bootstrap_ip.empty());

  // Verify the process can still initialize cleanly after a failed Init.
  ray::RayConfig local_config;
  local_config.local_mode = true;
  ray::Init(local_config);
  EXPECT_TRUE(ray::IsInitialized());
  auto obj = ray::Put(42);
  EXPECT_EQ(*obj.Get(), 42);
  ray::Shutdown();
  EXPECT_FALSE(ray::IsInitialized());

  // Verify a subsequent unreachable Init also throws cleanly.
  EXPECT_THROW(ray::Init(unreachable_config), ray::internal::RayException);
  EXPECT_FALSE(ray::IsInitialized());
  EXPECT_EQ(ray::internal::AbstractRayRuntime::GetInstance(), nullptr);
}
