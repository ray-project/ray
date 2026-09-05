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

#include <cstring>

#include "gcs_client_bridge.h"
#include "gcs_client_internal.h"
#include "gcs_memory.h"

// Tests the retained log-batch publish path (ray_gcs_client_publisher_publish_log_batch)
// error handling. Real publishing requires a live GCS cluster, so only the reachable
// error boundaries (null client, null backing gcs_client) are exercised here.

TEST(GcsClientPublisherTest, PublishLogBatchNullClient) {
  char *error = nullptr;

  int result = ray_gcs_client_publisher_publish_log_batch(nullptr,      // client
                                                          "key_id",     // key_id
                                                          "127.0.0.1",  // ip
                                                          "1",          // pid
                                                          "job",        // job_id
                                                          0,            // is_error
                                                          nullptr,      // lines
                                                          0,            // line_count
                                                          "actor",      // actor_name
                                                          "task",       // task_name
                                                          -1,           // timeout_ms
                                                          &error);

  ASSERT_EQ(result, 0);
  ASSERT_NE(error, nullptr);
  ASSERT_NE(std::strstr(error, "client is null"), nullptr)
      << "expected null-client error, got: " << error;

  ray_gcs_free_memory(error);
}

TEST(GcsClientPublisherTest, PublishLogBatchNullBackingClient) {
  // Construct a CGcsClient whose backing gcs_client is null; this triggers
  // the same invalid-argument guard as a null client pointer.
  CGcsClient client;
  client.gcs_client = nullptr;

  char *error = nullptr;

  const char *lines[] = {"line1", "line2"};
  int result = ray_gcs_client_publisher_publish_log_batch(
      &client,      // client (valid pointer, null backing client)
      "key_id",     // key_id
      "127.0.0.1",  // ip
      "1",          // pid
      "job",        // job_id
      1,            // is_error
      lines,        // lines
      2,            // line_count
      "actor",      // actor_name
      "task",       // task_name
      1000,         // timeout_ms
      &error);

  ASSERT_EQ(result, 0);
  ASSERT_NE(error, nullptr);
  ASSERT_NE(std::strstr(error, "client is null"), nullptr)
      << "expected null-backing-client error, got: " << error;

  ray_gcs_free_memory(error);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
