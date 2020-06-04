// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/external_store.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/test_util.h"

namespace plasma {

using arrow::internal::TemporaryDir;

std::string external_test_executable;  // NOLINT

void AssertObjectBufferEqual(const ObjectBuffer& object_buffer,
                             const std::string& metadata, const std::string& data) {
  arrow::AssertBufferEqual(*object_buffer.metadata, metadata);
  arrow::AssertBufferEqual(*object_buffer.data, data);
}

class TestPlasmaStoreWithExternal : public ::testing::Test {
 public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("ext-test-"));
    store_socket_name_ = temp_dir_->path().ToString() + "store";

    std::string plasma_directory =
        external_test_executable.substr(0, external_test_executable.find_last_of('/'));
    std::string plasma_command = plasma_directory +
                                 "/plasma-store-server -m 1024000 -e " +
                                 "hashtable://test -s " + store_socket_name_ +
                                 " 1> /tmp/log.stdout 2> /tmp/log.stderr & " +
                                 "echo $! > " + store_socket_name_ + ".pid";
    PLASMA_CHECK_SYSTEM(system(plasma_command.c_str()));
    ARROW_CHECK_OK(client_.Connect(store_socket_name_, ""));
  }

  void TearDown() override {
    ARROW_CHECK_OK(client_.Disconnect());
    // Kill plasma_store process that we started
#ifdef COVERAGE_BUILD
    // Ask plasma_store to exit gracefully and give it time to write out
    // coverage files
    std::string plasma_term_command =
        "kill -TERM `cat " + store_socket_name_ + ".pid` || exit 0";
    PLASMA_CHECK_SYSTEM(system(plasma_term_command.c_str()));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
#endif
    std::string plasma_kill_command =
        "kill -KILL `cat " + store_socket_name_ + ".pid` || exit 0";
    PLASMA_CHECK_SYSTEM(system(plasma_kill_command.c_str()));
  }

 protected:
  PlasmaClient client_;
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string store_socket_name_;
};

TEST_F(TestPlasmaStoreWithExternal, EvictionTest) {
  std::vector<ObjectID> object_ids;
  std::string data(100 * 1024, 'x');
  std::string metadata;
  for (int i = 0; i < 20; i++) {
    ObjectID object_id = random_object_id();
    object_ids.push_back(object_id);

    // Test for object non-existence.
    bool has_object;
    ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
    ASSERT_FALSE(has_object);

    // Test for the object being in local Plasma store.
    // Create and seal the object.
    ARROW_CHECK_OK(client_.CreateAndSeal(object_id, data, metadata));
    // Test that the client can get the object.
    ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
    ASSERT_TRUE(has_object);
  }

  for (int i = 0; i < 20; i++) {
    // Since we are accessing objects sequentially, every object we
    // access would be a cache "miss" owing to LRU eviction.
    // Try and access the object from the plasma store first, and then try
    // external store on failure. This should succeed to fetch the object.
    // However, it may evict the next few objects.
    std::vector<ObjectBuffer> object_buffers;
    ARROW_CHECK_OK(client_.Get({object_ids[i]}, -1, &object_buffers));
    ASSERT_EQ(object_buffers.size(), 1);
    ASSERT_EQ(object_buffers[0].device_num, 0);
    ASSERT_TRUE(object_buffers[0].data);
    AssertObjectBufferEqual(object_buffers[0], metadata, data);
  }

  // Make sure we still cannot fetch objects that do not exist
  std::vector<ObjectBuffer> object_buffers;
  ARROW_CHECK_OK(client_.Get({random_object_id()}, 100, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 0);
  ASSERT_EQ(object_buffers[0].data, nullptr);
  ASSERT_EQ(object_buffers[0].metadata, nullptr);
}

}  // namespace plasma

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::external_test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
