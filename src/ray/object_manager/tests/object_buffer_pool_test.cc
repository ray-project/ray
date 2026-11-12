// Copyright 2017 The Ray Authors.
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

#include "ray/object_manager/object_buffer_pool.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/object_manager/plasma/fake_plasma_client.h"

namespace ray {

class CustomFakePlasmaClient : public plasma::FakePlasmaClient {
 public:
  ray::Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                     const ray::rpc::Address &owner_address,
                                     bool is_experimental_mutable_object,
                                     int64_t data_size,
                                     const uint8_t *metadata,
                                     int64_t metadata_size,
                                     std::shared_ptr<Buffer> *data,
                                     plasma::flatbuf::ObjectSource source,
                                     int device_num) override {
    *data = std::make_shared<LocalMemoryBuffer>(data_size);
    return ray::Status::OK();
  }
};

class ObjectBufferPoolTest : public ::testing::Test {
 public:
  ObjectBufferPoolTest()
      : chunk_size_(1000),
        fake_plasma_client_(std::make_shared<CustomFakePlasmaClient>()),
        object_buffer_pool_(fake_plasma_client_, chunk_size_),
        fake_data_(chunk_size_, 'x') {}

  void AssertNoLeaks() {
    absl::MutexLock lock(&object_buffer_pool_.pool_mutex_);
    ASSERT_TRUE(object_buffer_pool_.create_buffer_state_.empty());
    ASSERT_TRUE(object_buffer_pool_.create_buffer_ops_.empty());
  }

  uint64_t chunk_size_;
  std::shared_ptr<CustomFakePlasmaClient> fake_plasma_client_;
  ObjectBufferPool object_buffer_pool_;
  std::string fake_data_;
};

TEST_F(ObjectBufferPoolTest, TestBasic) {
  auto obj_id = ObjectID::FromRandom();
  rpc::Address owner_address;

  ASSERT_TRUE(
      object_buffer_pool_.CreateChunk(obj_id, owner_address, chunk_size_, 0, 0).ok());
  ASSERT_FALSE(
      object_buffer_pool_.CreateChunk(obj_id, owner_address, chunk_size_, 0, 0).ok());
  fake_plasma_client_->ClearCallRecords();
  object_buffer_pool_.WriteChunk(obj_id, chunk_size_, 0, 0, fake_data_);
  EXPECT_EQ(fake_plasma_client_->sealed_objects, (std::vector<ObjectID>{obj_id}));
  EXPECT_EQ(fake_plasma_client_->released_objects, (std::vector<ObjectID>{obj_id}));
}

TEST_F(ObjectBufferPoolTest, TestMultiChunk) {
  auto obj_id = ObjectID::FromRandom();
  rpc::Address owner_address;

  for (int i = 0; i < 3; i++) {
    ASSERT_TRUE(
        object_buffer_pool_.CreateChunk(obj_id, owner_address, 3 * chunk_size_, 0, i)
            .ok());
    ASSERT_FALSE(
        object_buffer_pool_.CreateChunk(obj_id, owner_address, 3 * chunk_size_, 0, i)
            .ok());
  }
  fake_plasma_client_->ClearCallRecords();
  for (int i = 0; i < 3; i++) {
    object_buffer_pool_.WriteChunk(obj_id, 3 * chunk_size_, 0, i, fake_data_);
  }
  EXPECT_EQ(fake_plasma_client_->sealed_objects, (std::vector<ObjectID>{obj_id}));
  EXPECT_EQ(fake_plasma_client_->released_objects, (std::vector<ObjectID>{obj_id}));
}

TEST_F(ObjectBufferPoolTest, TestAbort) {
  auto obj_id = ObjectID::FromRandom();
  rpc::Address owner_address;

  ASSERT_TRUE(
      object_buffer_pool_.CreateChunk(obj_id, owner_address, chunk_size_, 0, 0).ok());
  ASSERT_FALSE(
      object_buffer_pool_.CreateChunk(obj_id, owner_address, chunk_size_, 0, 0).ok());
  fake_plasma_client_->ClearCallRecords();
  object_buffer_pool_.AbortCreate(obj_id);
  EXPECT_EQ(fake_plasma_client_->aborted_objects, (std::vector<ObjectID>{obj_id}));
  ASSERT_TRUE(
      object_buffer_pool_.CreateChunk(obj_id, owner_address, chunk_size_, 0, 0).ok());

  fake_plasma_client_->ClearCallRecords();
  object_buffer_pool_.WriteChunk(obj_id, chunk_size_, 0, 0, fake_data_);
  EXPECT_EQ(fake_plasma_client_->sealed_objects, (std::vector<ObjectID>{obj_id}));
  EXPECT_EQ(fake_plasma_client_->released_objects, (std::vector<ObjectID>{obj_id}));
}

TEST_F(ObjectBufferPoolTest, TestSizeMismatch) {
  auto obj_id = ObjectID::FromRandom();
  rpc::Address owner_address;

  int64_t data_size_1 = 3 * chunk_size_;
  int64_t data_size_2 = 2 * chunk_size_;
  ASSERT_TRUE(
      object_buffer_pool_.CreateChunk(obj_id, owner_address, data_size_1, 0, 0).ok());
  object_buffer_pool_.WriteChunk(obj_id, data_size_1, 0, 0, fake_data_);

  // Object gets created again with a different size: the partial buffer is
  // released and aborted.
  fake_plasma_client_->ClearCallRecords();
  ASSERT_TRUE(
      object_buffer_pool_.CreateChunk(obj_id, owner_address, data_size_2, 0, 1).ok());
  object_buffer_pool_.WriteChunk(obj_id, data_size_2, 0, 1, fake_data_);
  EXPECT_EQ(fake_plasma_client_->released_objects, (std::vector<ObjectID>{obj_id}));
  EXPECT_EQ(fake_plasma_client_->aborted_objects, (std::vector<ObjectID>{obj_id}));

  ASSERT_TRUE(
      object_buffer_pool_.CreateChunk(obj_id, owner_address, data_size_2, 0, 0).ok());
  // Writing a chunk with a stale data size has no effect.
  object_buffer_pool_.WriteChunk(obj_id, data_size_1, 0, 0, fake_data_);

  fake_plasma_client_->ClearCallRecords();
  object_buffer_pool_.WriteChunk(obj_id, data_size_2, 0, 0, fake_data_);
  EXPECT_EQ(fake_plasma_client_->sealed_objects, (std::vector<ObjectID>{obj_id}));
  EXPECT_EQ(fake_plasma_client_->released_objects, (std::vector<ObjectID>{obj_id}));
}

}  // namespace ray
