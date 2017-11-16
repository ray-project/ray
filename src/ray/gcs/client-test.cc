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

#include "gtest/gtest.h"

#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"

// TODO(pcm): get rid of this and replace with the type safe plasma event loop
extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
}

namespace ray {

aeEventLoop* loop;

class TestGCS : public ::testing::Test {
 public:
  TestGCS() {
    RAY_CHECK_OK(client_.Connect("127.0.0.1", 6379));
    job_id_ = UniqueID::from_random();
  }
 protected:
  gcs::AsyncGCSClient client_;
  UniqueID job_id_;
};

void ObjectAdded(gcs::AsyncGCSClient* client, const UniqueID& id, std::shared_ptr<ObjectTableDataT> data) {
  std::cout << "added object" << std::endl;
}

void Lookup(gcs::AsyncGCSClient* client, const UniqueID& id, std::shared_ptr<ObjectTableDataT> data) {
  std::cout << "looked up object" << std::endl;
  aeStop(loop);
}

TEST_F(TestGCS, TestObjectTableAdd) {
  loop = aeCreateEventLoop(1024);
  RAY_CHECK_OK(client_.context()->AttachToEventLoop(loop));
  auto data = std::make_shared<ObjectTableDataT>();
  UniqueID object_id = UniqueID::from_random();
  RAY_CHECK_OK(client_.object_table().Add(job_id_, object_id, data, &ObjectAdded));
  RAY_CHECK_OK(client_.object_table().Lookup(job_id_, object_id, &Lookup, &Lookup));
  aeMain(loop);
}

}
