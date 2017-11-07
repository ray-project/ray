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

namespace ray {

class TestGCS : public ::testing::Test {
};

void ObjectAdded(gcs::AsyncGCSClient* client, const UniqueID& id, std::shared_ptr<ObjectTableDataT> data) {
  std::cout << "added object" << std::endl;
}

TEST_F(TestGCS, TestClient) {
  gcs::AsyncGCSClient client;
  RAY_CHECK_OK(client.Connect("127.0.0.1", 6379));
  auto data = std::make_shared<ObjectTableDataT>();
  UniqueID job_id = UniqueID::from_random();
  UniqueID object_id = UniqueID::from_random();
  RAY_CHECK_OK(client.object_table().Add(job_id, object_id, data, &ObjectAdded));
}

}
