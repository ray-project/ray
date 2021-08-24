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

#include "ray/object_manager/plasma/get_request_queue.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace ray;
using namespace testing;

namespace plasma {

class MyTest {
 private:
  MyTest(int a) {}

  friend struct GetRequestQueeuTest;
};

struct GetRequestQueueTest : public Test {
 public:
  GetRequestQueueTest() {}
  void SetUp() override {
    test_ = std::make_shared<MyTest>(MyTest(100));
    test_ = std::make_shared<MyTest>(100);
  }

 protected:
  std::shared_ptr<MyTest> test_;
};

}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
