// Copyright 2021 The Ray Authors.
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


#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"


namespace ray {
namespace pubsub {

class IntegrationTest : public ::testing::Test {
 public:
  IntegrationTest() {}
  ~IntegrationTest() {}

};

}  // namespace pubsub
}  // namespace ray
