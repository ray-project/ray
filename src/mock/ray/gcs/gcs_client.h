// Copyright  The Ray Authors.
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

#pragma once

#include "mock/ray/gcs/accessor.h"

namespace ray {
namespace gcs {

class MockGcsClientOptions : public GcsClientOptions {
 public:
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsClient : public GcsClient {
 public:
  MOCK_METHOD(Status, Connect, (instrumented_io_context & io_service), (override));
  MOCK_METHOD(void, Disconnect, (), (override));
  MOCK_METHOD((std::pair<std::string, int>), GetGcsServerAddress, (), (override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));
  MockGcsClient() {
    mock_actor_accessor = new MockActorInfoAccessor();
    GcsClient::actor_accessor_.reset(mock_actor_accessor);
  }
  MockActorInfoAccessor *mock_actor_accessor;
};

}  // namespace gcs
}  // namespace ray
