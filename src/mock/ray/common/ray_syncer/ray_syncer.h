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

namespace ray {
namespace syncer {

class MockReporterInterface : public ReporterInterface {
 public:
  MOCK_METHOD(std::optional<RaySyncMessage>,
              Snapshot,
              (uint64_t current_version, RayComponentId component_id),
              (const, override));
};

}  // namespace syncer
}  // namespace ray

namespace ray {
namespace syncer {

class MockReceiverInterface : public ReceiverInterface {
 public:
  MOCK_METHOD(void, Update, (std::shared_ptr<RaySyncMessage> message), (override));
  MOCK_METHOD(bool, NeedBroadcast, (), (const, override));
};

}  // namespace syncer
}  // namespace ray

namespace ray {
namespace syncer {

class MockRaySyncer : public RaySyncer {
 public:
};

}  // namespace syncer
}  // namespace ray

namespace ray {
namespace syncer {

class MockRaySyncerService : public RaySyncerService {
 public:
  MOCK_METHOD(grpc::ServerUnaryReactor *,
              StartSync,
              (grpc::CallbackServerContext * context,
               const SyncMeta *request,
               SyncMeta *response),
              (override));
  MOCK_METHOD(grpc::ServerUnaryReactor *,
              Update,
              (grpc::CallbackServerContext * context,
               const RaySyncMessages *request,
               DummyResponse *),
              (override));
  MOCK_METHOD(grpc::ServerUnaryReactor *,
              LongPolling,
              (grpc::CallbackServerContext * context,
               const DummyRequest *,
               RaySyncMessages *response),
              (override));
};

}  // namespace syncer
}  // namespace ray
