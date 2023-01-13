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
              CreateSyncMessage,
              (int64_t current_version, MessageType message_type),
              (const, override));
};

}  // namespace syncer
}  // namespace ray

namespace ray {
namespace syncer {

class MockReceiverInterface : public ReceiverInterface {
 public:
  MOCK_METHOD(void,
              ConsumeSyncMessage,
              (std::shared_ptr<const RaySyncMessage> message),
              (override));
};

}  // namespace syncer
}  // namespace ray

namespace ray {
namespace syncer {

class MockRaySyncerBidiReactorBase : public RaySyncerBidiReactorBase {
 public:
  using RaySyncerBidiReactorBase::RaySyncerBidiReactorBase;

  MOCK_METHOD(void, Disconnect, (), (override));

  MOCK_METHOD(void, Send, (std::shared_ptr<const RaySyncMessage>, bool), (override));
};

}  // namespace syncer
}  // namespace ray
