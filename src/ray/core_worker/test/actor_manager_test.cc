#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

class MockDirectActorClients : public DirectActorClientsInterface {
 public:
  MockDirectActorClients() {}

  void ConnectActor(const ActorID &actor_id, const std::string &ip_address,
                    const int port) {
    clients_.insert(actor_id);
  }
  void DisconnectActor(const ActorID &actor_id) { clients_.erase(actor_id); }

  std::unordered_set<ActorID> clients_;
};

class ActorManagerTest : public ::testing::Test {
 public:
  ActorManagerTest()
      : mock_clients_(),
        actor_manager_(mock_clients_, [this](const ActorID &actor_id,
                                             const TaskSpecification &actor_creation_spec,
                                             uint64_t num_lifetimes) {
          HandleActorCreation(actor_id, num_lifetimes);
        }) {}

  void HandleActorCreation(const ActorID &actor_id, uint64_t num_lifetimes) {
    ASSERT_EQ(num_lifetimes, num_restarts_[actor_id] + 1);
    num_restarts_[actor_id]++;
  }

 private:
  MockDirectActorClients mock_clients_;
  ActorManager actor_manager_;
  std::unordered_map<ActorID, uint64_t> num_restarts_;
};

TEST_F(ActorManagerTest, TestActorRestart) {}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
