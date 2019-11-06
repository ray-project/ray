#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

using ::testing::_;

class MockDirectActorClients : public DirectActorClientsInterface {
 public:
  MockDirectActorClients() {}

  MOCK_METHOD3(ConnectActor, void(const ActorID &actor_id, const std::string &ip_address,
                                  const int port));
  MOCK_METHOD1(DisconnectActor, void(const ActorID &actor_id));
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

    // Simulate resubmitting the actor creation task.
    ClientID node_id = ClientID::FromRandom();
    actor_manager_.OnActorLocationChanged(actor_id, node_id, "", 0);
  }

 protected:
  MockDirectActorClients mock_clients_;
  ActorManager actor_manager_;
  std::unordered_map<ActorID, uint64_t> num_restarts_;
};

// Helper functions to create child actors or actors that we only have
// references to.
std::pair<ActorID, TaskSpecification> MockActorCreationTask(
    uint64_t max_reconstructions) {
  JobID job_id = JobID::FromInt(0);
  TaskID caller_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, caller_id, 0);
  const std::unordered_map<std::string, double> resources;
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  std::vector<ObjectID> return_ids;
  TaskSpecBuilder builder;
  builder.BuildCommonTaskSpec(job_id, actor_creation_task_id, caller_id, 0, caller_id,
                              RayFunction(), {}, 1, resources, resources,
                              TaskTransportType::RAYLET, &return_ids);
  builder.SetActorCreationTaskSpec(actor_id, max_reconstructions);
  const ray::TaskSpecification spec = builder.Build();
  return {actor_id, spec};
}

void CreateChildActor(ActorManager &manager, uint64_t max_reconstructions,
                      ActorID *actor_id) {
  // Create an actor.
  auto actor = MockActorCreationTask(max_reconstructions);
  *actor_id = actor.first;
  auto spec = actor.second;
  manager.RegisterChildActor(spec);

  // Check that if we get the handle to the actor, it has no location.
  ActorHandle *handle;
  auto status = manager.GetActorHandle(*actor_id, &handle);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(!handle->IsDead());
  ASSERT_TRUE(handle->NodeId().IsNil());
}

void CreateActorReference(ActorManager &manager, uint64_t max_reconstructions,
                          ActorID *actor_id) {
  CreateChildActor(manager, max_reconstructions, actor_id);

  // Simulate receiving a handle to the same actor, but this time from a
  // serialized handle, so we do not own the actor.
  ActorHandle *handle;
  auto status = manager.GetActorHandle(*actor_id, &handle);
  ASSERT_TRUE(status.ok());
  std::string serialized;
  handle->Serialize(&serialized);
  manager.Clear();
  std::unique_ptr<ActorHandle> actor_handle(new ActorHandle(serialized));
  manager.AddActorHandle(std::move(actor_handle));
}

void StartActor(ActorManager &manager, MockDirectActorClients &mock_clients,
                const ActorID &actor_id, ClientID *node_id) {
  // Simulate receiving the actor's location.
  *node_id = ClientID::FromRandom();
  std::string ip_address("1234");
  int port = 1234;
  EXPECT_CALL(mock_clients, ConnectActor(actor_id, ip_address, port));
  manager.OnActorLocationChanged(actor_id, *node_id, ip_address, port);

  // Check that if we get the handle to the actor, it has the right node ID.
  ActorHandle *handle;
  auto status = manager.GetActorHandle(actor_id, &handle);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(!handle->IsDead());
  ASSERT_EQ(handle->NodeId(), *node_id);
}

// Test the common case where an actor is created, then dies because the
// application terminates it.
TEST_F(ActorManagerTest, TestActorDead) {
  ActorID actor_id;
  CreateChildActor(actor_manager_, 0, &actor_id);
  ClientID node_id;
  StartActor(actor_manager_, mock_clients_, actor_id, &node_id);
  // Simulate intentional termination by the application.
  EXPECT_CALL(mock_clients_, DisconnectActor(actor_id));
  actor_manager_.OnActorFailed(actor_id, /*terminal=*/true);
  // Check that the handle is now marked as dead.
  ActorHandle *handle;
  auto status = actor_manager_.GetActorHandle(actor_id, &handle);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(handle->IsDead());
  ASSERT_TRUE(handle->NodeId().IsNil());
}

// Test a failure scenario where an actor is created, then dies of a process or
// node failure.
TEST_F(ActorManagerTest, TestActorUnintentionallyDead) {
  ActorID actor_id;
  CreateChildActor(actor_manager_, 0, &actor_id);
  ClientID node_id;
  StartActor(actor_manager_, mock_clients_, actor_id, &node_id);
  // Simulate unintentional actor death. It should still be marked as dead
  // because we set max reconstructions to 0.
  EXPECT_CALL(mock_clients_, DisconnectActor(actor_id));
  actor_manager_.OnActorFailed(actor_id, /*terminal=*/false);
  // Check that the handle is now marked as dead.
  ActorHandle *handle;
  auto status = actor_manager_.GetActorHandle(actor_id, &handle);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(handle->IsDead());
  ASSERT_TRUE(handle->NodeId().IsNil());
}

// Test a failure scenario where an actor is created with max_reconstructions
// set, then dies of a process or node failure up to that many times.
TEST_F(ActorManagerTest, TestActorRestart) {
  // Create an actor that can be restarted.
  int max_reconstructions = 3;
  ActorID actor_id;
  CreateChildActor(actor_manager_, max_reconstructions, &actor_id);
  ClientID node_id;
  StartActor(actor_manager_, mock_clients_, actor_id, &node_id);
  // Simulate the actor failing max_reconstructions times.
  EXPECT_CALL(mock_clients_, DisconnectActor(actor_id)).Times(max_reconstructions);
  EXPECT_CALL(mock_clients_, ConnectActor(actor_id, _, _)).Times(max_reconstructions);
  for (int i = 0; i < max_reconstructions; i++) {
    actor_manager_.OnActorFailed(actor_id, /*terminal=*/false);
    // Check that the actor was restarted on a new node.
    ActorHandle *handle;
    auto status = actor_manager_.GetActorHandle(actor_id, &handle);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(!handle->IsDead());
    ClientID new_node_id = handle->NodeId();
    ASSERT_FALSE(new_node_id.IsNil());
    ASSERT_NE(node_id, new_node_id);
    node_id = new_node_id;
  }

  // Simulate the actor failing one more time. This time, it should not be
  // restarted.
  EXPECT_CALL(mock_clients_, DisconnectActor(actor_id));
  actor_manager_.OnActorFailed(actor_id, /*terminal=*/false);
  ActorHandle *handle;
  auto status = actor_manager_.GetActorHandle(actor_id, &handle);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(handle->IsDead());
}

// Test a common case where we receive a reference to an actor from its creator
// and it later dies because the application terminates it.
TEST_F(ActorManagerTest, TestActorReferenceDead) {
  ActorID actor_id;
  CreateActorReference(actor_manager_, 0, &actor_id);
  ClientID node_id;
  StartActor(actor_manager_, mock_clients_, actor_id, &node_id);
  // Simulate intentional termination by the application.
  EXPECT_CALL(mock_clients_, DisconnectActor(actor_id));
  actor_manager_.OnActorFailed(actor_id, /*terminal=*/true);
  // Check that the handle is now marked as dead.
  ActorHandle *handle;
  auto status = actor_manager_.GetActorHandle(actor_id, &handle);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(handle->IsDead());
  ASSERT_TRUE(handle->NodeId().IsNil());
}

// Test a failure scenario where an actor that we do not own is created with
// max_reconstructions set and later dies due to process or node failure.
TEST_F(ActorManagerTest, TestActorReferenceRestart) {
  // Create an actor that can be restarted.
  int max_reconstructions = 3;
  ActorID actor_id;
  CreateActorReference(actor_manager_, max_reconstructions, &actor_id);
  ClientID node_id;
  StartActor(actor_manager_, mock_clients_, actor_id, &node_id);
  // Simulate the actor failing. Since we don't own the actor, we should not
  // restart it, so we will not try to connect to it again.
  EXPECT_CALL(mock_clients_, DisconnectActor(actor_id));
  actor_manager_.OnActorFailed(actor_id, /*terminal=*/false);
  // Since we don't know own the actor, it should be pending creation, not dead.
  ActorHandle *handle;
  auto status = actor_manager_.GetActorHandle(actor_id, &handle);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(!handle->IsDead());
  ASSERT_TRUE(handle->NodeId().IsNil());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
