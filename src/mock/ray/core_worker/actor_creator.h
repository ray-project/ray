namespace ray {
namespace core {

class MockActorCreatorInterface : public ActorCreatorInterface {
 public:
  MOCK_METHOD(Status, RegisterActor, (const TaskSpecification &task_spec), (override));
  MOCK_METHOD(Status, AsyncRegisterActor,
              (const TaskSpecification &task_spec, gcs::StatusCallback callback),
              (override));
  MOCK_METHOD(Status, AsyncCreateActor,
              (const TaskSpecification &task_spec, const gcs::StatusCallback &callback),
              (override));
  MOCK_METHOD(void, AsyncWaitForActorRegisterFinish,
              (const ActorID &actor_id, gcs::StatusCallback callback), (override));
  MOCK_METHOD(bool, IsActorInRegistering, (const ActorID &actor_id), (const, override));
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockDefaultActorCreator : public DefaultActorCreator {
 public:
  MOCK_METHOD(Status, RegisterActor, (const TaskSpecification &task_spec), (override));
  MOCK_METHOD(Status, AsyncRegisterActor,
              (const TaskSpecification &task_spec, gcs::StatusCallback callback),
              (override));
  MOCK_METHOD(bool, IsActorInRegistering, (const ActorID &actor_id), (const, override));
  MOCK_METHOD(Status, AsyncCreateActor,
              (const TaskSpecification &task_spec, const gcs::StatusCallback &callback),
              (override));
};

}  // namespace core
}  // namespace ray
