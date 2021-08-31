namespace ray {
namespace raylet {

class MockTaskDependencyManagerInterface : public TaskDependencyManagerInterface {
 public:
  MOCK_METHOD(bool, RequestTaskDependencies, (const TaskID &task_id, const std::vector<rpc::ObjectReference> &required_objects), (override));
  MOCK_METHOD(void, RemoveTaskDependencies, (const TaskID &task_id), (override));
  MOCK_METHOD(bool, TaskDependenciesBlocked, (const TaskID &task_id), (const, override));
  MOCK_METHOD(bool, CheckObjectLocal, (const ObjectID &object_id), (const, override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockDependencyManager : public DependencyManager {
 public:
};

}  // namespace raylet
}  // namespace ray
