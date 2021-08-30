namespace ray {
namespace core {

class MockTaskFinisherInterface : public TaskFinisherInterface {
 public:
  MOCK_METHOD(void, CompletePendingTask,
              (const TaskID &task_id, const rpc::PushTaskReply &reply,
               const rpc::Address &actor_addr),
              (override));
  MOCK_METHOD(bool, PendingTaskFailed,
              (const TaskID &task_id, rpc::ErrorType error_type, Status *status,
               const std::shared_ptr<rpc::RayException> &creation_task_exception,
               bool immediately_mark_object_fail),
              (override));
  MOCK_METHOD(void, OnTaskDependenciesInlined,
              (const std::vector<ObjectID> &inlined_dependency_ids,
               const std::vector<ObjectID> &contained_ids),
              (override));
  MOCK_METHOD(bool, MarkTaskCanceled, (const TaskID &task_id), (override));
  MOCK_METHOD(void, MarkPendingTaskFailed,
              (const TaskID &task_id, const TaskSpecification &spec,
               rpc::ErrorType error_type,
               const std::shared_ptr<rpc::RayException> &creation_task_exception),
              (override));
  MOCK_METHOD(absl::optional<TaskSpecification>, GetTaskSpec, (const TaskID &task_id),
              (const, override));
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockTaskResubmissionInterface : public TaskResubmissionInterface {
 public:
  MOCK_METHOD(Status, ResubmitTask,
              (const TaskID &task_id, std::vector<ObjectID> *task_deps), (override));
};

}  // namespace core
}  // namespace ray
