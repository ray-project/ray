#include "ray/common/task/task.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"

namespace ray {

namespace raylet {

Task CreateTask(const std::unordered_map<std::string, double> &required_resources,
                int num_args = 0, std::vector<ObjectID> args = {},
                std::string serialized_runtime_env = "{}") {
  TaskSpecBuilder spec_builder;
  TaskID id = RandomTaskId();
  JobID job_id = RandomJobId();
  rpc::Address address;
  spec_builder.SetCommonTaskSpec(
      id, "dummy_task", Language::PYTHON,
      FunctionDescriptorBuilder::BuildPython("", "", "", ""), job_id, TaskID::Nil(), 0,
      TaskID::Nil(), address, 0, required_resources, {},
      std::make_pair(PlacementGroupID::Nil(), -1), true, "", serialized_runtime_env);

  if (!args.empty()) {
    for (auto &arg : args) {
      spec_builder.AddArg(TaskArgByReference(arg, rpc::Address()));
    }
  } else {
    for (int i = 0; i < num_args; i++) {
      ObjectID put_id = ObjectID::FromIndex(RandomTaskId(), /*index=*/i + 1);
      spec_builder.AddArg(TaskArgByReference(put_id, rpc::Address()));
    }
  }

  rpc::TaskExecutionSpec execution_spec_message;
  execution_spec_message.set_num_forwards(1);
  return Task(spec_builder.Build(), TaskExecutionSpecification(execution_spec_message));
}

}  // namespace raylet

}  // namespace ray
