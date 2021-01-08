#include "ray/streaming/streaming.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace streaming {

void SendInternal(const ActorID &peer_actor_id, std::shared_ptr<LocalMemoryBuffer> buffer,
                  RayFunction &function, int return_num,
                  std::vector<ObjectID> &return_ids) {
  std::unordered_map<std::string, double> resources;
  std::string name = function.GetFunctionDescriptor()->DefaultTaskName();
  TaskOptions options{name, return_num, resources};

  char meta_data[3] = {'R', 'A', 'W'};
  std::shared_ptr<LocalMemoryBuffer> meta =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, 3, true);

  std::vector<std::unique_ptr<TaskArg>> args;
  if (function.GetLanguage() == Language::PYTHON) {
    auto dummy = "__RAY_DUMMY__";
    std::shared_ptr<LocalMemoryBuffer> dummyBuffer =
        std::make_shared<LocalMemoryBuffer>((uint8_t *)dummy, 13, true);
    args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
        std::move(dummyBuffer), meta, std::vector<ObjectID>(), true)));
  }
  args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
      std::move(buffer), meta, std::vector<ObjectID>(), true)));

  std::vector<std::shared_ptr<RayObject>> results;
  CoreWorkerProcess::GetCoreWorker().SubmitActorTask(peer_actor_id, function, args,
                                                     options, &return_ids);
}
}  // namespace streaming
}  // namespace ray
