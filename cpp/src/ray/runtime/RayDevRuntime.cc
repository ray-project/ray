
#include "RayDevRuntime.h"

#include <ray/api.h>
#include "../agent.h"
#include "../spi/TaskInterfaceMock.h"
#include "../spi/ObjectInterfaceMock.h"
#include "../util/BlobUtil.h"

namespace ray {

std::unordered_map<UniqueId, char *> RayDevRuntime::_actors;

RayDevRuntime::RayDevRuntime(std::shared_ptr<RayConfig> params) {
  _params = params;
  std::unique_ptr<Worker> work_ptr(new Worker(params));
  _worker = std::move(work_ptr);

  std::unique_ptr<TaskInterface> sch_client_ptr(new TaskInterfaceMock());
  std::unique_ptr<TaskProxy> sch_proxy_ptr(
      new TaskProxy(std::move(sch_client_ptr)));
  _taskProxy = std::move(sch_proxy_ptr);

  std::unique_ptr<ObjectInterface> store_client_ptr(new ObjectInterfaceMock());
  std::unique_ptr<ObjectProxy> store_proxy_ptr(
      new ObjectProxy(std::move(store_client_ptr)));
  _objectProxy = std::move(store_proxy_ptr);
}

std::unique_ptr<UniqueId> RayDevRuntime::createActor(remote_function_ptr_holder &fptr,
                                                     std::vector< ::ray::blob> &&args) {
  RayRuntime &runtime = RayRuntime::getInstance();
  std::unique_ptr<UniqueId> id = runtime.getCurrentTaskId().taskComputeReturnId(0);
  typedef std::vector< ::ray::blob> (*EXEC_FUNCTION)(
      uintptr_t base_addr, int32_t func_offset, const ::ray::blob &args);
  EXEC_FUNCTION exec_function = (EXEC_FUNCTION)(fptr.value[1]);
  ::ray::blob arg = blob_merge(args);
  auto data =
      (*exec_function)(dylib_base_addr, (int32_t)(fptr.value[0] - dylib_base_addr), arg);
  std::unique_ptr< ::ray::blob> bb = blob_merge_to_ptr(data);
  ::ray::blob *object = new ::ray::blob(*bb);
  _actors.emplace(*id, (char *)object);
  return id;
}

std::unique_ptr<UniqueId> RayDevRuntime::create(remote_function_ptr_holder &fptr,
                                                std::vector< ::ray::blob> &&args) {
  // TODO: Ray::call support right value input
  // TODO: createActor with Ray::call
  // return (Ray::call(RayDevRuntime::createActor, fptr, args)).id().copy();
  return createActor(fptr, std::move(args));
}

char *RayDevRuntime::get_actor_ptr(const UniqueId &id) {
  auto it = _actors.find(id);
  if (it != _actors.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

}  // namespace ray