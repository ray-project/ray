#include "ray/gcs/store_client/store_client_factory.h"

#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/in_memory_store_client.h"

#include "ray/common/ray_config.h"

namespace ray {
namespace gcs {

std::unique_ptr<StoreClient> MakeStoreClient(
    std::shared_ptr<RedisClient> redis_client,
    instrumented_io_context &main_io_service) {
  if(RayConfig::instance().gcs_storage_backend() == "redis" ||
     RayConfig::instance().gcs_storage_backend() == "legacy") {
    return std::make_unique<RedisStoreClient>(redis_client);
  } else if (RayConfig::instance().gcs_storage_backend() == "memory") {
    return std::make_unique<InMemoryStoreClient>(main_io_service);
  }
  RAY_LOG(FATAL) << "Invalid gcs_storage_backend "
                 << RayConfig::instance().gcs_storage_backend();
  return nullptr;
}
}
}
