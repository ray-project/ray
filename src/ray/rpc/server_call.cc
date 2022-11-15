#include "ray/rpc/server_call.h"

#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {

boost::asio::thread_pool &GetServerCallExecutor() {
  static boost::asio::thread_pool thread_pool(
      ::RayConfig::instance().num_server_call_thread());
  return thread_pool;
}

}  // namespace rpc
}  // namespace ray
