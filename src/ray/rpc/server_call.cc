#include "ray/common/ray_config.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace rpc {

static std::unique_ptr<boost::asio::thread_pool> executor_;

boost::asio::thread_pool& GetServerCallExecutor() {
  if(executor_ == nullptr) {
    executor_ = std::make_unique<boost::asio::thread_pool>(::RayConfig::instance().num_server_call_thread());
  }
  return *executor_;
}

}
}
