#include "health_check_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultHealthCheckHandler::HandlePing(
    const PingRequest &request, PingReply *reply,
    SendReplyCallback send_reply_callback) {
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace rpc
}  // namespace ray
