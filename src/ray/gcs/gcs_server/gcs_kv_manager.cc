#include "ray/gcs/gcs_server/gcs_kv_manager.h"

namespace ray {
namespace gcs {

void GcsKVManager::HandleGet(const rpc::GetRequest &request, rpc::GetReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {
    "HGET",
    request.key(),
    "value"
  };
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        if(!redis_reply->IsNil()) {
          reply->set_value(redis_reply->ReadAsString());
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

void GcsKVManager::HandlePut(const rpc::PutRequest &request, rpc::PutReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {
    "HSET",
    request.key(),
    "value",
    request.value()
  };
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        auto status = redis_reply->ReadAsStatus();
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      });
}

void GcsKVManager::HandleDel(const rpc::DelRequest &request, rpc::DelReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {
    "HDEL",
    request.key(),
    "value"
  };
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

void GcsKVManager::HandleExists(const rpc::ExistsRequest &request,
                                rpc::ExistsReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {
    "HEXISTS",
    request.key(),
    "value"
  };
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        bool exists = redis_reply->ReadAsInteger() > 0;
        reply->set_exists(exists);
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

void GcsKVManager::HandleKeys(const rpc::KeysRequest &request, rpc::KeysReply *reply,
                              rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {
    "HKEYS",
    request.prefix(),
    "value"
  };
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        const auto& results = redis_reply->ReadAsStringArray();
        for(const auto& result : results) {
          reply->add_results(result);
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

}  // namespace gcs
}  // namespace ray
