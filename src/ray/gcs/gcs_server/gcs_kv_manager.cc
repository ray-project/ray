#include "ray/gcs/gcs_server/gcs_kv_manager.h"

namespace ray {
namespace gcs {

void GcsKVManager::HandleGet(const rpc::GetRequest &request, rpc::GetReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {"HGET", request.key(), "value"};
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        if (!redis_reply->IsNil()) {
          reply->set_value(redis_reply->ReadAsString());
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

void GcsKVManager::HandlePut(const rpc::PutRequest &request, rpc::PutReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {request.overwrite() ? "HSET" : "HSETNX", request.key(),
                                  "value", request.value()};
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        reply->set_added_num(redis_reply->ReadAsInteger());
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

void GcsKVManager::HandleDel(const rpc::DelRequest &request, rpc::DelReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  AsyncDel(request.key(), [reply, send_reply_callback](int deleted_num) {
    reply->set_deleted_num(deleted_num);
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  });
}

void GcsKVManager::AsyncDel(const std::string& key,
                            std::function<void(int)> cb) {
  std::vector<std::string> cmd = {"HDEL", key, "value"};
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd,
      [cb](auto redis_reply) {
        cb(redis_reply->ReadAsInteger());
      });
}

void GcsKVManager::HandleExists(const rpc::ExistsRequest &request,
                                rpc::ExistsReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {"HEXISTS", request.key(), "value"};
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        bool exists = redis_reply->ReadAsInteger() > 0;
        reply->set_exists(exists);
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

void GcsKVManager::HandleKeys(const rpc::KeysRequest &request, rpc::KeysReply *reply,
                              rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {"KEYS", request.prefix() + "*"};
  redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        const auto &results = redis_reply->ReadAsStringArray();
        for (const auto &result : results) {
          reply->add_results(result);
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

}  // namespace gcs
}  // namespace ray
