#include "ray/gcs/gcs_server/gcs_kv_manager.h"

namespace ray {
namespace gcs {

void GcsInternalKVManager::HandleInternalKVGet(
    const rpc::InternalKVGetRequest &request, rpc::InternalKVGetReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {"HGET", request.key(), "value"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        if (!redis_reply->IsNil()) {
          reply->set_value(redis_reply->ReadAsString());
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
        } else {
          GCS_RPC_SEND_REPLY(send_reply_callback, reply,
                             Status::NotFound("Failed to find the key"));
        }
      }));
}

void GcsInternalKVManager::HandleInternalKVPut(
    const rpc::InternalKVPutRequest &request, rpc::InternalKVPutReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {request.overwrite() ? "HSET" : "HSETNX", request.key(),
                                  "value", request.value()};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        reply->set_added_num(redis_reply->ReadAsInteger());
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      }));
}

void GcsInternalKVManager::HandleInternalKVDel(
    const rpc::InternalKVDelRequest &request, rpc::InternalKVDelReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {"HDEL", request.key(), "value"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        reply->set_deleted_num(redis_reply->ReadAsInteger());
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      }));
}

void GcsInternalKVManager::HandleInternalKVExists(
    const rpc::InternalKVExistsRequest &request, rpc::InternalKVExistsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {"HEXISTS", request.key(), "value"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        bool exists = redis_reply->ReadAsInteger() > 0;
        reply->set_exists(exists);
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      }));
}

void GcsInternalKVManager::HandleInternalKVKeys(
    const rpc::InternalKVKeysRequest &request, rpc::InternalKVKeysReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::string> cmd = {"KEYS", request.prefix() + "*"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [reply, send_reply_callback](auto redis_reply) {
        const auto &results = redis_reply->ReadAsStringArray();
        for (const auto &result : results) {
          reply->add_results(result);
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      }));
}

}  // namespace gcs
}  // namespace ray
