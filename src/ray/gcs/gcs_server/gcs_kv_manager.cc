#include "ray/gcs/gcs_server/gcs_kv_manager.h"

namespace ray {
namespace gcs {

void GcsKVManager::HandleGet(const rpc::GetRequest &request, rpc::GetReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  store_client_->AsyncGet(table_name_, request.key(),
                          [reply, send_reply_callback](
                              Status status, const boost::optional<std::string> &result) {
                            if (result) {
                              reply->set_value(*result);
                            }
                            GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
                          });
}

void GcsKVManager::HandlePut(const rpc::PutRequest &request, rpc::PutReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  store_client_->AsyncPut(table_name_, request.key(), request.value(),
                          [reply, send_reply_callback](Status status) {
                            GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
                          });
}

void GcsKVManager::HandleDel(const rpc::DelRequest &request, rpc::DelReply *reply,
                             rpc::SendReplyCallback send_reply_callback) {
  store_client_->AsyncDelete(table_name_, request.key(),
                             [reply, send_reply_callback](Status status) {
                               GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
                             });
}

void GcsKVManager::HandleExists(const rpc::ExistsRequest &request,
                                rpc::ExistsReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  store_client_->AsyncExists(
      table_name_, request.key(),
      [reply, send_reply_callback](Status status, const boost::optional<bool> &result) {
        if (result) {
          reply->set_exists(*result);
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      });
}

void GcsKVManager::HandleKeys(const rpc::KeysRequest &request, rpc::KeysReply *reply,
                              rpc::SendReplyCallback send_reply_callback) {
  store_client_->AsyncKeys(
      table_name_, request.prefix(),
      [reply, send_reply_callback](
          Status status, const boost::optional<std::vector<std::string>> &results) {
        if (results) {
          for (const auto &r : *results) {
            reply->add_results(r);
          }
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      });
}

}  // namespace gcs
}  // namespace ray
