#include <sys/types.h>
#include <unistd.h>

#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <thread>
#include <utility>

#include "src/ray/common/client_connection.h"
#include "src/ray/protobuf/asio.pb.h"
#include "src/ray/rpc/asio_client.h"
#include "src/ray/rpc/client.h"
#include "src/ray/rpc/common.h"

namespace ray {
namespace rpc {

Status AsioRpcClientImpl::Connect() {
  boost::asio::ip::tcp::socket socket(io_service_);
  RAY_RETURN_NOT_OK(TcpConnect(socket, address_, port_));

  ClientHandler<boost::asio::ip::tcp> client_handler = [](TcpClientConnection &client) {
    // Begin listening for messages.
    client.ProcessMessages();
  };
  auto this_ptr = shared_from_this();
  MessageHandler<boost::asio::ip::tcp> message_handler =
      [this, this_ptr](std::shared_ptr<TcpClientConnection> client, int64_t message_type,
                       uint64_t length, const uint8_t *message) {
        ProcessServerMessage(client, message_type, length, message);
      };

  const std::vector<std::string> asio_common_message_enum =
      GenerateEnumNames(RpcServiceType);

  // Accept a new TCP client and dispatch it to the node manager.
  connection_ = TcpClientConnection::Create(
      client_handler, message_handler, std::move(socket), name_, asio_common_message_enum,
      static_cast<int64_t>(ServiceMessageType::DisconnectClient));
  // Prepare connect message.
  ConnectClientMessage message;
  message.set_service_type(service_type_);

  std::string serialized_message;
  message.SerializeToString(&serialized_message);

  // Send synchronously.
  RAY_RETURN_NOT_OK(connection_->WriteMessage(
      static_cast<int64_t>(ServiceMessageType::ConnectClient),
      static_cast<int64_t>(serialized_message.size()),
      reinterpret_cast<const uint8_t *>(serialized_message.data())));

  is_connected_ = true;
  return Status::OK();
}

void AsioRpcClientImpl::ProcessServerMessage(
    const std::shared_ptr<TcpClientConnection> &client, int64_t message_type,
    uint64_t length, const uint8_t *message_data) {
  if (message_type == static_cast<int64_t>(ServiceMessageType::DisconnectClient)) {
    ProcessDisconnectClientMessage(client);
    // We don't need to receive future messages from this client,
    // because it's already disconnected.
    return;
  }

  RpcReplyMessage reply_message;
  reply_message.ParseFromArray(message_data, length);

  RAY_LOG(DEBUG) << "Processing server message for request id: "
                 << reply_message.request_id() << ", service: " << name_
                 << ", message type: " << message_type;

  const auto request_id = reply_message.request_id();
  ReplyCallback reply_callback;

  {
    std::unique_lock<std::mutex> guard(callback_mutex_);
    auto iter = pending_callbacks_.find(request_id);
    if (iter != pending_callbacks_.end()) {
      reply_callback = iter->second;
      pending_callbacks_.erase(iter);
    }
  }

  if (reply_callback != nullptr) {
    reply_callback(reply_message);
  }
  // TODO: what would happen if we don't find an entry rom pending_callbacks_?

  client->ProcessMessages();
}

void AsioRpcClientImpl::ProcessDisconnectClientMessage(
    const std::shared_ptr<TcpClientConnection> &client) {
  RAY_LOG(INFO) << "Received DiconnectClient message from server " << address_ << ":"
                << port_ << ", service: " << name_;

  // Mark is as not connected, so that further requests will be rejected with failure.
  is_connected_ = false;

  // Invoke all the callbacks that are pending replies, this is necessary so that
  // the transport can put exceptions into store for these object ids, to avoid
  // the client from getting blocked on `ray.get`.
  InvokeAndClearPendingCallbacks();
}

void AsioRpcClientImpl::InvokeAndClearPendingCallbacks() {
  std::unique_lock<std::mutex> guard(callback_mutex_);

  for (const auto &entry : pending_callbacks_) {
    Status status = Status::Invalid("rpc server died");
    RpcReplyMessage reply_message;
    reply_message.set_request_id(entry.first);
    reply_message.set_error_code(static_cast<uint32_t>(status.code()));
    reply_message.set_error_message(status.message());

    entry.second(reply_message);
  }

  pending_callbacks_.clear();
}

}  // namespace rpc
}  // namespace ray
