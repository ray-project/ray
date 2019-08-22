#ifndef RAY_RPC_ASIO_CLIENT_H
#define RAY_RPC_ASIO_CLIENT_H

#include <thread>
#include <utility>
#include <boost/asio.hpp>

#include "src/ray/common/client_connection.h"
#include "src/ray/rpc/common.h"
#include "src/ray/rpc/client.h"
#include "src/ray/protobuf/asio.pb.h"

namespace ray {
namespace rpc {


class RpcClient {
 public:
  explicit RpcClient(rpc::RpcServiceType service_type,
      std::string name, const std::string &address, const int port)
    : service_type_(service_type),
      name_(name),
      address_(address),
      port_(port) {}

  /// Destruct this gRPC server.
  virtual ~RpcClient() {}

  virtual Status Connect() = 0;

 protected:
  const rpc::RpcServiceType service_type_;
  /// Name of this client, used for logging and debugging purpose.
  const std::string name_;
  /// IP address of the server.
  const std::string address_;
  /// Port of the server.
  int port_;
};

/// Class that represents an asio based rpc server.
///
/// An `AsioRpcServer` listens on a specific port. 
///
/// Subclasses can register one or multiple services to a `AsioRpcServer`, see
/// `RegisterServices`. And they should also implement `InitServerCallFactories` to decide
/// which kinds of requests this server should accept.
class AsioRpcClient : public RpcClient {
 public:

  explicit AsioRpcClient(rpc::RpcServiceType service_type,
      const std::string &address, const int port,
      boost::asio::io_service &io_service)
    : RpcClient(service_type, RpcServiceType_Name(service_type), address, port),
      io_service_(io_service),
      request_id_(0),
      is_connected_(false) {}

  Status Connect() override {
    boost::asio::ip::tcp::socket socket(io_service_);
    RAY_RETURN_NOT_OK(TcpConnect(socket, address_, port_));

    ClientHandler<boost::asio::ip::tcp> client_handler =
        [](TcpClientConnection &client) {
          // Begin listening for messages.
          client.ProcessMessages();
        };
    MessageHandler<boost::asio::ip::tcp> message_handler =
        [this](std::shared_ptr<TcpClientConnection> client, int64_t message_type,
                uint64_t length, const uint8_t *message) {
          ProcessServerMessage(client, message_type, length, message);
        };

    const std::vector<std::string> asio_common_message_enum =
        GenerateEnumNames(RpcServiceType);

    // Accept a new TCP client and dispatch it to the node manager.
    connection_ = TcpClientConnection::Create(
        client_handler, message_handler, std::move(socket), name_,
        asio_common_message_enum,
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

  /// Create a new `ClientCall` and send request.
  ///
  /// \tparam GrpcService Type of the gRPC-generated service class.
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] stub The gRPC-generated stub.
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \return A `ClientCall` representing the request that was just sent.
  template <class Request, class Reply, class MessageType>
  Status CallMethod(MessageType request_type, MessageType reply_type,
      const Request &request, const ClientCallback<Reply> &callback) {

    if (connection_ == nullptr || !is_connected_) {
      // There are errors, invoke the callback.
      auto status = Status::Invalid("server is not connected");
      Reply reply;
      callback(status, reply);     
      return status;
    }

    RpcRequestMessage request_message;
    auto request_id = ++request_id_;
    request_message.set_request_id(request_id);           
    request.SerializeToString(request_message.mutable_request());

    std::string serialized_message;
    request_message.SerializeToString(&serialized_message);

    RAY_LOG(DEBUG) << "Calling method for service " << name_
                  << ", request id: " << request_id
                  << ", request type: " << static_cast<int>(request_type);

    auto status = connection_->WriteMessage(request_type,
        static_cast<int64_t>(serialized_message.size()),
        reinterpret_cast<const uint8_t *>(serialized_message.data()));
    if (status.ok()) {
      // Send succeeds. Add the request to the records, so that
      // we can invoke the callback after receivig the reply.
      std::unique_lock<std::mutex> guard(callback_mutex_);
      pending_callbacks_.emplace(request_id,
        [callback, reply_type, this](const RpcReplyMessage &reply_message) {

          const auto request_id = reply_message.request_id();
          auto error_code = static_cast<StatusCode>(reply_message.error_code());
          auto error_message = reply_message.error_message();  
          Status status = (error_code == StatusCode::OK) ?
              Status::OK() : Status(error_code, error_message);

          Reply reply;
          reply.ParseFromString(reply_message.reply());

          callback(status, reply); 
          RAY_LOG(DEBUG) << "Calling reply callback for message " << static_cast<int>(reply_type)
                        << " for service " << name_ << ", request id " << request_id
                        << ", status: " << status.ToString();                           
      });

    } else {
      // There are errors, invoke the callback.
      Reply reply;
      callback(status, reply);
      RAY_LOG(DEBUG) << "Failed to write request message " << static_cast<int>(request_type)
                    << " for service " << name_ << " to " << address_ << ":" << port_
                    << ", request id " << request_id << ", status: " << status.ToString();              
    }
  

/*


    connection_->WriteMessageAsync(request_type,
        static_cast<int64_t>(serialized_message.size()),
        reinterpret_cast<const uint8_t *>(serialized_message.data()),
        [request_id, callback, request_type, reply_type, this](const ray::Status &status) {
          if (status.ok()) {
            // Send succeeds. Add the request to the records, so that
            // we can invoke the callback after receivig the reply.
            std::unique_lock<std::mutex> guard(callback_mutex_);
            pending_callbacks_.emplace(request_id,
              [callback, reply_type](const RpcReplyMessage &reply_message) {

                const auto request_id = reply_message.request_id();
                auto error_code = static_cast<StatusCode>(reply_message.error_code());
                auto error_message = reply_message.error_message();  
                Status status = (error_code == StatusCode::OK) ?
                    Status::OK() : Status(error_code, error_message);

                Reply reply;
                reply.ParseFromString(reply_message.reply());

                callback(status, reply); 
                RAY_LOG(INFO) << "Calling reply callback for message " << static_cast<int>(reply_type)
                              << ", request id " << request_id << ", status: " << status.ToString();                           
            });

          } else {
            // There are errors, invoke the callback.
            Reply reply;
            callback(status, reply);
            RAY_LOG(INFO) << "Failed to write request message " << static_cast<int>(request_type)
                          << " for service " << name_ << " to " << address_ << ":" << port_
                          << ", request id " << request_id << ", status: " << status.ToString();              
          }
        });

*/

    return Status::OK();
  }

 protected:

  void ProcessServerMessage(
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

    RAY_LOG(DEBUG) << "Processing server message for request id: " << reply_message.request_id()
                  << ", service: " << name_ << ", message type: " << message_type;  

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

  void ProcessDisconnectClientMessage(
      const std::shared_ptr<TcpClientConnection> &client) {
    RAY_LOG(INFO) << "Received DiconnectClient message from server "
                  << address_ << ":" << port_ << ", service: " << name_; 
    
    is_connected_ = false;

    // Invoke all the callbacks that are pending replies, this is necessary so that
    // the transport can put exceptions into store for these object ids, to avoid
    // the client from getting blocked on `ray.get`.
    {
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
  }


  using ReplyCallback = std::function<void(const RpcReplyMessage &)>;
  /// Map from request id to the corresponding reply callback, which will be
  /// invoked when the reply is received for the request.
  std::unordered_map<uint64_t, ReplyCallback> pending_callbacks_;
  /// Mutex to protect the `pending_callbacks_` above.
  std::mutex callback_mutex_;

  /// IO service to handle the service calls.
  boost::asio::io_service &io_service_;
  /// Connection to server. Note that TCP is full-duplex, and it's OK for
  /// read and write simultaneously in different threads, provided that
  /// there's only one thread for read and one for write. In this case
  /// we don't need a lock for it.
  std::shared_ptr<TcpClientConnection> connection_;

  // Request sequence id which starts with 1.
  std::atomic_uint64_t request_id_;

  /// Whether we have connected to server.
  std::atomic_bool is_connected_;
};

}  // namespace rpc
}  // namespace ray

#endif