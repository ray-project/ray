#pragma once

#include "absl/container/flat_hash_set.h"
#include "ray/common/client_connection.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/compat.h"

namespace plasma {

namespace flatbuf {
enum class MessageType : int64_t;
}

class Client;

using PlasmaStoreMessageHandler = std::function<ray::Status(
    std::shared_ptr<Client>, flatbuf::MessageType, const std::vector<uint8_t> &)>;

class ClientInterface {
 public:
  virtual ~ClientInterface() {}

  virtual ray::Status SendFd(MEMFD_TYPE fd) = 0;
  virtual const std::unordered_set<ray::ObjectID> &GetObjectIDs() = 0;
  virtual void MarkObjectAsUsed(const ray::ObjectID &object_id) = 0;
  virtual void MarkObjectAsUnused(const ray::ObjectID &object_id) = 0;
};

/// Contains all information that is associated with a Plasma store client.
class Client : public ray::ClientConnection, public ClientInterface {
 public:
  static std::shared_ptr<Client> Create(PlasmaStoreMessageHandler message_handler,
                                        ray::local_stream_socket &&socket);

  ray::Status SendFd(MEMFD_TYPE fd) override;

  const std::unordered_set<ray::ObjectID> &GetObjectIDs() override { return object_ids; }

  virtual void MarkObjectAsUsed(const ray::ObjectID &object_id) override {
    object_ids.insert(object_id);
  }

  virtual void MarkObjectAsUnused(const ray::ObjectID &object_id) override {
    object_ids.erase(object_id);
  }

  std::string name = "anonymous_client";

 private:
  Client(ray::MessageHandler &message_handler, ray::local_stream_socket &&socket);
  /// File descriptors that are used by this client.
  /// TODO(ekl) we should also clean up old fds that are removed.
  absl::flat_hash_set<MEMFD_TYPE> used_fds_;

  /// Object ids that are used by this client.
  std::unordered_set<ray::ObjectID> object_ids;
};

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<Client> &client);

/// Contains all information that is associated with a Plasma store client.
class StoreConn : public ray::ServerConnection {
 public:
  StoreConn(ray::local_stream_socket &&socket);

  /// Receive a file descriptor for the store.
  ///
  /// \return A file descriptor.
  ray::Status RecvFd(MEMFD_TYPE_NON_UNIQUE *fd);
};

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<StoreConn> &store_conn);

}  // namespace plasma
