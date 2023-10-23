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
  virtual void MarkObjectAsUsed(const ray::ObjectID &object_id,
                                std::optional<MEMFD_TYPE> fallback_allocated_fd) = 0;
  virtual bool MarkObjectAsUnused(const ray::ObjectID &object_id) = 0;
};

/// Contains all information that is associated with a Plasma store client.
class Client : public ray::ClientConnection, public ClientInterface {
 public:
  static std::shared_ptr<Client> Create(PlasmaStoreMessageHandler message_handler,
                                        ray::local_stream_socket &&socket);

  ray::Status SendFd(MEMFD_TYPE fd) override;

  const std::unordered_set<ray::ObjectID> &GetObjectIDs() override { return object_ids; }

  // Holds the object ID. If the object ID has a fallback-allocated fd, adds the ref count
  // to that fd. Note: used_fds_ is not updated. rather, it's updated in SendFd().
  //
  // Idempotency: only increments ref count if the object ID was not held.
  // TODO(ryw): if this method is called with same object_id but different fd, the latter
  // fd is ignored. Is this possible? Is it OK?
  virtual void MarkObjectAsUsed(
      const ray::ObjectID &object_id,
      std::optional<MEMFD_TYPE> fallback_allocated_fd) override {
    const auto [_, inserted] = object_ids.insert(object_id);
    if (fallback_allocated_fd.has_value() && inserted) {
      MEMFD_TYPE fd = fallback_allocated_fd.value();
      object_ids_to_fds_[object_id] = fd;
      fds_ref_count_[fd] += 1;
    }
  }

  // Removes object ID's ref to the fd, if any. If the fd's refcnt goes to 0, also remove
  // the fd from used_fds_ and return true, directing the client to unmap it.
  //
  // Returns: bool, client should unmap.
  // Idempotency: only decrements ref count if the object ID was held.
  virtual bool MarkObjectAsUnused(const ray::ObjectID &object_id) override {
    size_t erased = object_ids.erase(object_id);
    if (erased == 0) {
      return false;
    }
    auto iter = object_ids_to_fds_.find(object_id);
    if (iter == object_ids_to_fds_.end()) {
      return false;
    }
    MEMFD_TYPE fd = iter->second;
    object_ids_to_fds_.erase(iter);

    size_t &ref_count = fds_ref_count_[iter->second];
    ref_count -= 1;
    if (ref_count == 0) {
      fds_ref_count_.erase(fd);
      used_fds_.erase(fd);  // Next SendFd call will send this fd again.
      return true;
    }
    return false;
  }

  std::string name = "anonymous_client";

 private:
  Client(ray::MessageHandler &message_handler, ray::local_stream_socket &&socket);
  /// File descriptors that are used by this client.
  /// TODO(ekl) we should also clean up old fds that are removed.
  absl::flat_hash_set<MEMFD_TYPE> used_fds_;

  /// Object ids that are used by this client.
  /// TODO: remove this as we can always count on object_ids_to_fds_.
  std::unordered_set<ray::ObjectID> object_ids;

  // Records each fd sent to the client and which object IDs are in this fd.
  // Only tracks fallback-allocated fds. This means the main memory is not tracked, and we
  // won't tell client to unmap the main memory. Incremented by `Get`, Decremented by
  // `Release`. If an FD is emptied out, the fd can be unmapped on the client side.
  absl::flat_hash_map<MEMFD_TYPE, size_t> fallback_allocated_fds_ref_count_;
  absl::flat_hash_map<ray::ObjectID, MEMFD_TYPE> object_ids_to_fds_;
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
