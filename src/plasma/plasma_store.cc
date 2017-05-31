// PLASMA STORE: This is a simple object store server process
//
// It accepts incoming client connections on a unix domain socket
// (name passed in via the -s option of the executable) and uses a
// single thread to serve the clients. Each client establishes a
// connection and can create objects, wait for objects and seal
// objects through that connection.
//
// It keeps a hash table that maps object_ids (which are 20 byte long,
// just enough to store and SHA1 hash) to memory mapped files.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/un.h>
#include <getopt.h>
#include <string.h>
#include <signal.h>
#include <limits.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "plasma_common.h"
#include "plasma_store.h"
#include "format/common_generated.h"
#include "plasma_io.h"
#include "malloc.h"

extern "C" {
#include "fling.h"
void *dlmalloc(size_t);
void *dlmemalign(size_t alignment, size_t bytes);
void dlfree(void *);
size_t dlmalloc_set_footprint_limit(size_t bytes);
}

struct GetRequest {
  GetRequest(Client *client, const std::vector<ObjectID> &object_ids);

  /// The client that called get.
  Client *client;
  /// The ID of the timer that will time out and cause this wait to return to
  ///  the client if it hasn't already returned.
  int64_t timer;
  /// The object IDs involved in this request. This is used in the reply.
  std::vector<ObjectID> object_ids;
  /// The object information for the objects in this request. This is used in
  /// the reply.
  std::unordered_map<ObjectID, PlasmaObject, UniqueIDHasher> objects;
  /// The minimum number of objects to wait for in this request.
  int64_t num_objects_to_wait_for;
  /// The number of object requests in this wait request that are already
  /// satisfied.
  int64_t num_satisfied;
};

GetRequest::GetRequest(Client *client, const std::vector<ObjectID> &object_ids)
    : client(client),
      timer(-1),
      object_ids(object_ids.begin(), object_ids.end()),
      objects(object_ids.size()),
      num_satisfied(0) {
  std::unordered_set<ObjectID, UniqueIDHasher> unique_ids(object_ids.begin(),
                                                          object_ids.end());
  num_objects_to_wait_for = unique_ids.size();
}

Client::Client(int fd) : fd(fd) {}

PlasmaStore::PlasmaStore(EventLoop *loop, int64_t system_memory)
    : loop_(loop), eviction_policy_(&store_info_) {
  store_info_.memory_capacity = system_memory;
}

PlasmaStore::~PlasmaStore() {
  for (const auto &element : pending_notifications_) {
    auto object_notifications = element.second.object_notifications;
    for (int i = 0; i < object_notifications.size(); ++i) {
      uint8_t *notification = (uint8_t *) object_notifications.at(i);
      uint8_t *data = notification;
      free(data);
    }
  }
}

// If this client is not already using the object, add the client to the
// object's list of clients, otherwise do nothing.
void PlasmaStore::add_client_to_object_clients(ObjectTableEntry *entry,
                                               Client *client) {
  // Check if this client is already using the object.
  if (entry->clients.find(client) != entry->clients.end()) {
    return;
  }
  // If there are no other clients using this object, notify the eviction policy
  // that the object is being used.
  if (entry->clients.size() == 0) {
    // Tell the eviction policy that this object is being used.
    std::vector<ObjectID> objects_to_evict;
    eviction_policy_.begin_object_access(entry->object_id, objects_to_evict);
    delete_objects(objects_to_evict);
  }
  // Add the client pointer to the list of clients using this object.
  entry->clients.insert(client);
}

// Create a new object buffer in the hash table.
int PlasmaStore::create_object(ObjectID object_id,
                               int64_t data_size,
                               int64_t metadata_size,
                               Client *client,
                               PlasmaObject *result) {
  ARROW_LOG(DEBUG) << "creating object " << object_id.hex();
  if (store_info_.objects.count(object_id) != 0) {
    // There is already an object with the same ID in the Plasma Store, so
    // ignore this requst.
    return PlasmaError_ObjectExists;
  }
  // Try to evict objects until there is enough space.
  uint8_t *pointer;
  do {
    // Allocate space for the new object. We use dlmemalign instead of dlmalloc
    // in order to align the allocated region to a 64-byte boundary. This is not
    // strictly necessary, but it is an optimization that could speed up the
    // computation of a hash of the data (see compute_object_hash_parallel in
    // plasma_client.cc). Note that even though this pointer is 64-byte aligned,
    // it is not guaranteed that the corresponding pointer in the client will be
    // 64-byte aligned, but in practice it often will be.
    pointer = (uint8_t *) dlmemalign(BLOCK_SIZE, data_size + metadata_size);
    if (pointer == NULL) {
      // Tell the eviction policy how much space we need to create this object.
      std::vector<ObjectID> objects_to_evict;
      bool success = eviction_policy_.require_space(data_size + metadata_size,
                                                    objects_to_evict);
      delete_objects(objects_to_evict);
      // Return an error to the client if not enough space could be freed to
      // create the object.
      if (!success) {
        return PlasmaError_OutOfMemory;
      }
    }
  } while (pointer == NULL);
  int fd;
  int64_t map_size;
  ptrdiff_t offset;
  get_malloc_mapinfo(pointer, &fd, &map_size, &offset);
  assert(fd != -1);

  auto entry = std::unique_ptr<ObjectTableEntry>(new ObjectTableEntry());
  entry->object_id = object_id;
  entry->info.object_id = object_id.binary();
  entry->info.data_size = data_size;
  entry->info.metadata_size = metadata_size;
  entry->pointer = pointer;
  // TODO(pcm): Set the other fields.
  entry->fd = fd;
  entry->map_size = map_size;
  entry->offset = offset;
  entry->state = PLASMA_CREATED;

  store_info_.objects[object_id] = std::move(entry);
  result->handle.store_fd = fd;
  result->handle.mmap_size = map_size;
  result->data_offset = offset;
  result->metadata_offset = offset + data_size;
  result->data_size = data_size;
  result->metadata_size = metadata_size;
  // Notify the eviction policy that this object was created. This must be done
  // immediately before the call to add_client_to_object_clients so that the
  // eviction policy does not have an opportunity to evict the object.
  eviction_policy_.object_created(object_id);
  // Record that this client is using this object.
  add_client_to_object_clients(store_info_.objects[object_id].get(), client);
  return PlasmaError_OK;
}

void PlasmaObject_init(PlasmaObject *object, ObjectTableEntry *entry) {
  DCHECK(object != NULL);
  DCHECK(entry != NULL);
  DCHECK(entry->state == PLASMA_SEALED);
  object->handle.store_fd = entry->fd;
  object->handle.mmap_size = entry->map_size;
  object->data_offset = entry->offset;
  object->metadata_offset = entry->offset + entry->info.data_size;
  object->data_size = entry->info.data_size;
  object->metadata_size = entry->info.metadata_size;
}

void PlasmaStore::return_from_get(GetRequest *get_req) {
  // Send the get reply to the client.
  Status s = SendGetReply(get_req->client->fd, &get_req->object_ids[0],
                          get_req->objects, get_req->object_ids.size());
  warn_if_sigpipe(s.ok() ? 0 : -1, get_req->client->fd);
  // If we successfully sent the get reply message to the client, then also send
  // the file descriptors.
  if (s.ok()) {
    // Send all of the file descriptors for the present objects.
    for (const auto &object_id : get_req->object_ids) {
      PlasmaObject &object = get_req->objects[object_id];
      // We use the data size to indicate whether the object is present or not.
      if (object.data_size != -1) {
        int error_code = send_fd(get_req->client->fd, object.handle.store_fd);
        // If we failed to send the file descriptor, loop until we have sent it
        // successfully. TODO(rkn): This is problematic for two reasons. First
        // of all, sending the file descriptor should just succeed without any
        // errors, but sometimes I see a "Message too long" error number.
        // Second, looping like this allows a client to potentially block the
        // plasma store event loop which should never happen.
        while (error_code < 0) {
          if (errno == EMSGSIZE) {
            ARROW_LOG(WARNING) << "Failed to send file descriptor, retrying.";
            error_code = send_fd(get_req->client->fd, object.handle.store_fd);
            continue;
          }
          warn_if_sigpipe(error_code, get_req->client->fd);
          break;
        }
      }
    }
  }

  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out.
  for (ObjectID &object_id : get_req->object_ids) {
    auto &get_requests = object_get_requests_[object_id];
    // Erase get_req from the vector.
    auto it = std::find(get_requests.begin(), get_requests.end(), get_req);
    if (it != get_requests.end()) {
      get_requests.erase(it);
    }
  }
  // Remove the get request.
  if (get_req->timer != -1) {
    ARROW_CHECK(loop_->remove_timer(get_req->timer) == AE_OK);
  }
  delete get_req;
}

void PlasmaStore::update_object_get_requests(ObjectID object_id) {
  std::vector<GetRequest *> &get_requests = object_get_requests_[object_id];
  int index = 0;
  int num_requests = get_requests.size();
  for (int i = 0; i < num_requests; ++i) {
    GetRequest *get_req = get_requests[index];
    auto entry = get_object_table_entry(&store_info_, object_id);
    ARROW_CHECK(entry != NULL);

    PlasmaObject_init(&get_req->objects[object_id], entry);
    get_req->num_satisfied += 1;
    // Record the fact that this client will be using this object and will
    // be responsible for releasing this object.
    add_client_to_object_clients(entry, get_req->client);

    // If this get request is done, reply to the client.
    if (get_req->num_satisfied == get_req->num_objects_to_wait_for) {
      return_from_get(get_req);
    } else {
      // The call to return_from_get will remove the current element in the
      // array, so we only increment the counter in the else branch.
      index += 1;
    }
  }

  DCHECK(index == get_requests.size());
  // Remove the array of get requests for this object, since no one should be
  // waiting for this object anymore.
  object_get_requests_.erase(object_id);
}

void PlasmaStore::process_get_request(Client *client,
                                      const std::vector<ObjectID> &object_ids,
                                      uint64_t timeout_ms) {
  // Create a get request for this object.
  GetRequest *get_req = new GetRequest(client, object_ids);

  for (auto object_id : object_ids) {
    // Check if this object is already present locally. If so, record that the
    // object is being used and mark it as accounted for.
    auto entry = get_object_table_entry(&store_info_, object_id);
    if (entry && entry->state == PLASMA_SEALED) {
      // Update the get request to take into account the present object.
      PlasmaObject_init(&get_req->objects[object_id], entry);
      get_req->num_satisfied += 1;
      // If necessary, record that this client is using this object. In the case
      // where entry == NULL, this will be called from seal_object.
      add_client_to_object_clients(entry, client);
    } else {
      // Add a placeholder plasma object to the get request to indicate that the
      // object is not present. This will be parsed by the client. We set the
      // data size to -1 to indicate that the object is not present.
      get_req->objects[object_id].data_size = -1;
      // Add the get request to the relevant data structures.
      object_get_requests_[object_id].push_back(get_req);
    }
  }

  // If all of the objects are present already or if the timeout is 0, return to
  // the client.
  if (get_req->num_satisfied == get_req->num_objects_to_wait_for ||
      timeout_ms == 0) {
    return_from_get(get_req);
  } else if (timeout_ms != -1) {
    // Set a timer that will cause the get request to return to the client. Note
    // that a timeout of -1 is used to indicate that no timer should be set.
    get_req->timer =
        loop_->add_timer(timeout_ms, [this, get_req](int64_t timer_id) {
          return_from_get(get_req);
          return kEventLoopTimerDone;
        });
  }
}

int PlasmaStore::remove_client_from_object_clients(ObjectTableEntry *entry,
                                                   Client *client) {
  auto it = entry->clients.find(client);
  if (it != entry->clients.end()) {
    entry->clients.erase(it);
    // If no more clients are using this object, notify the eviction policy
    // that the object is no longer being used.
    if (entry->clients.size() == 0) {
      // Tell the eviction policy that this object is no longer being used.
      std::vector<ObjectID> objects_to_evict;
      eviction_policy_.end_object_access(entry->object_id, objects_to_evict);
      delete_objects(objects_to_evict);
    }
    // Return 1 to indicate that the client was removed.
    return 1;
  } else {
    // Return 0 to indicate that the client was not removed.
    return 0;
  }
}

void PlasmaStore::release_object(ObjectID object_id, Client *client) {
  auto entry = get_object_table_entry(&store_info_, object_id);
  ARROW_CHECK(entry != NULL);
  // Remove the client from the object's array of clients.
  ARROW_CHECK(remove_client_from_object_clients(entry, client) == 1);
}

// Check if an object is present.
int PlasmaStore::contains_object(ObjectID object_id) {
  auto entry = get_object_table_entry(&store_info_, object_id);
  return entry && (entry->state == PLASMA_SEALED) ? OBJECT_FOUND
                                                  : OBJECT_NOT_FOUND;
}

// Seal an object that has been created in the hash table.
void PlasmaStore::seal_object(ObjectID object_id, unsigned char digest[]) {
  ARROW_LOG(DEBUG) << "sealing object " << object_id.hex();
  auto entry = get_object_table_entry(&store_info_, object_id);
  ARROW_CHECK(entry != NULL);
  ARROW_CHECK(entry->state == PLASMA_CREATED);
  // Set the state of object to SEALED.
  entry->state = PLASMA_SEALED;
  // Set the object digest.
  entry->info.digest = std::string((char *) &digest[0], kDigestSize);
  // Inform all subscribers that a new object has been sealed.
  push_notification(&entry->info);

  // Update all get requests that involve this object.
  update_object_get_requests(object_id);
}

void PlasmaStore::delete_objects(const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    ARROW_LOG(DEBUG) << "deleting object " << object_id.hex();
    auto entry = get_object_table_entry(&store_info_, object_id);
    // TODO(rkn): This should probably not fail, but should instead throw an
    // error. Maybe we should also support deleting objects that have been
    // created but not sealed.
    ARROW_CHECK(entry != NULL)
        << "To delete an object it must be in the object table.";
    ARROW_CHECK(entry->state == PLASMA_SEALED)
        << "To delete an object it must have been sealed.";
    ARROW_CHECK(entry->clients.size() == 0)
        << "To delete an object, there must be no clients currently using it.";
    dlfree(entry->pointer);
    store_info_.objects.erase(object_id);
    // Inform all subscribers that the object has been deleted.
    ObjectInfoT notification;
    notification.object_id = object_id.binary();
    notification.is_deletion = true;
    push_notification(&notification);
  }
}

void PlasmaStore::connect_client(int listener_sock) {
  int client_fd = AcceptClient(listener_sock);
  // This is freed in disconnect_client.
  Client *client = new Client(client_fd);
  // Add a callback to handle events on this socket.
  // TODO(pcm): Check return value.
  loop_->add_file_event(client_fd, kEventLoopRead, [this, client](int events) {
    process_message(client);
  });
  ARROW_LOG(DEBUG) << "New connection with fd " << client_fd;
}

void PlasmaStore::disconnect_client(Client *client) {
  loop_->remove_file_event(client->fd);
  // Close the socket.
  close(client->fd);
  ARROW_LOG(INFO) << "Disconnecting client on fd " << client->fd;
  // If this client was using any objects, remove it from the appropriate
  // lists.
  for (const auto &entry : store_info_.objects) {
    remove_client_from_object_clients(entry.second.get(), client);
  }
  // Note, the store may still attempt to send a message to the disconnected
  // client (for example, when an object ID that the client was waiting for
  // is ready). In these cases, the attempt to send the message will fail, but
  // the store should just ignore the failure.
  delete client;
}

/// Send notifications about sealed objects to the subscribers. This is called
/// in seal_object. If the socket's send buffer is full, the notification will
/// be
/// buffered, and this will be called again when the send buffer has room.
///
/// @param client The client to send the notification to.
/// @return Void.
void PlasmaStore::send_notifications(int client_fd) {
  auto it = pending_notifications_.find(client_fd);

  int num_processed = 0;
  bool closed = false;
  // Loop over the array of pending notifications and send as many of them as
  // possible.
  for (int i = 0; i < it->second.object_notifications.size(); ++i) {
    uint8_t *notification = (uint8_t *) it->second.object_notifications.at(i);
    // Decode the length, which is the first bytes of the message.
    int64_t size = *((int64_t *) notification);

    // Attempt to send a notification about this object ID.
    int nbytes = send(client_fd, notification, sizeof(int64_t) + size, 0);
    if (nbytes >= 0) {
      ARROW_CHECK(nbytes == sizeof(int64_t) + size);
    } else if (nbytes == -1 &&
               (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      ARROW_LOG(DEBUG)
          << "The socket's send buffer is full, so we are caching this "
             "notification and will send it later.";
      // Add a callback to the event loop to send queued notifications whenever
      // there is room in the socket's send buffer. Callbacks can be added
      // more than once here and will be overwritten. The callback is removed
      // at the end of the method.
      // TODO(pcm): Introduce status codes and check in case the file descriptor
      // is added twice.
      loop_->add_file_event(
          client_fd, kEventLoopWrite,
          [this, client_fd](int events) { send_notifications(client_fd); });
      break;
    } else {
      ARROW_LOG(WARNING) << "Failed to send notification to client on fd "
                         << client_fd;
      if (errno == EPIPE) {
        closed = true;
        break;
      }
    }
    num_processed += 1;
    // The corresponding malloc happened in create_object_info_buffer
    // within push_notification.
    free(notification);
  }
  // Remove the sent notifications from the array.
  it->second.object_notifications.erase(
      it->second.object_notifications.begin(),
      it->second.object_notifications.begin() + num_processed);

  // Stop sending notifications if the pipe was broken.
  if (closed) {
    close(client_fd);
    pending_notifications_.erase(client_fd);
  }

  // If we have sent all notifications, remove the fd from the event loop.
  if (it->second.object_notifications.empty()) {
    loop_->remove_file_event(client_fd);
  }
}

void PlasmaStore::push_notification(ObjectInfoT *object_info) {
  for (auto &element : pending_notifications_) {
    uint8_t *notification = create_object_info_buffer(object_info);
    element.second.object_notifications.push_back(notification);
    send_notifications(element.first);
    // The notification gets freed in send_notifications when the notification
    // is sent over the socket.
  }
}

// Subscribe to notifications about sealed objects.
void PlasmaStore::subscribe_to_updates(Client *client) {
  ARROW_LOG(DEBUG) << "subscribing to updates on fd " << client->fd;
  // TODO(rkn): The store could block here if the client doesn't send a file
  // descriptor.
  int fd = recv_fd(client->fd);
  if (fd < 0) {
    // This may mean that the client died before sending the file descriptor.
    ARROW_LOG(WARNING) << "Failed to receive file descriptor from client on fd "
                       << client->fd << ".";
    return;
  }

  // Create a new array to buffer notifications that can't be sent to the
  // subscriber yet because the socket send buffer is full. TODO(rkn): the queue
  // never gets freed.
  NotificationQueue &queue = pending_notifications_[fd];

  // Push notifications to the new subscriber about existing objects.
  for (const auto &entry : store_info_.objects) {
    push_notification(&entry.second->info);
  }
  send_notifications(fd);
}

Status PlasmaStore::process_message(Client *client) {
  int64_t type;
  Status s = ReadMessage(client->fd, &type, input_buffer_);
  ARROW_CHECK(s.ok() || s.IsIOError());

  uint8_t *input = input_buffer_.data();
  ObjectID object_id;
  PlasmaObject object;
  // TODO(pcm): Get rid of the following.
  memset(&object, 0, sizeof(object));

  // Process the different types of requests.
  switch (type) {
  case MessageType_PlasmaCreateRequest: {
    int64_t data_size;
    int64_t metadata_size;
    RETURN_NOT_OK(
        ReadCreateRequest(input, &object_id, &data_size, &metadata_size));
    int error_code =
        create_object(object_id, data_size, metadata_size, client, &object);
    HANDLE_SIGPIPE(SendCreateReply(client->fd, object_id, &object, error_code),
                   client->fd);
    if (error_code == PlasmaError_OK) {
      warn_if_sigpipe(send_fd(client->fd, object.handle.store_fd), client->fd);
    }
  } break;
  case MessageType_PlasmaGetRequest: {
    std::vector<ObjectID> object_ids_to_get;
    int64_t timeout_ms;
    RETURN_NOT_OK(ReadGetRequest(input, object_ids_to_get, &timeout_ms));
    process_get_request(client, object_ids_to_get, timeout_ms);
  } break;
  case MessageType_PlasmaReleaseRequest:
    RETURN_NOT_OK(ReadReleaseRequest(input, &object_id));
    release_object(object_id, client);
    break;
  case MessageType_PlasmaContainsRequest:
    RETURN_NOT_OK(ReadContainsRequest(input, &object_id));
    if (contains_object(object_id) == OBJECT_FOUND) {
      HANDLE_SIGPIPE(SendContainsReply(client->fd, object_id, 1), client->fd);
    } else {
      HANDLE_SIGPIPE(SendContainsReply(client->fd, object_id, 0), client->fd);
    }
    break;
  case MessageType_PlasmaSealRequest: {
    unsigned char digest[kDigestSize];
    RETURN_NOT_OK(ReadSealRequest(input, &object_id, &digest[0]));
    seal_object(object_id, &digest[0]);
  } break;
  case MessageType_PlasmaEvictRequest: {
    // This code path should only be used for testing.
    int64_t num_bytes;
    RETURN_NOT_OK(ReadEvictRequest(input, &num_bytes));
    std::vector<ObjectID> objects_to_evict;
    int64_t num_bytes_evicted =
        eviction_policy_.choose_objects_to_evict(num_bytes, objects_to_evict);
    delete_objects(objects_to_evict);
    HANDLE_SIGPIPE(SendEvictReply(client->fd, num_bytes_evicted), client->fd);
  } break;
  case MessageType_PlasmaSubscribeRequest:
    subscribe_to_updates(client);
    break;
  case MessageType_PlasmaConnectRequest: {
    HANDLE_SIGPIPE(SendConnectReply(client->fd, store_info_.memory_capacity),
                   client->fd);
  } break;
  case DISCONNECT_CLIENT:
    ARROW_LOG(DEBUG) << "Disconnecting client on fd " << client->fd;
    disconnect_client(client);
    break;
  default:
    // This code should be unreachable.
    ARROW_CHECK(0);
  }
  return Status::OK();
}

// Report "success" to valgrind.
void signal_handler(int signal) {
  if (signal == SIGTERM) {
    exit(0);
  }
}

void start_server(char *socket_name, int64_t system_memory) {
  // Ignore SIGPIPE signals. If we don't do this, then when we attempt to write
  // to a client that has already died, the store could die.
  signal(SIGPIPE, SIG_IGN);
  // Create the event loop.
  EventLoop loop;
  PlasmaStore store(&loop, system_memory);
  int socket = bind_ipc_sock(socket_name, true);
  ARROW_CHECK(socket >= 0);
  // TODO(pcm): Check return value.
  loop.add_file_event(socket, kEventLoopRead, [&store, socket](int events) {
    store.connect_client(socket);
  });
  loop.run();
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
  char *socket_name = NULL;
  int64_t system_memory = -1;
  int c;
  while ((c = getopt(argc, argv, "s:m:")) != -1) {
    switch (c) {
    case 's':
      socket_name = optarg;
      break;
    case 'm': {
      char extra;
      int scanned = sscanf(optarg, "%" SCNd64 "%c", &system_memory, &extra);
      ARROW_CHECK(scanned == 1);
      ARROW_LOG(INFO) << "Allowing the Plasma store to use up to "
                      << ((double) system_memory) / 1000000000
                      << "GB of memory.";
      break;
    }
    default:
      exit(-1);
    }
  }
  if (!socket_name) {
    ARROW_LOG(FATAL)
        << "please specify socket for incoming connections with -s switch";
  }
  if (system_memory == -1) {
    ARROW_LOG(FATAL)
        << "please specify the amount of system memory with -m switch";
  }
#ifdef __linux__
  // On Linux, check that the amount of memory available in /dev/shm is large
  // enough to accommodate the request. If it isn't, then fail.
  int shm_fd = open("/dev/shm", O_RDONLY);
  struct statvfs shm_vfs_stats;
  fstatvfs(shm_fd, &shm_vfs_stats);
  // The value shm_vfs_stats.f_bsize is the block size, and the value
  // shm_vfs_stats.f_bavail is the number of available blocks.
  int64_t shm_mem_avail = shm_vfs_stats.f_bsize * shm_vfs_stats.f_bavail;
  close(shm_fd);
  if (system_memory > shm_mem_avail) {
    ARROW_LOG(FATAL)
        << "System memory request exceeds memory available in /dev/shm. The "
           "request is for "
        << system_memory << " bytes, and the amount available is "
        << shm_mem_avail
        << " bytes. You may be able to free up space by deleting files in "
           "/dev/shm. If you are inside a Docker container, you may need to "
           "pass "
           "an argument with the flag '--shm-size' to 'docker run'.";
  }
#endif
  // Make it so dlmalloc fails if we try to request more memory than is
  // available.
  dlmalloc_set_footprint_limit((size_t) system_memory);
  ARROW_LOG(DEBUG) << "starting server listening on " << socket_name;
  start_server(socket_name, system_memory);
}
