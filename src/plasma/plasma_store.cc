/* PLASMA STORE: This is a simple object store server process
 *
 * It accepts incoming client connections on a unix domain socket
 * (name passed in via the -s option of the executable) and uses a
 * single thread to serve the clients. Each client establishes a
 * connection and can create objects, wait for objects and seal
 * objects through that connection.
 *
 * It keeps a hash table that maps object_ids (which are 20 byte long,
 * just enough to store and SHA1 hash) to memory mapped files. */

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
#include <poll.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "format/common_generated.h"
#include "event_loop.h"
#include "eviction_policy.h"
#include "io.h"
#include "plasma_protocol.h"
#include "plasma_store.h"
#include "plasma.h"

extern "C" {
#include "fling.h"
#include "malloc.h"
void *dlmalloc(size_t);
void *dlmemalign(size_t alignment, size_t bytes);
void dlfree(void *);
size_t dlmalloc_set_footprint_limit(size_t bytes);
}

/** Contains all information that is associated with a Plasma store client. */
struct Client {
  Client(int sock, PlasmaStore *plasma_state);

  /** The socket used to communicate with the client. */
  int sock;
  /** A pointer to the global plasma state. */
  PlasmaStore *plasma_state;
};

struct NotificationQueue {
  /** The object notifications for clients. We notify the client about the
   *  objects in the order that the objects were sealed or deleted. */
  std::deque<uint8_t *> object_notifications;
};

struct GetRequest {
  GetRequest(Client *client, int num_object_ids, ObjectID object_ids[]);

  /** The client connection that called get. */
  Client *client;
  /** The ID of the timer that will time out and cause this wait to return to
   *  the client if it hasn't already returned. */
  int64_t timer;
  /** The object IDs involved in this request. This is used in the reply. */
  std::vector<ObjectID> object_ids;
  /** The object information for the objects in this request. This is used in
   *  the reply. */
  std::unordered_map<ObjectID, PlasmaObject, UniqueIDHasher> objects;
  /** The minimum number of objects to wait for in this request. */
  int64_t num_objects_to_wait_for;
  /** The number of object requests in this wait request that are already
   *  satisfied. */
  int64_t num_satisfied;
};

struct PlasmaStore {
  PlasmaStore(event_loop *loop, int64_t system_memory);

  ~PlasmaStore();

  // private:
  void push_notification(ObjectInfoT *object_notification);

  void add_client_to_object_clients(std::shared_ptr<ObjectTableEntry> entry, Client *client);
  
  void return_from_get(GetRequest *get_req);
  
  void update_object_get_requests(ObjectID object_id);

  int remove_client_from_object_clients(std::shared_ptr<ObjectTableEntry> entry,
                                        Client *client);

  // public:
  void delete_object(ObjectID object_id);

  void remove_objects(const std::vector<ObjectID> &objects);

  int create_object(ObjectID object_id,
                  int64_t data_size,
                  int64_t metadata_size,
                  Client *client,
                  PlasmaObject *result);

  void seal_object(ObjectID object_id, unsigned char digest[]);
  
  int contains_object(ObjectID object_id);

  void release_object(ObjectID object_id, Client *client);

  /* Event loop of the plasma store. */
  event_loop *loop;
  /** A hash table mapping object IDs to a vector of the get requests that are
   *  waiting for the object to arrive. */
  std::unordered_map<ObjectID, std::vector<GetRequest *>, UniqueIDHasher>
      object_get_requests;

  /** The pending notifications that have not been sent to subscribers because
   *  the socket send buffers were full. This is a hash table from client file
   *  descriptor to an array of object_ids to send to that client.
   *  TODO(pcm): Consider putting this into the Client data structure and
   *  reorganize the code slightly. */
  std::unordered_map<int, NotificationQueue> pending_notifications;
  /** The plasma store information, including the object tables, that is exposed
   *  to the eviction policy. */
  std::unique_ptr<PlasmaStoreInfo> plasma_store_info;
  /** The state that is managed by the eviction policy. */
  std::unique_ptr<EvictionPolicy> eviction_policy;
  /** Input buffer. This is allocated only once to avoid mallocs for every
   *  call to process_message. */
  std::vector<uint8_t> input_buffer;
  /** Buffer that holds memory for serializing plasma protocol messages. */
  protocol_builder *builder;
  /** Clients connected to this store. */
  std::vector<std::unique_ptr<Client>> clients;
};

Client::Client(int sock, PlasmaStore *plasma_state)
    : sock(sock), plasma_state(plasma_state) {}

GetRequest::GetRequest(Client *client,
                       int num_object_ids,
                       ObjectID object_ids[])
    : client(client),
      timer(-1),
      object_ids(object_ids, object_ids + num_object_ids),
      objects(num_object_ids),
      num_satisfied(0) {
  std::unordered_set<ObjectID, UniqueIDHasher> unique_ids(
      object_ids, object_ids + num_object_ids);
  num_objects_to_wait_for = unique_ids.size();
}

PlasmaStore::PlasmaStore(event_loop *loop, int64_t system_memory)
    : loop(loop),
      plasma_store_info(new PlasmaStoreInfo()),
      eviction_policy(new EvictionPolicy(plasma_store_info.get())),
      builder(make_protocol_builder()) {
  this->plasma_store_info->memory_capacity = system_memory;
}

PlasmaStore::~PlasmaStore() {
  for (const auto &element : pending_notifications) {
    auto object_notifications = element.second.object_notifications;
    for (int i = 0; i < object_notifications.size(); ++i) {
      uint8_t *notification = (uint8_t *) object_notifications.at(i);
      uint8_t *data = notification;
      free(data);
    }
  }
  free_protocol_builder(builder);
}

/* If this client is not already using the object, add the client to the
 * object's list of clients, otherwise do nothing. */
void PlasmaStore::add_client_to_object_clients(std::shared_ptr<ObjectTableEntry> entry,
                                  Client *client) {
  /* Check if this client is already using the object. */
  for (int i = 0; i < entry->clients.size(); ++i) {
    if (entry->clients[i] == client) {
      return;
    }
  }
  /* If there are no other clients using this object, notify the eviction policy
   * that the object is being used. */
  if (entry->clients.size() == 0) {
    /* Tell the eviction policy that this object is being used. */
    std::vector<ObjectID> objects_to_evict;
    eviction_policy->begin_object_access(entry->object_id, objects_to_evict);
    remove_objects(objects_to_evict);
  }
  /* Add the client pointer to the list of clients using this object. */
  entry->clients.push_back(client);
}

/* Create a new object buffer in the hash table. */
int PlasmaStore::create_object(ObjectID object_id,
                  int64_t data_size,
                  int64_t metadata_size,
                  Client *client,
                  PlasmaObject *result) {
  LOG_DEBUG("creating object"); /* TODO(pcm): add ObjectID here */
  if (plasma_store_info->objects.count(object_id) != 0) {
    /* There is already an object with the same ID in the Plasma Store, so
     * ignore this requst. */
    return PlasmaError_ObjectExists;
  }
  /* Try to evict objects until there is enough space. */
  uint8_t *pointer;
  do {
    /* Allocate space for the new object. We use dlmemalign instead of dlmalloc
     * in order to align the allocated region to a 64-byte boundary. This is not
     * strictly necessary, but it is an optimization that could speed up the
     * computation of a hash of the data (see compute_object_hash_parallel in
     * plasma_client.cc). Note that even though this pointer is 64-byte aligned,
     * it is not guaranteed that the corresponding pointer in the client will be
     * 64-byte aligned, but in practice it often will be. */
    pointer = (uint8_t *) dlmemalign(BLOCK_SIZE, data_size + metadata_size);
    if (pointer == NULL) {
      /* Tell the eviction policy how much space we need to create this object.
       */
      std::vector<ObjectID> objects_to_evict;
      bool success = eviction_policy->require_space(
          data_size + metadata_size, objects_to_evict);
      remove_objects(objects_to_evict);
      /* Return an error to the client if not enough space could be freed to
       * create the object. */
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

  auto entry = std::make_shared<ObjectTableEntry>();
  entry->object_id = object_id;
  entry->info.object_id = std::string((char *) &object_id.id[0], sizeof(object_id));
  entry->info.data_size = data_size;
  entry->info.metadata_size = metadata_size;
  entry->pointer = pointer;
  /* TODO(pcm): set the other fields */
  entry->fd = fd;
  entry->map_size = map_size;
  entry->offset = offset;
  entry->state = PLASMA_CREATED;

  plasma_store_info->objects[object_id] = entry;
  result->handle.store_fd = fd;
  result->handle.mmap_size = map_size;
  result->data_offset = offset;
  result->metadata_offset = offset + data_size;
  result->data_size = data_size;
  result->metadata_size = metadata_size;
  /* Notify the eviction policy that this object was created. This must be done
   * immediately before the call to add_client_to_object_clients so that the
   * eviction policy does not have an opportunity to evict the object. */
  eviction_policy->object_created(object_id);
  /* Record that this client is using this object. */
  add_client_to_object_clients(entry, client);
  return PlasmaError_OK;
}

void PlasmaObject_init(PlasmaObject *object, std::shared_ptr<ObjectTableEntry> entry) {
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
  /* Send the get reply to the client. */
  int status = plasma_send_GetReply(get_req->client->sock, builder,
                                    &get_req->object_ids[0], get_req->objects,
                                    get_req->object_ids.size());
  warn_if_sigpipe(status, get_req->client->sock);
  /* If we successfully sent the get reply message to the client, then also send
   * the file descriptors. */
  if (status >= 0) {
    /* Send all of the file descriptors for the present objects. */
    for (const auto &object_id : get_req->object_ids) {
      PlasmaObject &object = get_req->objects[object_id];
      /* We use the data size to indicate whether the object is present or not.
       */
      if (object.data_size != -1) {
        int error_code = send_fd(get_req->client->sock, object.handle.store_fd);
        /* If we failed to send the file descriptor, loop until we have sent it
         * successfully. TODO(rkn): This is problematic for two reasons. First
         * of all, sending the file descriptor should just succeed without any
         * errors, but sometimes I see a "Message too long" error number.
         * Second, looping like this allows a client to potentially block the
         * plasma store event loop which should never happen. */
        while (error_code < 0) {
          if (errno == EMSGSIZE) {
            LOG_WARN("Failed to send file descriptor, retrying.");
            error_code = send_fd(get_req->client->sock, object.handle.store_fd);
            continue;
          }
          warn_if_sigpipe(error_code, get_req->client->sock);
          break;
        }
      }
    }
  }

  /* Remove the get request from each of the relevant object_get_requests hash
   * tables if it is present there. It should only be present there if the get
   * request timed out. */
  for (ObjectID &object_id : get_req->object_ids) {
    auto &get_requests = object_get_requests[object_id];
    get_requests.erase(std::remove(get_requests.begin(), get_requests.end(), get_req), get_requests.end());
  }
  /* Remove the get request. */
  if (get_req->timer != -1) {
    CHECK(event_loop_remove_timer(loop, get_req->timer) == AE_OK);
  }
  delete get_req;
}

void PlasmaStore::update_object_get_requests(ObjectID object_id) {
  std::vector<GetRequest *> &get_requests = object_get_requests[object_id];
  int index = 0;
  int num_requests = get_requests.size();
  for (int i = 0; i < num_requests; ++i) {
    GetRequest *get_req = get_requests[index];
    auto entry = get_object_table_entry(plasma_store_info.get(), object_id);
    CHECK(entry != NULL);

    PlasmaObject_init(&get_req->objects[object_id], entry);
    get_req->num_satisfied += 1;
    /* Record the fact that this client will be using this object and will
     * be responsible for releasing this object. */
    add_client_to_object_clients(entry, get_req->client);

    /* If this get request is done, reply to the client. */
    if (get_req->num_satisfied == get_req->num_objects_to_wait_for) {
      return_from_get(get_req);
    } else {
      /* The call to return_from_get will remove the current element in the
       * array, so we only increment the counter in the else branch. */
      index += 1;
    }
  }

  DCHECK(index == get_requests.size());
  /* Remove the array of get requests for this object, since no one should be
   * waiting for this object anymore. */
  object_get_requests.erase(object_id);
}

int get_timeout_handler(event_loop *loop, timer_id id, void *context) {
  GetRequest *get_req = (GetRequest *) context;
  get_req->client->plasma_state->return_from_get(get_req);
  return EVENT_LOOP_TIMER_DONE;
}

void process_get_request(Client *client_context,
                         int num_object_ids,
                         ObjectID object_ids[],
                         uint64_t timeout_ms) {
  PlasmaStore *plasma_state = client_context->plasma_state;

  /* Create a get request for this object. */
  GetRequest *get_req =
      new GetRequest(client_context, num_object_ids, object_ids);

  for (int i = 0; i < num_object_ids; ++i) {
    ObjectID obj_id = object_ids[i];

    /* Check if this object is already present locally. If so, record that the
     * object is being used and mark it as accounted for. */
    auto entry =
        get_object_table_entry(plasma_state->plasma_store_info.get(), obj_id);
    if (entry && entry->state == PLASMA_SEALED) {
      /* Update the get request to take into account the present object. */
      PlasmaObject_init(&get_req->objects[obj_id], entry);
      get_req->num_satisfied += 1;
      /* If necessary, record that this client is using this object. In the case
       * where entry == NULL, this will be called from seal_object. */
      plasma_state->add_client_to_object_clients(entry, client_context);
    } else {
      /* Add a placeholder plasma object to the get request to indicate that the
       * object is not present. This will be parsed by the client. We set the
       * data size to -1 to indicate that the object is not present. */
      get_req->objects[obj_id].data_size = -1;
      /* Add the get request to the relevant data structures. */
      plasma_state->object_get_requests[obj_id].push_back(get_req);
    }
  }

  /* If all of the objects are present already or if the timeout is 0, return to
   * the client. */
  if (get_req->num_satisfied == get_req->num_objects_to_wait_for ||
      timeout_ms == 0) {
    plasma_state->return_from_get(get_req);
  } else if (timeout_ms != -1) {
    /* Set a timer that will cause the get request to return to the client. Note
     * that a timeout of -1 is used to indicate that no timer should be set. */
    get_req->timer = event_loop_add_timer(plasma_state->loop, timeout_ms,
                                          get_timeout_handler, get_req);
  }
}

int PlasmaStore::remove_client_from_object_clients(std::shared_ptr<ObjectTableEntry> entry,
                                      Client *client) {
  /* Find the location of the client in the array. */
  for (int i = 0; i < entry->clients.size(); ++i) {
    if (entry->clients[i] == client) {
      /* Remove the client from the array. */
      entry->clients.erase(entry->clients.begin() + i);
      /* If no more clients are using this object, notify the eviction policy
       * that the object is no longer being used. */
      if (entry->clients.size() == 0) {
        /* Tell the eviction policy that this object is no longer being used. */
        std::vector<ObjectID> objects_to_evict;
        eviction_policy->end_object_access(entry->object_id, objects_to_evict);
        remove_objects(objects_to_evict);
      }
      /* Return 1 to indicate that the client was removed. */
      return 1;
    }
  }
  /* Return 0 to indicate that the client was not removed. */
  return 0;
}

void PlasmaStore::release_object(ObjectID object_id, Client *client) {
  auto entry = get_object_table_entry(plasma_store_info.get(), object_id);
  CHECK(entry != NULL);
  /* Remove the client from the object's array of clients. */
  CHECK(remove_client_from_object_clients(entry, client) == 1);
}

/* Check if an object is present. */
int PlasmaStore::contains_object(ObjectID object_id) {
  auto entry = get_object_table_entry(plasma_store_info.get(), object_id);
  return entry && (entry->state == PLASMA_SEALED) ? OBJECT_FOUND
                                                  : OBJECT_NOT_FOUND;
}

/* Seal an object that has been created in the hash table. */
void PlasmaStore::seal_object(ObjectID object_id,
                              unsigned char digest[]) {
  LOG_DEBUG("sealing object");  // TODO(pcm): add ObjectID here
  auto entry = get_object_table_entry(plasma_store_info.get(), object_id);
  CHECK(entry != NULL);
  CHECK(entry->state == PLASMA_CREATED);
  /* Set the state of object to SEALED. */
  entry->state = PLASMA_SEALED;
  /* Set the object digest. */
  entry->info.digest = std::string((char *) &digest[0], DIGEST_SIZE);
  /* Inform all subscribers that a new object has been sealed. */
  push_notification(&entry->info);

  /* Update all get requests that involve this object. */
  update_object_get_requests(object_id);
}

/* Delete an object that has been created in the hash table. This should only
 * be called on objects that are returned by the eviction policy to evict. */
void PlasmaStore::delete_object(ObjectID object_id) {
  LOG_DEBUG("deleting object");
  auto entry = get_object_table_entry(plasma_store_info.get(), object_id);
  /* TODO(rkn): This should probably not fail, but should instead throw an
   * error. Maybe we should also support deleting objects that have been created
   * but not sealed. */
  CHECKM(entry != NULL, "To delete an object it must be in the object table.");
  CHECKM(entry->state == PLASMA_SEALED,
         "To delete an object it must have been sealed.");
  CHECKM(entry->clients.size() == 0,
         "To delete an object, there must be no clients currently using it.");
  dlfree(entry->pointer);
  plasma_store_info->objects.erase(object_id);
  /* Inform all subscribers that the object has been deleted. */
  ObjectInfoT notification;
  notification.object_id =
      std::string((char *) &object_id.id[0], sizeof(object_id));
  notification.is_deletion = true;
  push_notification(&notification);
}

void PlasmaStore::remove_objects(const std::vector<ObjectID> &objects) {
  for (const auto &object_id : objects) {
    delete_object(object_id);
  }
}

void PlasmaStore::push_notification(ObjectInfoT *object_info) {
  for (auto &element : pending_notifications) {
    uint8_t *notification = create_object_info_buffer(object_info);
    element.second.object_notifications.push_back(notification);
    send_notifications(loop, element.first, this, 0);
    /* The notification gets freed in send_notifications when the notification
     * is sent over the socket. */
  }
}

/* Send more notifications to a subscriber. */
void send_notifications(event_loop *loop,
                        int client_sock,
                        void *context,
                        int events) {
  PlasmaStore *plasma_state = (PlasmaStore *) context;
  auto it = plasma_state->pending_notifications.find(client_sock);

  int num_processed = 0;
  bool closed = false;
  /* Loop over the array of pending notifications and send as many of them as
   * possible. */
  for (int i = 0; i < it->second.object_notifications.size(); ++i) {
    uint8_t *notification = (uint8_t *) it->second.object_notifications.at(i);
    /* Decode the length, which is the first bytes of the message. */
    int64_t size = *((int64_t *) notification);

    /* Attempt to send a notification about this object ID. */
    int nbytes = send(client_sock, notification, sizeof(int64_t) + size, 0);
    if (nbytes >= 0) {
      CHECK(nbytes == sizeof(int64_t) + size);
    } else if (nbytes == -1 &&
               (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      LOG_DEBUG(
          "The socket's send buffer is full, so we are caching this "
          "notification and will send it later.");
      /* Add a callback to the event loop to send queued notifications whenever
       * there is room in the socket's send buffer. Callbacks can be added
       * more than once here and will be overwritten. The callback is removed
       * at the end of the method. */
      event_loop_add_file(plasma_state->loop, client_sock, EVENT_LOOP_WRITE,
                          send_notifications, plasma_state);
      break;
    } else {
      LOG_WARN("Failed to send notification to client on fd %d", client_sock);
      if (errno == EPIPE) {
        closed = true;
        break;
      }
    }
    num_processed += 1;
    /* The corresponding malloc happened in create_object_info_buffer
     * within push_notification. */
    free(notification);
  }
  /* Remove the sent notifications from the array. */
  it->second.object_notifications.erase(
      it->second.object_notifications.begin(),
      it->second.object_notifications.begin() + num_processed);

  /* Stop sending notifications if the pipe was broken. */
  if (closed) {
    close(client_sock);
    plasma_state->pending_notifications.erase(client_sock);
  }

  /* If we have sent all notifications, remove the fd from the event loop. */
  if (it->second.object_notifications.empty()) {
    event_loop_remove_file(loop, client_sock);
  }
}

/* Subscribe to notifications about sealed objects. */
void subscribe_to_updates(Client *client_context, int conn) {
  LOG_DEBUG("subscribing to updates");
  PlasmaStore *plasma_state = client_context->plasma_state;
  /* TODO(rkn): The store could block here if the client doesn't send a file
   * descriptor. */
  int fd = recv_fd(conn);
  if (fd < 0) {
    /* This may mean that the client died before sending the file descriptor. */
    LOG_WARN("Failed to receive file descriptor from client on fd %d.", conn);
    return;
  }

  /* Create a new array to buffer notifications that can't be sent to the
   * subscriber yet because the socket send buffer is full. TODO(rkn): the queue
   * never gets freed. */
  NotificationQueue &queue = plasma_state->pending_notifications[fd];

  /* Push notifications to the new subscriber about existing objects. */
  for (const auto &entry : plasma_state->plasma_store_info->objects) {
    plasma_state->push_notification(&entry.second->info);
  }
  send_notifications(plasma_state->loop, fd, plasma_state, 0);
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  Client *client_context = (Client *) context;
  PlasmaStore *state = client_context->plasma_state;
  int64_t type;
  read_vector(client_sock, &type, state->input_buffer);

  uint8_t *input = state->input_buffer.data();
  ObjectID object_ids[1];
  int64_t num_objects;
  PlasmaObject objects[1];
  memset(&objects[0], 0, sizeof(objects));

  /* Process the different types of requests. */
  switch (type) {
  case MessageType_PlasmaCreateRequest: {
    int64_t data_size;
    int64_t metadata_size;
    plasma_read_CreateRequest(input, &object_ids[0], &data_size,
                              &metadata_size);
    int error_code = state->create_object(object_ids[0], data_size,
                                          metadata_size, client_context, &objects[0]);
    warn_if_sigpipe(
        plasma_send_CreateReply(client_sock, state->builder, object_ids[0],
                                &objects[0], error_code),
        client_sock);
    if (error_code == PlasmaError_OK) {
      warn_if_sigpipe(send_fd(client_sock, objects[0].handle.store_fd),
                      client_sock);
    }
  } break;
  case MessageType_PlasmaGetRequest: {
    num_objects = plasma_read_GetRequest_num_objects(input);
    std::vector<ObjectID> object_ids_to_get(num_objects);
    int64_t timeout_ms;
    plasma_read_GetRequest(input, object_ids_to_get.data(), &timeout_ms, num_objects);
    /* TODO(pcm): The array object_ids_to_get could be reused in
     * process_get_request. */
    process_get_request(client_context, num_objects, object_ids_to_get.data(),
                        timeout_ms);
  } break;
  case MessageType_PlasmaReleaseRequest:
    plasma_read_ReleaseRequest(input, &object_ids[0]);
    state->release_object(object_ids[0], client_context);
    break;
  case MessageType_PlasmaContainsRequest:
    plasma_read_ContainsRequest(input, &object_ids[0]);
    if (state->contains_object(object_ids[0]) == OBJECT_FOUND) {
      warn_if_sigpipe(plasma_send_ContainsReply(client_sock, state->builder,
                                                object_ids[0], 1),
                      client_sock);
    } else {
      warn_if_sigpipe(plasma_send_ContainsReply(client_sock, state->builder,
                                                object_ids[0], 0),
                      client_sock);
    }
    break;
  case MessageType_PlasmaSealRequest: {
    unsigned char digest[DIGEST_SIZE];
    plasma_read_SealRequest(input, &object_ids[0], &digest[0]);
    state->seal_object(object_ids[0], &digest[0]);
  } break;
  case MessageType_PlasmaEvictRequest: {
    /* This code path should only be used for testing. */
    int64_t num_bytes;
    plasma_read_EvictRequest(input, &num_bytes);
    std::vector<ObjectID> objects_to_evict;
    int64_t num_bytes_evicted = client_context->plasma_state->eviction_policy->choose_objects_to_evict(
        num_bytes, objects_to_evict);
    client_context->plasma_state->remove_objects(objects_to_evict);
    warn_if_sigpipe(
        plasma_send_EvictReply(client_sock, state->builder, num_bytes_evicted),
        client_sock);
  } break;
  case MessageType_PlasmaSubscribeRequest:
    subscribe_to_updates(client_context, client_sock);
    break;
  case MessageType_PlasmaConnectRequest: {
    warn_if_sigpipe(
        plasma_send_ConnectReply(client_sock, state->builder,
                                 state->plasma_store_info->memory_capacity),
        client_sock);
  } break;
  case DISCONNECT_CLIENT: {
    LOG_INFO("Disconnecting client on fd %d", client_sock);
    event_loop_remove_file(loop, client_sock);
    /* If this client was using any objects, remove it from the appropriate
     * lists. */
    PlasmaStore *plasma_state = client_context->plasma_state;
    for (const auto &entry : plasma_state->plasma_store_info->objects) {
      state->remove_client_from_object_clients(entry.second, client_context);
    }
    /* Note, the store may still attempt to send a message to the disconnected
     * client (for example, when an object ID that the client was waiting for
     * is ready). In these cases, the attempt to send the message will fail, but
     * the store should just ignore the failure. */
  } break;
  default:
    /* This code should be unreachable. */
    CHECK(0);
  }
}

void new_client_connection(event_loop *loop,
                           int listener_sock,
                           void *context,
                           int events) {
  PlasmaStore *store = (PlasmaStore *) context;
  int new_socket = accept_client(listener_sock);
  /* Create a new client object. This will also be used as the context to use
   * for events on this client's socket.  */
  store->clients.emplace_back(new Client(new_socket, store));
  /* Add a callback to handle events on this socket. */
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message,
                      store->clients.back().get());
  LOG_DEBUG("new connection with fd %d", new_socket);
}

/* Report "success" to valgrind. */
void signal_handler(int signal) {
  if (signal == SIGTERM) {
    exit(0);
  }
}

void start_server(char *socket_name, int64_t system_memory) {
  /* Ignore SIGPIPE signals. If we don't do this, then when we attempt to write
   * to a client that has already died, the store could die. */
  signal(SIGPIPE, SIG_IGN);
  /* Create the event loop. */
  event_loop *loop = event_loop_create();
  PlasmaStore store(loop, system_memory);
  int socket = bind_ipc_sock(socket_name, true);
  CHECK(socket >= 0);
  event_loop_add_file(loop, socket, EVENT_LOOP_READ, new_client_connection,
                      &store);
  event_loop_run(loop);
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
      CHECK(scanned == 1);
      LOG_INFO("Allowing the Plasma store to use up to %.2fGB of memory.",
               ((double) system_memory) / 1000000000);
      break;
    }
    default:
      exit(-1);
    }
  }
  if (!socket_name) {
    LOG_FATAL("please specify socket for incoming connections with -s switch");
  }
  if (system_memory == -1) {
    LOG_FATAL("please specify the amount of system memory with -m switch");
  }
#ifdef __linux__
  /* On Linux, check that the amount of memory available in /dev/shm is large
   * enough to accommodate the request. If it isn't, then fail. */
  int shm_fd = open("/dev/shm", O_RDONLY);
  struct statvfs shm_vfs_stats;
  fstatvfs(shm_fd, &shm_vfs_stats);
  /* The value shm_vfs_stats.f_bsize is the block size, and the value
   * shm_vfs_stats.f_bavail is the number of available blocks. */
  int64_t shm_mem_avail = shm_vfs_stats.f_bsize * shm_vfs_stats.f_bavail;
  close(shm_fd);
  if (system_memory > shm_mem_avail) {
    LOG_FATAL(
        "System memory request exceeds memory available in /dev/shm. The "
        "request is for %" PRId64 " bytes, and the amount available is %" PRId64
        " bytes. You may be able to free up space by deleting files in "
        "/dev/shm. If you are inside a Docker container, you may need to pass "
        "an argument with the flag '--shm-size' to 'docker run'.",
        system_memory, shm_mem_avail);
  }
#endif
  /* Make it so dlmalloc fails if we try to request more memory than is
   * available. */
  dlmalloc_set_footprint_limit((size_t) system_memory);
  LOG_DEBUG("starting server listening on %s", socket_name);
  start_server(socket_name, system_memory);
}
