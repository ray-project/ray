#ifndef PLASMA_STORE_H
#define PLASMA_STORE_H

#include "eviction_policy.h"
#include "plasma.h"
#include "plasma_common.h"
#include "plasma_events.h"
#include "plasma_protocol.h"

class GetRequest;

struct NotificationQueue {
  /// The object notifications for clients. We notify the client about the
  /// objects in the order that the objects were sealed or deleted.
  std::deque<uint8_t *> object_notifications;
};

/// Contains all information that is associated with a Plasma store client.
struct Client {
  Client(int fd);

  /// The file descriptor used to communicate with the client.
  int fd;
};

class PlasmaStore {
 public:
  PlasmaStore(EventLoop *loop, int64_t system_memory);

  ~PlasmaStore();

   /// Create a new object. The client must do a call to release_object to tell
   /// the store when it is done with the object.
   ///
   /// @param object_id Object ID of the object to be created.
   /// @param data_size Size in bytes of the object to be created.
   /// @param metadata_size Size in bytes of the object metadata.
   /// @return One of the following error codes:
   ///         - PlasmaError_OK, if the object was created successfully.
   ///         - PlasmaError_ObjectExists, if an object with this ID is already
   ///           present in the store. In this case, the client should not call
   ///           plasma_release.
   ///         - PlasmaError_OutOfMemory, if the store is out of memory and cannot
   ///           create the object. In this case, the client should not call
   ///           plasma_release.
  int create_object(ObjectID object_id,
                    int64_t data_size,
                    int64_t metadata_size,
                    Client *client,
                    PlasmaObject *result);

   /// Delete objects that have been created in the hash table. This should only
   /// be called on objects that are returned by the eviction policy to evict.
   ///
   /// @param object_ids Object IDs of the objects to be deleted.
   /// @return Void.
  void delete_objects(const std::vector<ObjectID> &object_ids);

   /// Process a get request from a client. This method assumes that we will
   /// eventually have these objects sealed. If one of the objects has not yet
   /// been sealed, the client that requested the object will be notified when it
   /// is sealed.
   ///
   /// For each object, the client must do a call to release_object to tell the
   /// store when it is done with the object.
   ///
   /// @param client The client making this request.
   /// @param object_ids Object IDs of the objects to be gotten.
   /// @param timeout_ms The timeout for the get request in milliseconds.
   /// @return Void.
  void process_get_request(Client *client,
                           const std::vector<ObjectID> &object_ids,
                           uint64_t timeout_ms);

   /// Seal an object. The object is now immutable and can be accessed with get.
   ///
   /// @param object_id Object ID of the object to be sealed.
   /// @param digest The digest of the object. This is used to tell if two objects
   ///        with the same object ID are the same.
   /// @return Void.
  void seal_object(ObjectID object_id, unsigned char digest[]);

   /// Check if the plasma store contains an object:
   ///
   /// @param object_id Object ID that will be checked.
   /// @return OBJECT_FOUND if the object is in the store, OBJECT_NOT_FOUND if not
  int contains_object(ObjectID object_id);

   /// Record the fact that a particular client is no longer using an object.
   ///
   /// @param object_id The object ID of the object that is being released.
   /// @param client The client making this request.
   /// @param Void.
  void release_object(ObjectID object_id, Client *client);

   /// Subscribe a file descriptor to updates about new sealed objects.
   ///
   /// @param client The client making this request.
   /// @return Void.
  void subscribe_to_updates(Client *client);

   /// Connect a new client to the PlasmaStore.
   ///
   /// @param listener_sock The socket that is listening to incoming connections.
   /// @return Void.
  void connect_client(int listener_sock);

   /// Disconnect a client from the PlasmaStore.
   ///
   /// @param client The client that is disconnected.
   /// @return Void.
  void disconnect_client(Client *client);

  void send_notifications(int client_fd);

  Status process_message(Client *client);

 private:
  void push_notification(ObjectInfoT *object_notification);

  void add_client_to_object_clients(ObjectTableEntry *entry, Client *client);

  void return_from_get(GetRequest *get_req);

  void update_object_get_requests(ObjectID object_id);

  int remove_client_from_object_clients(ObjectTableEntry *entry,
                                        Client *client);

  /// Event loop of the plasma store.
  EventLoop *loop_;
  /// The plasma store information, including the object tables, that is exposed
  /// to the eviction policy.
  PlasmaStoreInfo store_info_;
  /// The state that is managed by the eviction policy.
  EvictionPolicy eviction_policy_;
  /// Input buffer. This is allocated only once to avoid mallocs for every
  /// call to process_message.
  std::vector<uint8_t> input_buffer_;
  /// A hash table mapping object IDs to a vector of the get requests that are
  /// waiting for the object to arrive.
  std::unordered_map<ObjectID, std::vector<GetRequest *>, UniqueIDHasher>
      object_get_requests_;
  /// The pending notifications that have not been sent to subscribers because
  /// the socket send buffers were full. This is a hash table from client file
  /// descriptor to an array of object_ids to send to that client.
  /// TODO(pcm): Consider putting this into the Client data structure and
  /// reorganize the code slightly.
  std::unordered_map<int, NotificationQueue> pending_notifications_;
};

#endif // PLASMA_STORE_H
