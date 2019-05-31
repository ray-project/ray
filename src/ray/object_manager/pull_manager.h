#ifndef RAY_OBJECT_MANAGER_PULL_MANAGER_H
#define RAY_OBJECT_MANAGER_PULL_MANAGER_H

#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "ray/id.h"
#include "ray/status.h"

namespace ray {

class PullManager;

/// \struct PullInfo
///
/// This object manages all of the information associated with an attempt to
/// receive a given object.
struct PullInfo {
  PullInfo(bool required);

  /// Fill out the total_num_chunks field. We won't know this until we know the
  /// object size and so can't always fill out this field in the constructor.
  ///
  /// \param num_chunks The number of chunks.
  /// \return Void.
  void InitializeChunksIfNecessary(int64_t num_chunks);

  /// Check if this PullInfo object can safely be cleaned up because the object
  /// is not required, and we are not receiving the object from anywhere, and
  /// there are no chunks in the process of being read for this object.
  ///
  /// \return True if this object can be cleaned up and false otherwise.
  bool LifetimeEnded();

  /// True if we must pull this object. False if we are simply receiving the
  /// object but do not need to pull the object (meaning we do not have to
  /// guarantee that the object appears locally).
  bool required;
  /// Our most recent estimate of which object managers have the object.
  std::unordered_set<ClientID> clients_with_object;
  /// The IDs of the remote object managers that we have already requested
  /// the object from. If we cancel a request, then we will remove that
  /// client from this set.
  std::unordered_set<ClientID> clients_requested_from;
  /// If this object manager is currently receiving the object from a remote
  /// object manager, this will be the client ID of the remote object
  /// manager. Otherwise, it will be nil.
  ClientID client_receiving_from;
  /// The push id of the attempt to push the object from client_receiving_from.
  /// We use the push id to determine if this is the original push attempt we
  /// are receiving the object from.
  UniqueID push_id;
  /// The total number of chunks that the object is divided into. If this is
  /// -1, then the number is not known.
  int64_t total_num_chunks;
  /// The chunk IDs that have successfully been received.
  std::unordered_set<int> received_chunk_ids;
  /// The chunks that have not yet been received. This is the complement of
  /// the values in received_chunk_ids.
  std::unordered_set<int> remaining_chunk_ids;
  /// The number of object chunks that we are in the process of reading.
  int64_t num_in_progress_chunk_ids;
};

/// This class is responsible for ensuring that objects that this object
/// manager is attempting to pull eventually show up. It is responsible for
/// deciding when to issue requests to and cancel requests from remote object
/// managers as well as when to abort an object creation.
///
/// We say that we are "receiving" an object from another object manager's
/// specific push request if we received a ReceivePushRequest from it while we
/// were not receiving the object from any other object managers, no reads from
/// that object manager have subsequently failed, and not more than a certain
/// amount of time has passed since the last chunk from the specific push
/// request of the sending object manager has been received.
///
/// From the "receiving" state, we can transition to the "received" state if we
/// have received all the chunks associated with the object. We can also
/// transition into the "not receiving" state if we fail to read a chunk from
/// the push request from the sending object manager or the timer has expired.
///
/// The lifetime of a "pull" begins when either PullObject is called or when
/// ReceivePushRequest is called (whichever comes first). The lifetime ends as
/// soon as three conditions are met:
/// 1. The pull is no longer required because CancelPullObject has been called
///    or it was never required in the first place because PullObject was
///    never called.
/// 2. There are no in progress chunks as represented by
///    num_in_progress_chunk_ids.
/// 3. We are currently not receiving the object from any object manager. This
///    means that if we started receiving it from some object manager earlier,
///    then we one of the chunks from that object manager failed to be read or
///    we have not received any chunks from that object manager in a while.
///
/// A given object is considered to be "being received" from a remote object
/// manager if the following conditions are met:
/// 1. While we were in the state of not receiving the object, we received a
///    ReceivePushRequest message.
/// 2. Since the ReceivePushRequest message, none of the chunks failed to be
///    read and the timer has not expired.
///
/// Each pull has a timer, which operates as follows:
/// 1. The timer begins as soon as the pull object is created.
/// 2. Whenever we request the object from a new object manager, the timer is
///    reset.
/// 3. Whenever we receive a chunk from the object manager that we are
///    receiving the object from, the timer is reset.
/// 4. The timer is canceled when the object has been fully received
///    successfully.
/// 5. The timer is canceled when the pull lifetime ends.
/// 6. When the timer expires, we do the following:
///    - If no remote object managers have the object, we reset the timer.
///    - If other remote object managers have the object and have not started
///    - receiving the object, we issue some new requests for the object and
///      possibly cancel some requests.
///    - If we are in the process of receiving the object, we cancel the
///      request and issue at least one new request for the object.
///    - If the object is required, reset the timer.
///
/// When we are pulling an object, we only will read chunks from the first
/// object manager that sends us a push request. Chunks from other object
/// managers will be ignored. If we fail to read a chunk from the object
/// manager that we are not ignoring, then we consider ourselves to be no
/// longer reading from that object manager.
///
/// When do we abort an object creation? We only ever abort a creation for
/// objects that we are not required to pull (e.g., those that were being
/// pushed to us or whose pulls were canceled). There is the danger that we
/// abort an object creation only to have a subsequent read try to recreate
/// it. To deal with this, we have the main thread abort object creations only
/// when the lifetime of a pull ends.
class PullManager {
 public:
  /// Construct a PullManager.
  ///
  /// \param main_service The service to use for running timer callbacks.
  /// \param client_id The ID of this object manager.
  /// \param callback The callback that the pull manager can use to request
  /// objects, cancel requests, and abort object creations.
  PullManager(const ClientID &client_id);

  PullManager(const PullManager &other) = delete;

  PullManager &operator=(const PullManager &other) = delete;

  /// Pull an object. This will guarantee that the object is pulled. That is,
  /// it will keep trying to request the object until CancelPullObject is
  /// called. CancelPullObject is also called when the object appears locally.
  ///
  /// \param object_id The ID of the object to pull.
  /// \param[out] subscribe_to_locations This will be set to true if the caller
  /// should subscribe to the object locations and false otherwise.
  /// \param[out] start_timer This will be set to true if a timer should be
  /// started for this object ID and false otherwise.
  /// \return Void.
  void PullObject(const ObjectID &object_id, bool *subscribe_to_locations,
                  bool *start_timer);

  /// Notify the PullManager that a certain object is no longer required. This
  /// is also called when an object appears locally.
  ///
  /// \param object_id The ID of the object whose to pull.
  /// \param[out] clients_to_cancel This will be set to a vector of clients to
  /// issue cancellation requests to.
  /// \param[out] unsubscribe_from_locations This will be set to true if the
  /// caller should unsubscribe from the object locations and false otherwise.
  /// \return Void.
  void CancelPullObject(const ObjectID &object_id,
                        std::vector<ClientID> *clients_to_cancel,
                        bool *unsubscribe_from_locations);

  /// Notify the PullManager that a remote object manager wishes to push an
  /// object chunk to this object manager. Note that this will happen once per
  /// chunk, not once per object.
  ///
  /// \param push_id The ID of the push attempt of object_id from client_id that
  /// the object is being pushed from.
  /// \param object_id The ID of the object that is being pushed.
  /// \param client_id The ID of the remote object manager that is pushing the
  /// object.
  /// \param chunk_index The index of the chunk that is being pushed.
  /// \param num_chunks The total number of chunks that the object is divided
  /// into.
  /// \param[out] clients_to_cancel A vector of clients to issue cancellation
  /// requests to.
  /// \param[out] start_timer This will be set to true if the caller should
  /// start a timer for this object ID and false otherwise.
  /// \return Void.
  void ReceivePushRequest(const UniqueID &push_id, const ObjectID &object_id,
                          const ClientID &client_id, int64_t chunk_index,
                          int64_t num_chunks, std::vector<ClientID> *clients_to_cancel,
                          bool *start_timer);

  /// Notify the PullManager that the locations of the object in the object
  /// table have changed.
  ///
  /// \param object_id The ID of the object whose locations have changed.
  /// \param clients_with_object The IDs of the object managers that have the
  /// object.
  /// \param[out] clients_to_request This will be set to a list of clients to
  /// issue requests to.
  /// \param[out] restart_timer This will be set to true if the timer for this
  /// object ID should be restarted and false otherwise.
  /// \return Void.
  void NewObjectLocations(const ObjectID &object_id,
                          const std::unordered_set<ClientID> &clients_with_object,
                          std::vector<ClientID> *clients_to_request, bool *restart_timer);

  /// Notify the PullManager that a chunk was read successfully.
  ///
  /// \param push_id The ID of the push attempt of object_id from client_id that
  /// the object was read.
  /// \param object_id The ID of the object that was read.
  /// \param client_id The ID of the remote object manager that the object was
  /// read from.
  /// \param chunk_index The index of the chunk.
  /// \param[out] abort_creation This is set to true if the object creation
  /// should be aborted and false otherwise.
  /// \param[out] restart_timer This is set to true if the timer for this object
  /// should be restarted and false otherwise.
  /// \return Void.
  void ChunkReadSucceeded(const UniqueID &push_id, const ObjectID &object_id,
                          const ClientID &client_id, int64_t chunk_index,
                          bool *abort_creation, bool *restart_timer);

  /// Notify the PullManager that a chunk was not successfully read. This could
  /// happen because the chunk was intentionally ignored, because the object
  /// already existed in the store, because the remote object manager died, or
  /// because the object store was full or the object was already present in the
  /// object store when we tried to create a chunk.
  ///
  /// \param push_id The ID of the push attempt of object_id from client_id that
  /// the object was read.
  /// \param object_id The ID of the object that was read.
  /// \param client_id The ID of the remote object manager that the object was
  /// read from.
  /// \param chunk_index The index of the chunk.
  /// \param[out] This is set to a vector of clients to issue cancellation
  /// requests to.
  /// \param[out] This is set to true if the object creation should be aborted
  /// and false otherwise.
  /// \return Void.
  void ChunkReadFailed(const UniqueID &push_id, const ObjectID &object_id,
                       const ClientID &client_id, int64_t chunk_index,
                       std::vector<ClientID> *clients_to_cancel, bool *abort_creation);

  /// Handle the fact that the timer for a pull has expired.
  ///
  /// \param push_id The ID of the push attempt of object_id from client_id that
  /// the pull is for.
  /// \param object_id The ID of the object that the pull is for.
  /// \param[out] clients_to_request This will be set to a vector of clients to
  /// issue requests.
  /// \param[out] abort_creation This wil be set to true if the object creation
  /// should be aborted and false otherwise.
  /// \param[out] restart_timer This will be set to true if the timer for this
  /// object should be restarted and false otherwise.
  /// \return Void.
  void TimerExpired(const ObjectID &object_id, std::vector<ClientID> *clients_to_request,
                    bool *abort_creation, bool *restart_timer);

  /// Print out a human-readable string describing the PullManager's state.
  ///
  /// \return A human-readable string.
  std::string DebugString() const;

 private:
  /// The total number of times PullObject has been called.
  int64_t total_pull_calls_;
  /// The total number of times CancelObject has been called.
  int64_t total_cancel_calls_;
  /// The total number of times ChunkReadSucceeded has been called.
  int64_t total_successful_chunk_reads_;
  /// The total number of times ChunkReadFailed has been called.
  int64_t total_failed_chunk_reads_;
  /// The client ID of the object manager that this pull manager is part of.
  ClientID client_id_;
  /// This is a map from object ID that we are pulling to the information
  /// associated with that pull. NOTE: We use unique_ptr<PullInfo> instead of
  /// PullInfo because the PullInfo object uses the "this" pointer and so cannot
  /// be moved around.
  std::unordered_map<ObjectID, std::unique_ptr<PullInfo>> pulls_;
  /// A random number generator.
  std::mt19937_64 gen_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_PULL_MANAGER_H
