#ifndef RAY_GCS_ACTOR_STATE_ACCESSOR_H
#define RAY_GCS_ACTOR_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class ActorStateAccessor
/// ActorStateAccessor class encapsulates the implementation details of
/// read or write or subscribe of actor's specification(immutable fields which
/// determined at submission time, and mutable fields which determined at runtime).
class ActorStateAccessor {
 public:
  explicit ActorStateAccessor(RedisGcsClient &client_impl);

  ~ActorStateAccessor() {}

  /// Get actor specification from gcs asynchronously.
  ///
  /// \param job_id The ID of the job (driver or app).
  /// \param actor_id The ID of actor that is looked up in the GCS.
  /// \param callback Callback that is called after read data done.
  /// \return Status
  Status AsyncGet(const JobID &job_id, const ActorID &actor_id,
                  const MultiItemCallback<ActorTableData> &callback);

  /// Add a actor to gcs asynchronously.
  ///
  /// \param job_id The ID of the job (driver or app).
  /// \param actor_id The ID of actor that is add to the GCS.
  /// \param data_ptr The actor that is add to the GCS.
  /// \param callback Callback that is called after the data has been written to the GCS.
  /// \return Status
  Status AsyncAdd(const JobID &job_id, const ActorID &actor_id,
                  std::shared_ptr<ActorTableData> data_ptr,
                  const StatusCallback &callback);

  /// Update actor runtime state to gcs asynchronously.
  ///
  /// \param job_id The ID of the job (driver or app).
  /// \param actor_id The ID of actor that is update to the GCS.
  /// \param data_ptr The actor that is update to the GCS.
  /// \param callback Callback that is called after the data has been written to the GCS.
  /// \return Status
  /// TODO(micafan) Modify parameter 3 from ActorTableData to 'ActorRuntimeState'
  Status AsyncUpdate(const JobID &job_id, const ActorID &actor_id,
                     std::shared_ptr<ActorTableData> data_ptr,
                     const StatusCallback &callback);

  /// Subscribe to any add operations of actor. The caller may choose
  /// to subscribe to all add, or to subscribe only to actors that it
  /// requests notifications for. This may only be called once per update.
  ///
  /// \param job_id The ID of the job (= driver).
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given client will be received. In the latter
  /// case, the client may request notifications on specific actors in GCS
  /// via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data of the actor.
  /// \param done Callback that is called when subscription is complete and we
  /// are ready to receive messages.
  /// \return Status
  Status AsyncSubscribe(const JobID &job_id, const ClientID &client_id,
                        const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done);

 private:
  RedisGcsClient &client_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ACTOR_STATE_ACCESSOR_H
