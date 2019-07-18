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
/// reading or writing or subscribing of actor's specification (immutable fields which
/// determined at submission time, and mutable fields which are determined at runtime).
class ActorStateAccessor {
 public:
  explicit ActorStateAccessor(RedisGcsClient &client_impl);

  ~ActorStateAccessor() {}

  /// Get actor specification from GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  Status AsyncGet(const ActorID &actor_id,
                  const MultiItemCallback<ActorTableData> &callback);

  /// Register an actor to GCS asynchronously.
  ///
  /// \param data_ptr The actor that will be registered to the GCS.
  /// \param callback Callback that will be called after actor has been registered
  /// to the GCS.
  /// \return Status
  Status AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                       const StatusCallback &callback);

  /// Update dynamic states of actor in GCS asynchronously.
  ///
  /// \param actor_id ID of the actor to update.
  /// \param data_ptr Data of the actor to update.
  /// \param callback Callback that will be called after update finishes.
  /// \return Status
  /// TODO(micafan) Don't expose the whole `ActorTableData` and only allow
  /// updating dynamic states.
  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<ActorTableData> &data_ptr,
                     const StatusCallback &callback);

  /// Subscribe to any register operations of actors.
  ///
  /// \param subscribe Callback that will be called each time when an actor is registered
  /// or updated.
  /// \param done Callback that will be called when subscription is complete and we
  /// are ready to receive notification.
  /// \return Status
  Status AsyncSubscribe(const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done);

 private:
  RedisGcsClient &client_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ACTOR_STATE_ACCESSOR_H
