#ifndef RAY_GCS_ACTOR_STATE_ACCESSOR_INTERFACE_H
#define RAY_GCS_ACTOR_STATE_ACCESSOR_INTERFACE_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"

namespace ray {

namespace gcs {

/// \class ActorStateAccessorInterface
/// ActorStateAccessorInterface class introduce the interface of
/// reading or writing or subscribing of actor's specification (immutable fields which
/// determined at submission time, and mutable fields which are determined at runtime).
class ActorStateAccessorInterface {
 public:
  virtual ~ActorStateAccessorInterface() = default;

  /// Get actor specification from GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGet(const ActorID &actor_id,
                          const OptionalItemCallback<ActorTableData> &callback) = 0;

  /// Get actor specification from GCS synchronously.
  ///
  /// \param actor_id The ID of actor to look up in the GCS.
  /// \param actor_data Actor data that will be returned.
  /// \return Status
  virtual Status SyncGet(const ActorID &actor_id, ActorTableData *actor_data) = 0;

  /// Get all actors id from GCS synchronously.
  ///
  /// \param actor_ids Actor id list that will be returned.
  /// \return Status
  virtual Status SyncGetAllIds(std::vector<ActorID> *actor_ids) = 0;

  /// Get all actors id from GCS asynchronously.
  ///
  /// \param callback Callback that will be called when received reply.
  /// \return Status
  virtual Status AsyncGetAllIds(const MultiItemCallback<ActorID> &callback) = 0;

  /// Get all actors specification from GCS asynchronously.
  ///
  /// \param callback Callback that will be called when received reply.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<ActorTableData> &callback) = 0;

  /// Register an actor to GCS asynchronously.
  ///
  /// \param data_ptr The actor that will be registered to the GCS.
  /// \param callback Callback that will be called after actor has been registered
  /// to the GCS.
  /// \return Status
  virtual Status AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                               const StatusCallback &callback) = 0;

  /// Update dynamic states of actor in GCS asynchronously.
  ///
  /// \param actor_id ID of the actor to update.
  /// \param data_ptr Data of the actor to update.
  /// \param callback Callback that will be called after update finishes.
  /// \return Status
  /// TODO(micafan) Don't expose the whole `ActorTableData` and only allow
  /// updating dynamic states.
  virtual Status AsyncUpdate(const ActorID &actor_id,
                             const std::shared_ptr<ActorTableData> &data_ptr,
                             const StatusCallback &callback) = 0;

  /// Update dynamic states of actor in GCS synchronously.
  ///
  /// \param actor_id ID of the actor to update.
  /// \param data_ptr Data of the actor to update.
  /// \return Status
  /// TODO(micafan) Don't expose the whole `ActorTableData` and only allow
  /// updating dynamic states.
  virtual Status SyncUpdate(const ActorID &actor_id,
                            const std::shared_ptr<ActorTableData> &data_ptr) = 0;

  /// Subscribe to any register or update operations of actors.
  ///
  /// \param subscribe Callback that will be called each time when an actor is registered
  /// or updated.
  /// \param done Callback that will be called when subscription is complete and we
  /// are ready to receive notification.
  /// \return Status
  virtual Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, ActorTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Subscribe to any update operations of an actor.
  ///
  /// \param actor_id The ID of actor to be subscribed to.
  /// \param subscribe Callback that will be called each time when the actor is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribe(
      const ActorID &actor_id,
      const SubscribeCallback<ActorID, ActorTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to an actor.
  ///
  /// \param actor_id The ID of the actor to be unsubscribed to.
  /// \param done Callback that will be called when unsubscribe is complete.
  /// \return Status
  virtual Status AsyncUnsubscribe(const ActorID &actor_id, const StatusCallback &done) = 0;

 protected:
  ActorStateAccessorInterface() = default;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ACTOR_STATE_ACCESSOR_INTERFACE_H