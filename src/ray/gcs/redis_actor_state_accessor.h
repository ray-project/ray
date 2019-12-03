#ifndef RAY_GCS_REDIS_ACTOR_STATE_ACCESSOR_H
#define RAY_GCS_REDIS_ACTOR_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/actor_state_accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class RedisActorStateAccessor
/// RedisActorStateAccessor class encapsulates the implementation details of
/// reading or writing or subscribing of actor's specification (immutable fields which
/// determined at submission time, and mutable fields which are determined at runtime).
class RedisActorStateAccessor : public ActorStateAccessor {
 public:
  explicit RedisActorStateAccessor(RedisGcsClient *client_impl);

  virtual ~RedisActorStateAccessor() {}

  /// Get actor specification from GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<ActorTableData> &callback) override;

  /// Register an actor to GCS asynchronously.
  ///
  /// \param data_ptr The actor that will be registered to the GCS.
  /// \param callback Callback that will be called after actor has been registered
  /// to the GCS.
  /// \return Status
  Status AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                       const StatusCallback &callback) override;

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
                     const StatusCallback &callback) override;

  /// Subscribe to any register or update operations of actors.
  ///
  /// \param subscribe Callback that will be called each time when an actor is registered
  /// or updated.
  /// \param done Callback that will be called when subscription is complete and we
  /// are ready to receive notification.
  /// \return Status
  Status AsyncSubscribeAll(const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                           const StatusCallback &done) override;

  /// Subscribe to any update operations of an actor.
  ///
  /// \param actor_id The ID of actor to be subscribed to.
  /// \param subscribe Callback that will be called each time when the actor is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  /// Cancel subscription to an actor.
  ///
  /// \param actor_id The ID of the actor to be unsubscribed to.
  /// \param done Callback that will be called when unsubscribe is complete.
  /// \return Status
  Status AsyncUnsubscribe(const ActorID &actor_id, const StatusCallback &done) override;

 private:
  RedisGcsClient *client_impl_{nullptr};
  // Use a random ClientID for actor subscription. Because:
  // If we use ClientID::Nil, GCS will still send all actors' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local ClientID, so we use
  // random ClientID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  ClientID node_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<ActorID, ActorTableData, ActorTable>
      ActorSubscriptionExecutor;
  ActorSubscriptionExecutor actor_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_ACTOR_STATE_ACCESSOR_H
