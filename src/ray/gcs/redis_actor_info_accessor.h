#ifndef RAY_GCS_REDIS_ACTOR_INFO_ACCESSOR_H
#define RAY_GCS_REDIS_ACTOR_INFO_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/actor_info_accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

std::shared_ptr<gcs::ActorTableData> CreateActorTableData(
    const TaskSpecification &task_spec, const rpc::Address &address,
    gcs::ActorTableData::ActorState state, uint64_t remaining_reconstructions);

class RedisGcsClient;

/// \class RedisActorInfoAccessor
/// `RedisActorInfoAccessor` is an implementation of `ActorInfoAccessor`
/// that uses Redis as the backend storage.
class RedisActorInfoAccessor : public ActorInfoAccessor {
 public:
  explicit RedisActorInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisActorInfoAccessor() {}

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<ActorTableData> &callback) override;

  Status AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                       const StatusCallback &callback) override;

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<ActorTableData> &data_ptr,
                     const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                           const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done) override;

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

#endif  // RAY_GCS_REDIS_ACTOR_INFO_ACCESSOR_H
