#ifndef RAY_GCS_REDIS_OBJECT_INFO_ACCESSOR_H
#define RAY_GCS_REDIS_OBJECT_INFO_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/entry_change_notification.h"
#include "ray/gcs/object_info_accessor.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class RedisObjectInfoAccessor
/// RedisObjectInfoAccessor is an implementation of `ObjectInfoAccessor`
/// that uses Redis as the backend storage.
class RedisObjectInfoAccessor : public ObjectInfoAccessor {
 public:
  explicit RedisObjectInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisObjectInfoAccessor() {}

  Status AsyncGetLocations(const ObjectID &object_id,
                           const MultiItemCallback<ObjectTableData> &callback) override;

  Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                          const StatusCallback &callback) override;

  Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                             const StatusCallback &callback) override;

  Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeToLocations(const ObjectID &object_id,
                                     const StatusCallback &done) override;

 private:
  RedisGcsClient *client_impl_{nullptr};

  // Use a random ClientID for object subscription. Because:
  // If we use ClientID::Nil, GCS will still send all objects' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local ClientID, so we use
  // random ClientID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  ClientID node_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<ObjectID, ObjectChangeNotification, ObjectTable>
      ObjectSubscriptionExecutor;
  ObjectSubscriptionExecutor object_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_OBJECT_INFO_ACCESSOR_H
