#ifndef RAY_GCS_OBJECT_STATE_ACCESSOR_H
#define RAY_GCS_OBJECT_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class ObjectStateAccessor
/// ObjectStateAccessor class encapsulates the implementation details of
/// reading or writing or subscribing of object's information.
class ObjectStateAccessor {
 public:
  explicit ObjectStateAccessor(RedisGcsClient &client_impl);

  ~ObjectStateAccessor() {}

  /// Get object information from GCS asynchronously.
  ///
  /// \param object_id The ID of object to lookup in GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  Status AsyncGet(const ObjectID &object_id,
                  const MultiItemCallback<ObjectTableData> &callback);

  /// Add object information to GCS asynchronously.
  ///
  /// \param object_id The ID of object that will be add to GCS.
  /// \param data_ptr The object information that will be add to GCS.
  /// \param callback Callback that will be called after object has been added to GCS.
  /// \return Status
  Status AsyncAdd(const ObjectID &object_id,
                  const std::shared_ptr<ObjectTableData> &data_ptr,
                  const StatusCallback &callback);

  /// Delete object information from GCS asynchronously.
  ///
  /// \param object_id The ID of object that will be delete from GCS.
  /// \param data_ptr The object information that will be delete from GCS.
  /// \param callback Callback that will be called after object has been deleted from GCS.
  /// \return Status
  Status AsyncDelete(const ObjectID &object_id,
                     const std::shared_ptr<ObjectTableData> &data_ptr,
                     const StatusCallback &callback);

  /// Subscribe to any update operations of an object.
  ///
  /// \param object_id The ID of the object to be subscribed to.
  /// \param subscribe Callback that will be called each time when the object is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribe(const ObjectID &object_id,
                        const SubscribeCallback<ObjectID, ObjectNotification> &subscribe,
                        const StatusCallback &done);

  /// Cancel subscription to any update operations of an object.
  ///
  /// \param object_id The ID of the object to be unsubscribed to.
  /// \param done Callback that will be called when unsubscription is complete.
  Status AsyncUnsubscribe(const ObjectID &object_id, const StatusCallback &done);

 private:
  RedisGcsClient &client_impl_;

  // Use a random ClientID for actor subscription. Because:
  // If we use ClientID::Nil, GCS will still send all objects' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local ClientID, so we use
  // random ClientID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  ClientID node_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<ObjectID, ObjectNotification, ObjectTable>
      ObjectSubscriptionExecutor;
  ObjectSubscriptionExecutor object_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_OBJECT_STATE_ACCESSOR_H
