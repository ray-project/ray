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

  /// Get object's locations from GCS asynchronously.
  ///
  /// \param object_id The ID of object to lookup in GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  Status AsyncGetLocations(const ObjectID &object_id,
                           const MultiItemCallback<ObjectTableData> &callback);

  /// Add location of object to GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be added to GCS.
  /// \param node_id The location that will be added to GCS.
  /// \param callback Callback that will be called after object has been added to GCS.
  /// \return Status
  Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                          const StatusCallback &callback);

  /// Remove location of object from GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be removed from GCS.
  /// \param node_id The location that will be removed from GCS.
  /// \param callback Callback that will be called after the delete finished.
  /// \return Status
  Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                             const StatusCallback &callback);

  /// Subscribe to any update of an object's location.
  ///
  /// \param object_id The ID of the object to be subscribed to.
  /// \param subscribe Callback that will be called each time when the object's
  /// location is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, ObjectNotification> &subscribe,
      const StatusCallback &done);

  /// Cancel subscription to any update of an object's location.
  ///
  /// \param object_id The ID of the object to be unsubscribed to.
  /// \param done Callback that will be called when unsubscription is complete.
  Status AsyncUnsubscribeToLocations(const ObjectID &object_id,
                                     const StatusCallback &done);

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
