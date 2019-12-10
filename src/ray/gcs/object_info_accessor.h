#ifndef RAY_GCS_OBJECT_INFO_ACCESSOR_H
#define RAY_GCS_OBJECT_INFO_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/entry_change_notification.h"

namespace ray {

namespace gcs {

/// `ObjectInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// object information in the GCS.
class ObjectInfoAccessor {
 public:
  virtual ~ObjectInfoAccessor() {}

  /// Get object's locations from GCS asynchronously.
  ///
  /// \param object_id The ID of object to lookup in GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetLocations(
      const ObjectID &object_id,
      const MultiItemCallback<rpc::ObjectTableData> &callback) = 0;

  /// Add location of object to GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be added to GCS.
  /// \param node_id The location that will be added to GCS.
  /// \param callback Callback that will be called after object has been added to GCS.
  /// \return Status
  virtual Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                                  const StatusCallback &callback) = 0;

  /// Remove location of object from GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be removed from GCS.
  /// \param node_id The location that will be removed from GCS.
  /// \param callback Callback that will be called after the delete finished.
  /// \return Status
  virtual Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                                     const StatusCallback &callback) = 0;

  /// Subscribe to any update of an object's location.
  ///
  /// \param object_id The ID of the object to be subscribed to.
  /// \param subscribe Callback that will be called each time when the object's
  /// location is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to any update of an object's location.
  ///
  /// \param object_id The ID of the object to be unsubscribed to.
  /// \param done Callback that will be called when unsubscription is complete.
  /// \return Status
  virtual Status AsyncUnsubscribeToLocations(const ObjectID &object_id,
                                             const StatusCallback &done) = 0;

 protected:
  ObjectInfoAccessor() = default;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_OBJECT_INFO_ACCESSOR_H
