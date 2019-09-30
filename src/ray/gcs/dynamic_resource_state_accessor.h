#ifndef RAY_GCS_DYNAMIC_RESOURCE_STATE_ACCESSOR_H
#define RAY_GCS_DYNAMIC_RESOURCE_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class DynamicResourceStateAccessor
/// DynamicResourceStateAccessor class encapsulates the implementation details of
/// reading or writing or subscribing of node's dynamic resources from GCS.
class DynamicResourceStateAccessor {
 public:
  explicit DynamicResourceStateAccessor(RedisGcsClient &client_impl);

  ~DynamicResourceStateAccessor() {}

  // TODO(micafan) Define ResourceMap in GCS proto.
  typedef std::unordered_map<std::string, std::shared_ptr<ResourceTableData>> ResourceMap;

  /// Get node's dynamic resources from GCS asynchronously.
  ///
  /// \param node_id The ID of node to lookup dynamic resources.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  Status AsyncGet(const ClientID &node_id,
                  const OptionalItemCallback<ResourceMap> &callback);

  /// Update dynamic resources of node in GCS asynchronously.
  ///
  /// \param node_id The ID of node to update dynamic resources.
  /// \param resources The dynamic resources of node to be updated.
  /// \param callback Callback that will be called after update finishes.
  Status AsyncUpdate(const ClientID &node_id, const ResourceMap &resources,
                     const StatusCallback &callback);

  /// Delete resources of an node from GCS asynchronously.
  ///
  /// \param node_id The ID of node to delete resources from GCS.
  /// \param resource_tags The tags of resource to be deleted.
  /// \param callback Callback that will be called after delete finishes.
  Status AsyncDelete(const ClientID &node_id,
                     const std::vector<std::string> &resource_tags,
                     const StatusCallback &callback);

  /// Subscribe to any update operations of dynamic resources.
  ///
  /// \param subscribe Callback that will be called when any resource is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribe(
      const SubscribeCallback<ClientID, DynamicResourceNotification> &subscribe,
      const StatusCallback &done);

 private:
  RedisGcsClient &client_impl_;

  typedef SubscriptionExecutor<ClientID, DynamicResourceNotification,
                               DynamicResourceTable>
      DynamicResourceSubscriptionExecutor;
  DynamicResourceSubscriptionExecutor resource_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_DYNAMIC_RESOURCE_STATE_ACCESSOR_H
