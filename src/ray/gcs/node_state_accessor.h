#ifndef RAY_GCS_NODE_STATE_ACCESSOR_H
#define RAY_GCS_NODE_STATE_ACCESSOR_H

#include <boost/optional.hpp>
#include <map>
#include <mutex>
#include <vector>
#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class NodeStateAccessor
/// NodeStateAccessor class encapsulates the implementation details of
/// reading, writing and subscribing of node's information (immutable fields like id,
/// and mutable fields like runtime state).
class NodeStateAccessor {
 public:
  explicit NodeStateAccessor(RedisGcsClient &client_impl);

  ~NodeStateAccessor() {}

  /// Register local node to GCS synchronously, and begins subscription to all nodes
  /// from GCS (the node informations will be cached).
  ///
  /// \param node_info The information of node to register to GCS.
  /// \return Status
  Status RegisterSelf(const GcsNodeInfo &local_node_info);

  /// Cancel registration of local node to GCS synchronously, and cancel subscription
  /// to all nodes from GCS.
  ///
  /// \return Status
  Status UnregisterSelf();

  /// This callback is used to receive node information when a node member changed.
  using NodeInfoCallback = std::function<void(const GcsNodeInfo &node_info)>;

  /// Register callbacks to monitor the changes of node members.
  ///
  /// \param node_added_callback Callback that will be called if a new node is added.
  /// \param node_removed_callback Callback that will be called if a node is removed.
  /// TODO(micafan) Begin subscription to all nodes in this method.
  void RegisterWatcher(const NodeInfoCallback &node_added_callback,
                       const NodeInfoCallback &node_removed_callback);

  /// Get id of local node which registered by 'RegisterSelf'.
  ///
  /// \return ClientID
  const ClientID &GetSelfId() const;

  /// Get information of local node which registered by 'RegisterSelf'.
  ///
  /// \return GcsNodeInfo
  const GcsNodeInfo &GetSelfInfo() const;

  /// Cancel registration of a node to GCS asynchronously.
  ///
  /// \param node_id The ID of node that to be unregistered.
  /// \param callback Callback that will be called when unregistration is complete.
  /// \return Status
  Status AsyncUnregister(const ClientID &node_id, const StatusCallback &callback);

  /// Get information of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  Status AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback);

  /// Get node information from local cache.
  /// Non-thread safe.
  ///
  /// \param node_id The ID of node to look up in local cache.
  /// \return The item returned by GCS. If the item to read doesn't exist,
  /// this optional object is empty.
  boost::optional<GcsNodeInfo> GetFromCache(const ClientID &node_id) const;

  /// Get information of all nodes from local cache.
  /// Non-thread safe.
  ///
  /// \return All nodes in cache.
  const std::unordered_map<ClientID, GcsNodeInfo> &GetAllFromCache() const;

  /// Get the ids of all nodes from local cache.
  /// Non-thread safe.
  ///
  /// \return The ids of all nodes.
  std::vector<ClientID> GetAllIdsFromCache() const;

  /// Search the local cache to find out if the given node is removed.
  /// Non-thread safe.
  ///
  /// \param node_id The id of the node to check.
  /// \return Whether the node is removed.
  bool IsRemoved(const ClientID &node_id) const;

 private:
  RedisGcsClient &client_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NODE_STATE_ACCESSOR_H
