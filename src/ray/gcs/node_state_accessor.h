#ifndef RAY_GCS_NODE_STATE_ACCESSOR_H
#define RAY_GCS_NODE_STATE_ACCESSOR_H

#include <boost/optional.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <vector>
#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/entry_change_notification.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;
class NodeStateCache;

/// \class NodeStateAccessor
/// NodeStateAccessor class encapsulates the implementation details of
/// reading, writing and subscribing of node's information (immutable fields like id,
/// and mutable fields like runtime state).
class NodeStateAccessor {
 public:
  explicit NodeStateAccessor(RedisGcsClient *client_impl);

  ~NodeStateAccessor() {}

  /// Register local node to GCS synchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \return Status
  Status RegisterSelf(const GcsNodeInfo &local_node_info);

  /// Cancel registration of local node to GCS synchronously.
  ///
  /// \return Status
  Status UnregisterSelf();

  /// Get id of local node which was registered by 'RegisterSelf'.
  ///
  /// \return ClientID
  const ClientID &GetSelfId() const;

  /// Get information of local node which was registered by 'RegisterSelf'.
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

  /// Subscribe to node addition and removal events from GCS and cache those information.
  ///
  /// \param subscribe Callback that will be called if a node is
  /// added or a node is removed.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
      const StatusCallback &done);

  /// Get nodes cache. The cache stores node information subscribed from GCS.
  /// NodeInfoCahce provides read-only interfaces such as `Get`.
  /// AsyncSubscribeToNodeChange() must be called successfully before you call this method.
  ///
  /// \return NodeStateCache
  NodeStateCache &Cache() const;

 private:
  RedisGcsClient *client_impl_{nullptr};
  std::unique_ptr<NodeStateCache> cache_{nullptr};
};

/// \class NodeStateCache
/// NodeStateCache stores node information subscribed from GCS.
/// It provides read-only interfaces such as `Get`, `GetAll`...
class NodeStateCache {
 public:
  explicit NodeStateCache(ClientTable *client_table);

  /// Get node information from local cache.
  /// Non-thread safe.
  ///
  /// \param node_id The ID of node to look up in local cache.
  /// \return The item returned by GCS. If the item to read doesn't exist,
  /// this optional object is empty.
  boost::optional<GcsNodeInfo> Get(const ClientID &node_id) const;

  /// Get information of all nodes from local cache.
  /// Non-thread safe.
  ///
  /// \return All nodes in cache.
  const std::unordered_map<ClientID, GcsNodeInfo> &GetAll() const;

  /// Search the local cache to find out if the given node is removed.
  /// Non-thread safe.
  ///
  /// \param node_id The id of the node to check.
  /// \return Whether the node is removed.
  bool IsRemoved(const ClientID &node_id) const;

 private:
  ClientTable *client_table_{nullptr};
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NODE_STATE_ACCESSOR_H
