#ifndef RAY_GCS_NODE_INFO_ACCESSOR_H
#define RAY_GCS_NODE_INFO_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class NodeInfoAccessor
/// `NodeInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// node information in the GCS.
class NodeInfoAccessor {
 public:
  virtual ~NodeInfoAccessor() = default;

  /// Register local node to GCS synchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \return Status
  virtual Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info) = 0;

  /// Cancel registration of local node to GCS synchronously.
  ///
  /// \return Status
  virtual Status UnregisterSelf() = 0;

  /// Whether local node has been unregistered to GCS.
  /// Non-thread safe.
  ///
  /// \return bool
  virtual bool IsSelfUnregistered() const = 0;

  /// Get id of local node which was registered by 'RegisterSelf'.
  ///
  /// \return ClientID
  virtual const ClientID &GetSelfId() const = 0;

  /// Get information of local node which was registered by 'RegisterSelf'.
  ///
  /// \return GcsNodeInfo
  virtual const rpc::GcsNodeInfo &GetSelfInfo() const = 0;

  /// Cancel registration of a node to GCS asynchronously.
  ///
  /// \param node_id The ID of node that to be unregistered.
  /// \param callback Callback that will be called when unregistration is complete.
  /// \return Status
  virtual Status AsyncUnregister(const ClientID &node_id, const StatusCallback &callback) = 0;

  /// Get information of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback) = 0;

  /// Subscribe to node addition and removal events from GCS and cache those information.
  ///
  /// \param subscribe Callback that will be called if a node is
  /// added or a node is removed.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<ClientID, rpc::GcsNodeInfo> &subscribe,
      const StatusCallback &done) = 0;

  /// Get node information from local cache.
  /// Non-thread safe.
  ///
  /// \param node_id The ID of node to look up in local cache.
  /// \return The item returned by GCS. If the item to read doesn't exist,
  /// this optional object is empty.
  virtual boost::optional<rpc::GcsNodeInfo> Get(const ClientID &node_id) const = 0;

  /// Get information of all nodes from local cache.
  /// Non-thread safe.
  ///
  /// \return All nodes in cache.
  virtual const std::unordered_map<ClientID, rpc::GcsNodeInfo> &GetAll() const = 0;

  /// Search the local cache to find out if the given node is removed.
  /// Non-thread safe.
  ///
  /// \param node_id The id of the node to check.
  /// \return Whether the node is removed.
  virtual bool IsRemoved(const ClientID &node_id) const = 0;

 protected:
  NodeInfoAccessor() = default;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NODE_INFO_ACCESSOR_H
