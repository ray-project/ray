#ifndef RAY_GCS_NODE_STATE_ACCESSOR_H
#define RAY_GCS_NODE_STATE_ACCESSOR_H

#include <map>
#include <vector>
#include "ray/common/id.h"
#include "ray/gcs/call_back.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class GcsClientImpl;

/// \class NodeStateAccessor
/// NodeStateAccessor class encapsulates the implementation details of
/// read or write or subscribe of node's specification(immutable fields like id,
/// and mutable fields like runtime state).
class NodeStateAccessor {
 public:
  NodeStateAccessor(GcsClientImpl &client_impl);

  ~NodeStateAccessor() {}

  /// Register node(raylet) to GCS.
  ///
  /// \param node The node that is add to GCS
  /// \return Status
  Status Register(const ClientTableData &node);

  /// Get the information of all nodes(all raylet) from GCS.
  ///
  /// \param result all nodes list
  /// \return Status
  Status GetAll(std::unordered_map<ClientID, ClientTableData> *result);

 private:
  GcsClientImpl &client_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NODE_STATE_ACCESSOR_H
