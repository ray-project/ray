#ifndef RAY_GCS_NODE_STATE_ACCESSOR_H
#define RAY_GCS_NODE_STATE_ACCESSOR_H

#include <map>
#include <vector>
#include "ray/common/id.h"
#include "ray/gcs/call_back.h"
#include "ray/gcs/format/gcs_generated.h"

namespace ray {

namespace gcs {

class AsyncGcsClient;

class NodeStateAccessor {
 public:
  NodeStateAccessor(AsyncGcsClient *client_impl);

  ~NodeStateAccessor() {}

  Status GetAll(std::vector<ClientTableDataT> *result);

  Status AsyncGetAvailableResource(const ClientID &node_id,
      DatumCallback<std::map<std::string, double>>::SingleItem callback);

 private:
  AsyncGcsClient *client_impl_{nullptr};
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NODE_STATE_ACCESSOR_H
