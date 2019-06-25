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

class NodeStateAccessor {
 public:
  NodeStateAccessor(GcsClientImpl &client_impl);

  ~NodeStateAccessor() {}

  Status GetAll(std::vector<ClientTableData> *result);

  Status AsyncGetAvailableResource(
      const ClientID &node_id,
      DatumCallback<std::map<std::string, double>>::SingleItem callback);

 private:
  GcsClientImpl &client_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NODE_STATE_ACCESSOR_H
