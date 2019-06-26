#include "ray/gcs/node_state_accessor.h"
#include "ray/gcs/client.h"
#include "ray/gcs/gcs_client_impl.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

NodeStateAccessor::NodeStateAccessor(GcsClientImpl &client_impl)
    : client_impl_(client_impl) {}

Status NodeStateAccessor::GetAll(std::unordered_map<ClientID, ClientTableData> *result) {
  RAY_CHECK(result != nullptr);
  ClientTable &client_table = client_impl_.AsyncClient().client_table();
  *result = std::move(client_table.GetAllClients());
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
