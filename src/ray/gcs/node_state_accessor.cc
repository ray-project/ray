#include "ray/gcs/node_state_accessor.h"
#include "ray/gcs/client.h"
#include "ray/gcs/gcs_client_impl.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

NodeStateAccessor::NodeStateAccessor(GcsClientImpl &client_impl)
    : client_impl_(client_impl) {}

Status NodeStateAccessor::GetAll(std::vector<ClientTableData> *result) {
  RAY_CHECK(result != nullptr);
  ClientTable &client_table = client_impl_.AsyncClient().client_table();
  std::unordered_map<ClientID, ClientTableData> node_map = client_table.GetAllClients();
  for (auto const &node : node_map) {
    result->emplace_back(std::move(node.second));
  }
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
