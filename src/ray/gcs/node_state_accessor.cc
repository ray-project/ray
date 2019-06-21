#include "ray/gcs/node_state_accessor.h"
#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

NodeStateAccessor::NodeStateAccessor(AsyncGcsClient *client_impl)
    : client_impl_(client_impl) {
  RAY_DCHECK(client_impl != nullptr);
}

Status NodeStateAccessor::GetAll(std::vector<ClientTableDataT> *result) {
  RAY_CHECK(result != nullptr);
  ClientTable &client_table = client_impl_->client_table();
  std::unordered_map<ClientID, ClientTableDataT> node_map = client_table.GetAllClients();
  for (auto const &node : node_map) {
    result->emplace_back(std::move(node.second));
  }
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
