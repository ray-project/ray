#include "ray/common/runtime_env_manager.h"
#include "ray/util/logging.h"
namespace ray {

void RuntimeEnvManager::AddURIReference(const std::string &hex_id,
                                        const rpc::RuntimeEnv &runtime_env) {
  const auto &uris = runtime_env.uris();
  for (const auto &uri : uris) {
    AddURIReference(hex_id, uri);
  }
}

void RuntimeEnvManager::AddURIReference(const std::string &hex_id,
                                        const std::string &uri) {
  if (unused_uris_.count(uri)) {
    unused_uris_.erase(uri);
  }
  uri_reference_[uri]++;
  id_to_uris_[hex_id].push_back(uri);
}

const std::vector<std::string> &RuntimeEnvManager::GetReferences(
    const std::string &hex_id) const {
  static const std::vector<std::string> _default;
  auto it = id_to_uris_.find(hex_id);
  return it == id_to_uris_.end() ? _default : it->second;
}

void RuntimeEnvManager::RemoveURIReference(const std::string &hex_id) {
  if (!id_to_uris_.count(hex_id)) {
    return;
  }

  for (const auto &uri : id_to_uris_[hex_id]) {
    --uri_reference_[uri];
    auto ref_count = uri_reference_[uri];
    RAY_CHECK(ref_count >= 0);
    if (ref_count == 0) {
      uri_reference_.erase(uri);
      RAY_LOG(DEBUG) << "Deleting uri: " << uri;
      deleter_(uri, [this, uri](bool success) {
        if (!success) {
          unused_uris_.insert(uri);
        }
      });
    }
  }
  id_to_uris_.erase(hex_id);
}

}  // namespace ray
