#include "ray/common/runtime_env_manager.h"
#include "ray/util/logging.h"
namespace ray {

void RuntimeEnvManager::AddUriReference(const std::string &hex_id,
                                        const rpc::RuntimeEnv &runtime_env) {
  const auto &uris = runtime_env.uris();
  for (const auto &uri : uris) {
    if (unused_uris_.count(uri)) {
      unused_uris_.erase(uri);
    }
    uri_reference_[uri]++;
    id_to_uris_[hex_id].push_back(uri);
  }
}

void RuntimeEnvManager::RemoveUriReference(const std::string &hex_id) {
  for (const auto &uri : id_to_uris_[hex_id]) {
    --uri_reference_[uri];
    auto ref_count = uri_reference_[uri];
    RAY_CHECK(ref_count >= 0);
    if (ref_count == 0) {
      uri_reference_.erase(uri);
      RAY_LOG(INFO) << "Deleting uri: " << uri;
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
