#include "ray/runtime_env_manager.h"

void RuntimeEnvManagerBase::IncrPackageReference(const std::string &hex_id,
                                                 const rpc::RuntimeEnv &runtime_env) {
  if (!runtime_env.working_dir_uri().empty()) {
    const auto &uri = runtime_env.working_dir_uri();
    package_reference_[uri]++;
    id_to_packages_[hex_id].push_back(uri);
  }
}

void RuntimeEnvManagerBase::DecrPackageReference(const std::string &hex_id) {
  for (const auto &package_uri : id_to_packages_[hex_id]) {
    --package_reference_[package_uri];
    auto ref_cnt = package_reference_[package_uri];
    RAY_CHECK(ref_cnt >= 0);
    if (ref_cnt == 0) {
      package_reference_.erase(package_uri);
    }
    DeleteURI(package_uri);
  }
  id_to_packages_.erase(hex_id);
}
