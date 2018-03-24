#include "reconstruction_policy.h"

namespace ray {

namespace raylet {

void ReconstructionPolicy::Listen(const ObjectID &object) {
  throw std::runtime_error("Method not implemented");
}

void ReconstructionPolicy::Notify(const ObjectID &object_id) {
  throw std::runtime_error("Method not implemented");
}

void ReconstructionPolicy::Cancel(const ObjectID &object_id) {
  throw std::runtime_error("Method not implemented");
}

void ReconstructionPolicy::HandleNotification(
    const ObjectID &object_id, const std::vector<ObjectTableDataT> new_locations) {
  throw std::runtime_error("Method not implemented");
}

void ReconstructionPolicy::Reconstruct(const ObjectID &object_id) {
  throw std::runtime_error("Method not implemented");
}

}  // namespace raylet

}  // end namespace ray
