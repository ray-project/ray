#ifndef RAY_RAYLET_RECONSTRUCTION_POLICY_H
#define RAY_RAYLET_RECONSTRUCTION_POLICY_H

#include <functional>

#include "ray/id.h"

namespace ray {

namespace raylet {

// TODO(swang): Use std::function instead of boost.

class ReconstructionPolicy {
 public:
  /// Create the reconstruction policy.
  ///
  /// \param reconstruction_handler The handler to call if a task needs to be
  /// re-executed.
  // TODO(swang): This requires at minimum references to the Raylet's lineage
  // cache and GCS client.
  ReconstructionPolicy(std::function<void(const TaskID &)> reconstruction_handler) {}

  /// Check whether an object requires reconstruction. If this object requires
  /// reconstruction, the registered task reconstruction handler will be called
  /// for each task that needs to be re-executed.
  ///
  /// \param object_id The object to check for reconstruction.
  void CheckObjectReconstruction(const ObjectID &object_id);

 private:
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RECONSTRUCTION_POLICY_H
