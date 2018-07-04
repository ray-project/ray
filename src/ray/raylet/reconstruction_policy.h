#ifndef RAY_RAYLET_RECONSTRUCTION_POLICY_H
#define RAY_RAYLET_RECONSTRUCTION_POLICY_H

#include <functional>

#include "ray/id.h"

namespace ray {

namespace raylet {

class ReconstructionPolicyInterface {
 public:
  virtual void Listen(const ObjectID &object_id) = 0;
  virtual void Cancel(const ObjectID &object_id) = 0;
  virtual ~ReconstructionPolicyInterface(){};
};

class ReconstructionPolicy : public ReconstructionPolicyInterface {
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
  void Listen(const ObjectID &object_id);

  void Cancel(const ObjectID &object_id);

 private:
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RECONSTRUCTION_POLICY_H
