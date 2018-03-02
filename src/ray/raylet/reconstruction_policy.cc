#ifndef RECONSTRUCTION_POLICY_CC
#define RECONSTRUCTION_POLICY_CC

#include "reconstruction_policy.h"

using namespace std;
namespace ray {

void ReconstructionPolicy::CheckObjectReconstruction(const ObjectID &object) {
  throw std::runtime_error("Method not implemented");
}

} // end namespace ray

#endif  // RECONSTRUCTION_POLICY_CC
