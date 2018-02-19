#ifndef ACTOR_INFORMATION_CC
#define ACTOR_INFORMATION_CC

#include "ActorInformation.h"

namespace ray {
ActorID ActorInformation::GetActorId() const {
  return this->id_;
}
} // end namespace ray

#endif
