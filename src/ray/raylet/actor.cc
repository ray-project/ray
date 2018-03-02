#ifndef ACTOR_CC
#define ACTOR_CC

#include "actor.h"

namespace ray {

ActorInformation::ActorInformation(): id_(UniqueID::nil()) {}

ActorInformation::~ActorInformation() {}

const ActorID& ActorInformation::GetActorId() const {
  return this->id_;
}

} // namespace ray

#endif // ACTOR_CC
