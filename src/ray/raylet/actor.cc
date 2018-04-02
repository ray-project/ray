#include "actor.h"

namespace ray {

namespace raylet {

ActorInformation::ActorInformation() : id_(UniqueID::nil()) {}

ActorInformation::~ActorInformation() {}

const ActorID &ActorInformation::GetActorId() const { return this->id_; }

}  // namespace raylet

}  // namespace ray
