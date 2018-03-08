#include "actor.h"

namespace ray {

ActorInformation::ActorInformation() : id_(UniqueID::nil()) {}

ActorInformation::~ActorInformation() {}

const ActorID &ActorInformation::GetActorId() const { return this->id_; }

}  // namespace ray
