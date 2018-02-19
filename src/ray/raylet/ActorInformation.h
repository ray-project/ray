#ifndef ACTOR_INFORMATION_H
#define ACTOR_INFORMATION_H

#include "ray/id.h"

namespace ray {
class ActorInformation {
public:
  ActorInformation(): id_(UniqueID::nil()) {}
  ~ActorInformation() {}
  ActorID GetActorId() const;
private:
  ActorID id_;
}; // end class ActorInformation
} // end namespace ray

#endif
