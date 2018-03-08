#ifndef RAY_RAYLET_ACTOR_H
#define RAY_RAYLET_ACTOR_H

#include "ray/id.h"

namespace ray {
class ActorInformation {
 public:
  /// \brief ActorInformation constructor.
  ActorInformation();

  /// \brief ActorInformation destructor.
  ~ActorInformation();

  /// \brief Return the id of this actor.
  /// \return actor id.
  const ActorID &GetActorId() const;

 private:
  /// Unique identifier for this actor.
  ActorID id_;
};  // class ActorInformation

}  // namespace ray

#endif  // RAY_RAYLET_ACTOR_H
