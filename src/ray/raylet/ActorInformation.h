#ifndef ACTOR_INFORMATION_H
#define ACTOR_INFORMATION_H
namespace ray {
class ActorInformation {
public:
  ActorInformation() {}
  ~ActorInformation() {}
  ActorID GetActorId() const;
private:
  ActorID id_;
}; // end class ActorInformation
} // end namespace ray

#endif
