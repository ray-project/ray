package org.ray.api.options;

public enum ActorLifecycleStrategy {
  INDEPENDENT,  // Default. Actors die and recover independently.
  WITH_GROUP,  // When a part of Actors in Group die, all Actors in Group will be killed and recover.
}
