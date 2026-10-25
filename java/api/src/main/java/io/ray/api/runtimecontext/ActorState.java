package io.ray.api.runtimecontext;

/** The state of an actor. Note, this class should be aligned with the actor state of proto. */
public enum ActorState {
  DEPENDENCIES_UNREADY("DEPENDENCIES_UNREADY", 0),
  PENDING_CREATION("PENDING_CREATION", 1),
  ALIVE("ALIVE", 2),
  RESTARTING("RESTARTING", 3),
  DEAD("DEAD", 4);

  private String name;

  private int value;

  private ActorState(String name, int value) {
    this.name = name;
    this.value = value;
  }

  public static ActorState fromValue(int value) {
    switch (value) {
      case 0:
        return DEPENDENCIES_UNREADY;
      case 1:
        return PENDING_CREATION;
      case 2:
        return ALIVE;
      case 3:
        return RESTARTING;
      case 4:
        return DEAD;
      default:
        throw new RuntimeException("Value out of range.");
    }
  }

  public String getName() {
    return name;
  }

  public int getValue() {
    return value;
  }
}
