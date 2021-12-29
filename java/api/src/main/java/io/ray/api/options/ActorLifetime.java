package io.ray.api.options;

/** The enumeration class is used for declaring lifetime of actors. */
public enum ActorLifetime {
  DETACHED("DETACHED", 1),
  NON_DETACHED("NON_DETACHED", 2);

  private String name;
  private int value;

  ActorLifetime(String name, int value) {
    this.name = name;
    this.value = value;
  }
}
