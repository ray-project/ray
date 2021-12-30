package io.ray.api.options;

/** The enumeration class is used for declaring lifetime of actors. It's non detached by default. */
public enum ActorLifetime {
  DEFAULT("DEFAULT", 0),
  DETACHED("DETACHED", 1);

  private String name;
  private int value;

  ActorLifetime(String name, int value) {
    this.name = name;
    this.value = value;
  }
}
