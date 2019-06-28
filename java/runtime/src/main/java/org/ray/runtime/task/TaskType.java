package org.ray.runtime.task;

public enum TaskType {
  NORMAL_TASK(0),
  ACTOR_CREATION_TASK(1),
  ACTOR_TASK(2);

  private int value;

  TaskType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static TaskType fromInteger(int value) {
    for (TaskType item : values()) {
      if (item.value == value) {
        return item;
      }
    }
    throw new IllegalArgumentException("Illegal value of " + TaskType.class.getSimpleName() + ": " + value);
  }
}
