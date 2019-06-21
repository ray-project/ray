package org.ray.runtime;

public enum WorkerLanguage {
  PYTHON(0),
  JAVA(1);

  private int value;

  WorkerLanguage(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static WorkerLanguage fromInteger(int value) {
    for (WorkerLanguage item : values()) {
      if (item.value == value) {
        return item;
      }
    }
    throw new IllegalArgumentException("Illegal value of " + WorkerLanguage.class.getSimpleName() + ": " + value);
  }
}
