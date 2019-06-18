package org.ray.runtime.config;

public enum WorkerMode {
  WORKER(0),
  DRIVER(1);

  private int value;

  WorkerMode(int value) {
    this.value = value;
  }

  public int value() {
    return this.value;
  }
}
