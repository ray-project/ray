package io.ray.test;

public class Counter {

  public Counter(int v) {
    value = v;
  }

  public int increase(int delta) {
    value += delta;
    return value;
  }

  private int value;
}
