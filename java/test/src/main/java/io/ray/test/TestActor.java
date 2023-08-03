package io.ray.test;

public class TestActor {

  public TestActor(byte[] v) {
    value = v;
  }

  public byte[] concat(byte[] v) {
    byte[] c = new byte[value.length + v.length];
    System.arraycopy(value, 0, c, 0, value.length);
    System.arraycopy(v, 0, c, value.length, v.length);
    return c;
  }

  public byte[] getValue() {
    return value;
  }

  private byte[] value;
}
