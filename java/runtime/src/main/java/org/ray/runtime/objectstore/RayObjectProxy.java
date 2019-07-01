package org.ray.runtime.objectstore;

public class RayObjectProxy {
  public byte[] data;
  public byte[] metadata;

  public RayObjectProxy(byte[] data, byte[] metadata) {
    this.data = data;
    this.metadata = metadata;
  }
}

