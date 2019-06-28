package org.ray.runtime.proxyTypes;

public class RayObjectProxy {
  public byte[] data;
  public byte[] metadata;

  public RayObjectProxy(byte[] data, byte[] metadata) {
    this.data = data;
    this.metadata = metadata;
  }
}

