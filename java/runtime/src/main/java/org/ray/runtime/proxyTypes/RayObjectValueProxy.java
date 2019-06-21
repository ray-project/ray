package org.ray.runtime.proxyTypes;

public class RayObjectValueProxy {
  public byte[] data;
  public byte[] metadata;

  public RayObjectValueProxy(byte[] data, byte[] metadata) {
    this.data = data;
    this.metadata = metadata;
  }
}

