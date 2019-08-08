package org.ray.runtime.object;

public class NativeRayObject {

  public byte[] data;
  public byte[] metadata;

  public NativeRayObject(byte[] data, byte[] metadata) {
    this.data = data;
    this.metadata = metadata;
  }
}

