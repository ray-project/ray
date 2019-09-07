package org.ray.runtime.object;

/**
 * Binary representation of ray object.
 */
public class NativeRayObject {

  public byte[] data;
  public byte[] metadata;

  public NativeRayObject(byte[] data, byte[] metadata) {
    this.data = data;
    this.metadata = metadata;
  }
}

